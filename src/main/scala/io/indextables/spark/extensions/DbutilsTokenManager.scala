/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.indextables.spark.extensions

import java.util.concurrent.atomic.AtomicBoolean

import scala.util.Try

import org.apache.spark.sql.SparkSession

import org.slf4j.LoggerFactory

/**
 * Opt-in bootstrap for Databricks dbutils-managed API-token auth.
 *
 * Background: on SINGLE_USER Databricks clusters `spark.databricks.token` is absent, but the dbutils notebook-context
 * token IS available on the DRIVER and — unlike an external OAuth client-credentials token — bypasses the metastore's
 * external-access gate (no `EXTERNAL_ACCESS_DISABLED_ON_METASTORE` 403). That token has a short (~1h) TTL, which is
 * insufficient for indefinitely-running streaming/companion jobs.
 *
 * This manager, when explicitly enabled, reads the dbutils token on the driver, writes it to
 * `spark.indextables.databricks.apiToken` in the session's runtime conf, tags it with
 * `spark.indextables.databricks.apiToken.source=dbutils` (which makes
 * [[io.indextables.spark.auth.unity.UnityCatalogAWSCredentialProvider]] select its DbutilsRefreshedToken mode), and
 * starts a single daemon thread that rewrites the token before it expires. Because scan/write planning re-reads the
 * session conf on every invocation, each streaming micro-batch serializes the current token to its executors — so the
 * refreshed token reaches the workers via normal per-plan config propagation.
 *
 * IMPORTANT — footprint: this is entirely opt-in and additive. It does nothing unless
 * `spark.indextables.databricks.dbutilsTokenRefresh.enabled=true` AND a dbutils token is actually readable (i.e. a
 * Databricks driver). It never overrides an explicitly configured apiToken, and it changes no default behaviour for any
 * existing deployment or non-Databricks environment.
 *
 * Known limitation: dbutils and the driver session conf are driver-only, and the token reaches executors only by being
 * serialized into a plan's config map. A single Spark job/plan that runs longer than the token TTL cannot refresh the
 * token on its executors (there is no new plan and no executor-side way to mint one). This mechanism therefore covers
 * many-plan workloads (streaming micro-batches, multi-job pipelines) but not a single run-to-completion batch that
 * exceeds the TTL.
 */
object DbutilsTokenManager {
  private val logger = LoggerFactory.getLogger(getClass)

  /** Opt-in switch. When absent/false (the default), every method below is a no-op. */
  val EnabledKey = "spark.indextables.databricks.dbutilsTokenRefresh.enabled"

  /** Refresh cadence override (seconds). Default 1800 (30 min); comfortably under the ~1h dbutils TTL. */
  val IntervalKey = "spark.indextables.databricks.dbutilsTokenRefresh.intervalSeconds"

  val ApiTokenKey       = "spark.indextables.databricks.apiToken"
  val ApiTokenSourceKey = "spark.indextables.databricks.apiToken.source"
  val WorkspaceUrlKey   = "spark.indextables.databricks.workspaceUrl"

  /** Ambient hostname Databricks injects; normalized to https:// when used as the workspace URL. */
  val AmbientWorkspaceUrlKey = "spark.databricks.workspaceUrl"

  /** Marker value written to [[ApiTokenSourceKey]] and recognized by the credential provider. */
  val DbutilsSourceValue = "dbutils"

  val DefaultIntervalSeconds = 1800L

  // One refresh thread per JVM, regardless of how many sessions bootstrap.
  private val refreshThreadStarted = new AtomicBoolean(false)

  /** True only when the opt-in flag is explicitly set to "true" (case-insensitive). */
  def isEnabled(session: SparkSession): Boolean =
    session.conf.getOption(EnabledKey).exists(_.trim.equalsIgnoreCase("true"))

  def intervalSeconds(session: SparkSession): Long =
    session.conf
      .getOption(IntervalKey)
      .flatMap(v => Try(v.trim.toLong).toOption)
      .filter(_ > 0)
      .getOrElse(DefaultIntervalSeconds)

  /**
   * Idempotent, side-effecting bootstrap. Safe to call on any cluster and multiple times.
   *
   * No-ops unless the opt-in flag is set and the injected `tokenReader` yields a token. Never overrides an explicitly
   * configured apiToken. `startThread` is exposed so unit tests can bootstrap without spawning the daemon thread.
   */
  def bootstrap(
    session: SparkSession,
    tokenReader: () => Option[String] = () => readDbutilsApiToken(),
    startThread: Boolean = true
  ): Unit = {
    if (!isEnabled(session)) return

    // Respect an operator-configured token — dbutils only fills the zero-config gap.
    if (session.conf.getOption(ApiTokenKey).exists(_.trim.nonEmpty)) {
      logger.info(
        s"$EnabledKey is set but $ApiTokenKey is already configured; leaving the explicit token in place"
      )
      return
    }

    tokenReader() match {
      case Some(token) if token.trim.nonEmpty =>
        session.conf.set(ApiTokenKey, token)
        session.conf.set(ApiTokenSourceKey, DbutilsSourceValue)
        if (session.conf.getOption(WorkspaceUrlKey).forall(_.trim.isEmpty))
          readAmbientWorkspaceUrl(session).foreach { url =>
            session.conf.set(WorkspaceUrlKey, url)
            logger.info(s"Auto-resolved $WorkspaceUrlKey from $AmbientWorkspaceUrlKey")
          }
        logger.info(s"Injected dbutils-managed apiToken into session conf ($ApiTokenSourceKey=$DbutilsSourceValue)")
        if (startThread) startRefreshThread(session, tokenReader, intervalSeconds(session))
      case _ =>
        logger.info(
          s"$EnabledKey is set but no dbutils token is available (not a Databricks driver?); no token injected"
        )
    }
  }

  /**
   * Read the dbutils notebook-context API token on the DRIVER via reflection. Returns None off Databricks (class
   * absent) or on executors (no notebook context). Reflection keeps this module free of a compile-time Databricks
   * dependency. Method chain confirmed present on SINGLE_USER driver probes:
   * `DBUtilsHolder.dbutils.notebook.getContext.apiToken`.
   */
  def readDbutilsApiToken(): Option[String] =
    Try {
      val holderClass = Class.forName("com.databricks.dbutils_v1.DBUtilsHolder")
      val dbutils     = holderClass.getMethod("dbutils").invoke(null)
      val notebook    = dbutils.getClass.getMethod("notebook").invoke(dbutils)
      val ctx         = notebook.getClass.getMethod("getContext").invoke(notebook)
      ctx.getClass
        .getMethod("apiToken")
        .invoke(ctx)
        .asInstanceOf[Option[String]]
        .map(_.trim)
        .filter(_.nonEmpty)
    }.toOption.flatten

  /** Read `spark.databricks.workspaceUrl` (a bare hostname) and normalize to an https:// URL. */
  def readAmbientWorkspaceUrl(session: SparkSession): Option[String] =
    session.conf
      .getOption(AmbientWorkspaceUrlKey)
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(url => if (url.startsWith("https://") || url.startsWith("http://")) url else s"https://$url")

  /**
   * Start the single per-JVM daemon refresh thread. Returns the started thread, or None if one was already started. The
   * reader and interval are injected so tests can drive the loop deterministically.
   */
  def startRefreshThread(
    session: SparkSession,
    tokenReader: () => Option[String],
    intervalSeconds: Long
  ): Option[Thread] =
    if (!refreshThreadStarted.compareAndSet(false, true)) {
      logger.debug("dbutils token refresh thread already started for this JVM; not starting another")
      None
    } else {
      val thread = new Thread(
        new Runnable {
          override def run(): Unit = refreshLoop(session, tokenReader, intervalSeconds)
        },
        "indextables-dbutils-token-refresh"
      )
      thread.setDaemon(true)
      thread.start()
      logger.info(s"Started dbutils token refresh thread (interval=${intervalSeconds}s)")
      Some(thread)
    }

  private def refreshLoop(
    session: SparkSession,
    tokenReader: () => Option[String],
    intervalSeconds: Long
  ): Unit =
    while (!Thread.currentThread().isInterrupted)
      try {
        Thread.sleep(intervalSeconds * 1000L)
        refreshOnce(session, tokenReader)
      } catch {
        case _: InterruptedException =>
          Thread.currentThread().interrupt()
        case e: Throwable =>
          // Never let a transient failure kill the thread — the current token stays in place and we retry.
          logger.warn("dbutils token refresh iteration failed; keeping previous token and retrying", e)
      }

  /**
   * One refresh step, extracted for unit testing without threads. Re-reads the token and rewrites the session conf; on
   * an empty read it keeps the previous value rather than clobbering a working token with nothing.
   */
  def refreshOnce(session: SparkSession, tokenReader: () => Option[String]): Boolean =
    tokenReader() match {
      case Some(token) if token.trim.nonEmpty =>
        session.conf.set(ApiTokenKey, token)
        session.conf.set(ApiTokenSourceKey, DbutilsSourceValue)
        logger.debug("Refreshed dbutils-managed apiToken in session conf")
        true
      case _ =>
        logger.warn("dbutils token refresh returned no token; keeping previous value")
        false
    }

  /** Test-only: reset the per-JVM thread guard so a subsequent startRefreshThread will start a new thread. */
  private[extensions] def resetForTests(): Unit = refreshThreadStarted.set(false)
}
