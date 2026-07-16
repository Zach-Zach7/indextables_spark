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

import java.util.concurrent.atomic.AtomicInteger

import io.indextables.spark.TestBase

/**
 * Unit tests for DbutilsTokenManager. Uses a stub token-reader so no real dbutils (or Databricks classpath) is
 * required, and drives the extracted refresh step directly so no thread timing is involved. Extends TestBase for a real
 * driver SparkSession to write runtime conf into.
 */
class DbutilsTokenManagerTest extends TestBase {

  import DbutilsTokenManager._

  private val AmbientWorkspaceKey = AmbientWorkspaceUrlKey

  override def beforeEach(): Unit = {
    super.beforeEach()
    clearKeys()
    resetForTests()
  }

  override def afterEach(): Unit = {
    clearKeys()
    resetForTests()
    super.afterEach()
  }

  private def clearKeys(): Unit =
    Seq(EnabledKey, IntervalKey, ApiTokenKey, ApiTokenSourceKey, WorkspaceUrlKey, AmbientWorkspaceKey)
      .foreach(spark.conf.unset)

  private def reader(token: Option[String]): () => Option[String] = () => token

  // ==================== Flag / interval parsing ====================

  test("isEnabled is false by default and true only for an explicit true") {
    assert(!isEnabled(spark))
    spark.conf.set(EnabledKey, "false")
    assert(!isEnabled(spark))
    spark.conf.set(EnabledKey, "TRUE")
    assert(isEnabled(spark))
  }

  test("intervalSeconds defaults to 1800 and honors a positive override") {
    assert(intervalSeconds(spark) == 1800L)
    spark.conf.set(IntervalKey, "600")
    assert(intervalSeconds(spark) == 600L)
    spark.conf.set(IntervalKey, "0") // non-positive ignored
    assert(intervalSeconds(spark) == 1800L)
    spark.conf.set(IntervalKey, "not-a-number")
    assert(intervalSeconds(spark) == 1800L)
  }

  // ==================== bootstrap gating ====================

  test("bootstrap is a no-op when the flag is off (even with a token available)") {
    bootstrap(spark, reader(Some("dbutils-token")), startThread = false)
    assert(spark.conf.getOption(ApiTokenKey).isEmpty)
    assert(spark.conf.getOption(ApiTokenSourceKey).isEmpty)
  }

  test("bootstrap does not override an explicitly configured apiToken") {
    spark.conf.set(EnabledKey, "true")
    spark.conf.set(ApiTokenKey, "operator-token")
    bootstrap(spark, reader(Some("dbutils-token")), startThread = false)
    assert(spark.conf.get(ApiTokenKey) == "operator-token")
    // No marker written, so the provider keeps StaticToken semantics for the operator token.
    assert(spark.conf.getOption(ApiTokenSourceKey).isEmpty)
  }

  test("bootstrap is a no-op when enabled but no dbutils token is available") {
    spark.conf.set(EnabledKey, "true")
    bootstrap(spark, reader(None), startThread = false)
    assert(spark.conf.getOption(ApiTokenKey).isEmpty)
    assert(spark.conf.getOption(ApiTokenSourceKey).isEmpty)
  }

  // ==================== bootstrap happy path ====================

  test("bootstrap injects the token and the dbutils marker when enabled") {
    spark.conf.set(EnabledKey, "true")
    bootstrap(spark, reader(Some("dbutils-token")), startThread = false)
    assert(spark.conf.get(ApiTokenKey) == "dbutils-token")
    assert(spark.conf.get(ApiTokenSourceKey) == DbutilsSourceValue)
  }

  test("bootstrap auto-resolves workspaceUrl from spark.databricks.workspaceUrl (https prepended)") {
    spark.conf.set(EnabledKey, "true")
    spark.conf.set(AmbientWorkspaceKey, "my-workspace.cloud.databricks.com")
    bootstrap(spark, reader(Some("dbutils-token")), startThread = false)
    assert(spark.conf.get(WorkspaceUrlKey) == "https://my-workspace.cloud.databricks.com")
  }

  test("bootstrap does not overwrite an explicit workspaceUrl") {
    spark.conf.set(EnabledKey, "true")
    spark.conf.set(WorkspaceUrlKey, "https://explicit.example.com")
    spark.conf.set(AmbientWorkspaceKey, "ambient.cloud.databricks.com")
    bootstrap(spark, reader(Some("dbutils-token")), startThread = false)
    assert(spark.conf.get(WorkspaceUrlKey) == "https://explicit.example.com")
  }

  // ==================== refreshOnce ====================

  test("refreshOnce rewrites the token and returns true") {
    spark.conf.set(ApiTokenKey, "old")
    val ok = refreshOnce(spark, reader(Some("new-token")))
    assert(ok)
    assert(spark.conf.get(ApiTokenKey) == "new-token")
    assert(spark.conf.get(ApiTokenSourceKey) == DbutilsSourceValue)
  }

  test("refreshOnce keeps the previous token and returns false on an empty read") {
    spark.conf.set(ApiTokenKey, "keep-me")
    val ok = refreshOnce(spark, reader(None))
    assert(!ok)
    assert(spark.conf.get(ApiTokenKey) == "keep-me")
  }

  // ==================== refresh thread guard ====================

  test("startRefreshThread starts exactly one thread per JVM guard") {
    val calls                   = new AtomicInteger(0)
    val r: () => Option[String] = () => { calls.incrementAndGet(); Some("t") }

    // Large interval so the loop sleeps immediately without refreshing during the test.
    val t1 = startRefreshThread(spark, r, 3600L)
    val t2 = startRefreshThread(spark, r, 3600L)
    try {
      assert(t1.isDefined, "first call should start a thread")
      assert(t2.isEmpty, "second call should be suppressed by the per-JVM guard")
    } finally
      t1.foreach(_.interrupt())
  }
}
