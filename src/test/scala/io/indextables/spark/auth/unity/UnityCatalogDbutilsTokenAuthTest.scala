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

package io.indextables.spark.auth.unity

import java.net.{InetSocketAddress, URI}

import scala.collection.mutable.ArrayBuffer

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import io.indextables.spark.TestBase

/**
 * Tests for the DbutilsRefreshedToken auth mode: selected when the apiToken carries an `apiToken.source=dbutils` marker
 * (set by IndexTables4SparkExtensions.DbutilsTokenManager). resolveToken re-reads the freshest token from the driver's
 * runtime SQLConf on every call, falling back to the token captured in the per-plan config map when the session key is
 * absent (the executor path).
 *
 * Extends TestBase so a real driver SparkSession is active, exercising the actual `SparkSession.active.conf` reflection
 * path rather than a mock.
 */
class UnityCatalogDbutilsTokenAuthTest extends TestBase {

  private var mockServer: HttpServer               = _
  private var serverPort: Int                      = _
  private val requestLog: ArrayBuffer[MockRequest] = ArrayBuffer.empty

  case class MockRequest(
    method: String,
    path: String,
    body: String,
    headers: Map[String, String])

  private val ApiTokenKey       = "spark.indextables.databricks.apiToken"
  private val ApiTokenSourceKey = "spark.indextables.databricks.apiToken.source"

  override def beforeAll(): Unit = {
    super.beforeAll()
    mockServer = HttpServer.create(new InetSocketAddress(0), 0)
    serverPort = mockServer.getAddress.getPort
    mockServer.setExecutor(null)
    mockServer.start()
  }

  override def afterAll(): Unit = {
    if (mockServer != null) mockServer.stop(0)
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    requestLog.clear()
    UnityCatalogAWSCredentialProvider.clearCache()
    clearSessionTokenKeys()
    try mockServer.removeContext("/api/2.1/unity-catalog/temporary-path-credentials")
    catch { case _: IllegalArgumentException => }
  }

  override def afterEach(): Unit = {
    clearSessionTokenKeys()
    UnityCatalogAWSCredentialProvider.clearCache()
    super.afterEach()
  }

  private def clearSessionTokenKeys(): Unit = {
    spark.conf.unset(ApiTokenKey)
    spark.conf.unset(ApiTokenSourceKey)
  }

  private def setupHandler(responseBody: String): Unit = {
    val path = "/api/2.1/unity-catalog/temporary-path-credentials"
    try mockServer.removeContext(path)
    catch { case _: IllegalArgumentException => }
    mockServer.createContext(
      path,
      (exchange: HttpExchange) => {
        val body = new String(exchange.getRequestBody.readAllBytes())
        val headers = {
          import scala.jdk.CollectionConverters._
          exchange.getRequestHeaders.asScala.map { case (k, v) => k -> v.get(0) }.toMap
        }
        requestLog += MockRequest(exchange.getRequestMethod, exchange.getRequestURI.getPath, body, headers)
        exchange.sendResponseHeaders(200, responseBody.length)
        val os = exchange.getResponseBody
        os.write(responseBody.getBytes)
        os.close()
      }
    )
  }

  private def credentialResponse(key: String = "DBUTILS_KEY"): String =
    s"""{
       |  "aws_temp_credentials": {
       |    "access_key_id": "$key",
       |    "secret_access_key": "test-secret",
       |    "session_token": "test-session"
       |  },
       |  "expiration_time": ${System.currentTimeMillis() + 3600000}
       |}""".stripMargin

  private def credentialRequests: Seq[MockRequest] =
    requestLog.filter(_.path.contains("temporary-path-credentials")).toSeq

  private def bearerOf(req: MockRequest): String = req.headers.getOrElse("Authorization", "")

  /** Config map as it would arrive from a per-plan extraction: token + dbutils marker + workspace URL. */
  private def dbutilsConfig(token: String): Map[String, String] = Map(
    "spark.indextables.databricks.workspaceUrl" -> s"http://localhost:$serverPort",
    "spark.indextables.databricks.apiToken"     -> token,
    ApiTokenSourceKey                           -> "dbutils"
  )

  // ==================== Mode selection ====================

  test("dbutils marker selects DbutilsRefreshedToken and vends with the token") {
    setupHandler(credentialResponse())

    val provider = UnityCatalogAWSCredentialProvider.fromConfig(
      new URI("s3://test-bucket/path"),
      dbutilsConfig("plan-token-1")
    )

    val creds = provider.getCredentials()
    assert(creds.getAWSAccessKeyId == "DBUTILS_KEY")
    // No session override set, so resolveToken falls back to the config-map token (executor path).
    assert(bearerOf(credentialRequests.head) == "Bearer plan-token-1")
  }

  test("apiToken without the dbutils marker stays StaticToken (unchanged behavior)") {
    setupHandler(credentialResponse())

    val provider = UnityCatalogAWSCredentialProvider.fromConfig(
      new URI("s3://test-bucket/path"),
      Map(
        "spark.indextables.databricks.workspaceUrl" -> s"http://localhost:$serverPort",
        "spark.indextables.databricks.apiToken"     -> "plain-static-token"
      )
    )

    provider.getCredentials()
    assert(bearerOf(credentialRequests.head) == "Bearer plain-static-token")
  }

  // ==================== Driver re-read vs executor fallback ====================

  test("driver re-read: resolveToken prefers the live session token over the config-map token") {
    setupHandler(credentialResponse())
    // Simulate the background refresh thread having written a fresher token into the driver session conf.
    spark.conf.set(ApiTokenKey, "session-fresh-token")

    val provider = UnityCatalogAWSCredentialProvider.fromConfig(
      new URI("s3://test-bucket/path"),
      dbutilsConfig("plan-token-stale")
    )

    provider.getCredentials()
    assert(bearerOf(credentialRequests.head) == "Bearer session-fresh-token")
  }

  test("executor fallback: with no live session token, the captured config-map token is used") {
    setupHandler(credentialResponse())
    // Session key absent (unset in beforeEach) — mimics an executor where SparkSession.active has no token.
    val provider = UnityCatalogAWSCredentialProvider.fromConfig(
      new URI("s3://test-bucket/path"),
      dbutilsConfig("plan-token-captured")
    )

    provider.getCredentials()
    assert(bearerOf(credentialRequests.head) == "Bearer plan-token-captured")
  }

  test("resolveToken re-reads on every call — rotation in the session conf is picked up") {
    setupHandler(credentialResponse())
    spark.conf.set(ApiTokenKey, "rotating-1")

    val provider = UnityCatalogAWSCredentialProvider.fromConfig(
      new URI("s3://test-bucket/path"),
      dbutilsConfig("plan-token")
    )

    provider.getCredentials()
    assert(bearerOf(credentialRequests.head) == "Bearer rotating-1")

    // Rotate the session token, force a re-fetch, and confirm the new value is sent.
    spark.conf.set(ApiTokenKey, "rotating-2")
    UnityCatalogAWSCredentialProvider.clearCache()
    provider.getCredentials()
    assert(bearerOf(credentialRequests(1)) == "Bearer rotating-2")
  }

  // ==================== Cache identity ====================

  test("AWS credential cache survives token rotation (fixed dbutils identity)") {
    setupHandler(credentialResponse())
    spark.conf.set(ApiTokenKey, "rotating-1")

    val provider = UnityCatalogAWSCredentialProvider.fromConfig(
      new URI("s3://test-bucket/path"),
      dbutilsConfig("plan-token")
    )

    provider.getCredentials()
    assert(credentialRequests.size == 1)

    // Rotation must NOT invalidate cached AWS credentials — the cache key is a fixed dbutils identity,
    // not the transient token.
    spark.conf.set(ApiTokenKey, "rotating-2")
    provider.getCredentials()
    assert(credentialRequests.size == 1, "cached AWS credentials should be reused across token rotation")
  }
}
