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

import org.apache.spark.SparkEnv

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import io.indextables.spark.TestBase

/**
 * Tests for the AmbientClusterToken auth mode: when neither apiToken nor OAuth client credentials are configured, the
 * provider falls back to the cluster-scoped token Databricks injects into SparkConf as `spark.databricks.token`.
 *
 * Extends TestBase so a real local SparkContext (and therefore a real SparkEnv) is running — this exercises the actual
 * `SparkEnv.get.conf` reflection path used in production, not a mock. Ambient conf keys are injected into the live
 * SparkEnv conf per-test and removed afterwards.
 */
class UnityCatalogAmbientTokenAuthTest extends TestBase {

  private var mockServer: HttpServer               = _
  private var serverPort: Int                      = _
  private val requestLog: ArrayBuffer[MockRequest] = ArrayBuffer.empty

  case class MockRequest(
    method: String,
    path: String,
    body: String,
    headers: Map[String, String])

  private def ambientConf = SparkEnv.get.conf

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
    removeAmbientKeys()
    Seq("/api/2.1/unity-catalog/temporary-path-credentials", "/oidc/v1/token").foreach { path =>
      try mockServer.removeContext(path)
      catch { case _: IllegalArgumentException => }
    }
  }

  override def afterEach(): Unit = {
    // Always remove ambient keys from the shared SparkEnv conf so other suites in the same JVM
    // never see a stray spark.databricks.token.
    removeAmbientKeys()
    UnityCatalogAWSCredentialProvider.clearCache()
    super.afterEach()
  }

  private def removeAmbientKeys(): Unit = {
    ambientConf.remove("spark.databricks.token")
    ambientConf.remove("spark.databricks.workspaceUrl")
  }

  private def setupHandler(path: String, responseBody: String): Unit = {
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

  private def credentialResponse(key: String = "AMBIENT_KEY"): String =
    s"""{
       |  "aws_temp_credentials": {
       |    "access_key_id": "$key",
       |    "secret_access_key": "test-secret",
       |    "session_token": "test-session"
       |  },
       |  "expiration_time": ${System.currentTimeMillis() + 3600000}
       |}""".stripMargin

  private def oauthTokenResponse(token: String): String =
    s"""{"access_token": "$token", "token_type": "Bearer", "expires_in": 3600}"""

  private def credentialRequests: Seq[MockRequest] =
    requestLog.filter(_.path.contains("temporary-path-credentials")).toSeq

  private def bearerOf(req: MockRequest): String =
    req.headers.getOrElse("Authorization", "")

  // ==================== Happy path ====================

  test("ambient cluster token is used when no explicit auth is configured") {
    ambientConf.set("spark.databricks.token", "ambient-token-abc")
    setupHandler("/api/2.1/unity-catalog/temporary-path-credentials", credentialResponse())

    val provider = UnityCatalogAWSCredentialProvider.fromConfig(
      new URI("s3://test-bucket/path"),
      Map("spark.indextables.databricks.workspaceUrl" -> s"http://localhost:$serverPort")
    )

    val credentials = provider.getCredentials()
    assert(credentials.getAWSAccessKeyId == "AMBIENT_KEY")
    assert(bearerOf(credentialRequests.head) == "Bearer ambient-token-abc")
  }

  test("workspace URL is auto-resolved from spark.databricks.workspaceUrl in ambient mode") {
    ambientConf.set("spark.databricks.token", "ambient-token-abc")
    ambientConf.set("spark.databricks.workspaceUrl", s"http://localhost:$serverPort")
    setupHandler("/api/2.1/unity-catalog/temporary-path-credentials", credentialResponse())

    // Zero explicit configuration — both token and workspace URL come from the ambient conf.
    val provider = UnityCatalogAWSCredentialProvider.fromConfig(
      new URI("s3://test-bucket/path"),
      Map.empty[String, String]
    )

    val credentials = provider.getCredentials()
    assert(credentials.getAWSAccessKeyId == "AMBIENT_KEY")
    assert(bearerOf(credentialRequests.head) == "Bearer ambient-token-abc")
  }

  test("normalizeWorkspaceUrl prepends https:// to bare hostnames and strips trailing slash") {
    import UnityCatalogAWSCredentialProvider.normalizeWorkspaceUrl
    // Databricks stores spark.databricks.workspaceUrl as a bare hostname, no scheme.
    assert(normalizeWorkspaceUrl("my-workspace.cloud.databricks.com") == "https://my-workspace.cloud.databricks.com")
    assert(normalizeWorkspaceUrl("my-workspace.cloud.databricks.com/") == "https://my-workspace.cloud.databricks.com")
    assert(normalizeWorkspaceUrl("https://already-scheme.com") == "https://already-scheme.com")
    assert(normalizeWorkspaceUrl("http://localhost:8080") == "http://localhost:8080")
  }

  // ==================== Priority ====================

  test("explicit apiToken takes precedence over ambient token") {
    ambientConf.set("spark.databricks.token", "ambient-token-abc")
    setupHandler("/api/2.1/unity-catalog/temporary-path-credentials", credentialResponse())

    val provider = UnityCatalogAWSCredentialProvider.fromConfig(
      new URI("s3://test-bucket/path"),
      Map(
        "spark.indextables.databricks.workspaceUrl" -> s"http://localhost:$serverPort",
        "spark.indextables.databricks.apiToken"     -> "explicit-static-token"
      )
    )

    provider.getCredentials()
    assert(bearerOf(credentialRequests.head) == "Bearer explicit-static-token")
  }

  test("OAuth client credentials take precedence over ambient token") {
    ambientConf.set("spark.databricks.token", "ambient-token-abc")
    setupHandler("/oidc/v1/token", oauthTokenResponse("oauth-access-token"))
    setupHandler("/api/2.1/unity-catalog/temporary-path-credentials", credentialResponse())

    val provider = UnityCatalogAWSCredentialProvider.fromConfig(
      new URI("s3://test-bucket/path"),
      Map(
        "spark.indextables.databricks.workspaceUrl"        -> s"http://localhost:$serverPort",
        "spark.indextables.databricks.clientId"            -> "test-client",
        "spark.indextables.databricks.clientSecret"        -> "test-secret",
        "spark.indextables.databricks.oauth.allowInsecure" -> "true"
      )
    )

    provider.getCredentials()
    assert(requestLog.exists(_.path == "/oidc/v1/token"), "OIDC endpoint should have been called")
    assert(bearerOf(credentialRequests.head) == "Bearer oauth-access-token")
  }

  // ==================== Token rotation ====================

  test("ambient token is re-read from SparkConf on each credential fetch (rotation)") {
    ambientConf.set("spark.databricks.token", "rotating-token-1")
    setupHandler("/api/2.1/unity-catalog/temporary-path-credentials", credentialResponse())

    val provider = UnityCatalogAWSCredentialProvider.fromConfig(
      new URI("s3://test-bucket/path"),
      Map("spark.indextables.databricks.workspaceUrl" -> s"http://localhost:$serverPort")
    )

    provider.getCredentials()
    assert(bearerOf(credentialRequests.head) == "Bearer rotating-token-1")

    // Databricks rotates the token; force a re-fetch and verify the NEW token is sent.
    ambientConf.set("spark.databricks.token", "rotating-token-2")
    UnityCatalogAWSCredentialProvider.clearCache()
    provider.getCredentials()
    assert(bearerOf(credentialRequests(1)) == "Bearer rotating-token-2")
  }

  test("AWS credential cache survives ambient token rotation (fixed cache identity)") {
    ambientConf.set("spark.databricks.token", "rotating-token-1")
    setupHandler("/api/2.1/unity-catalog/temporary-path-credentials", credentialResponse())

    val provider = UnityCatalogAWSCredentialProvider.fromConfig(
      new URI("s3://test-bucket/path"),
      Map("spark.indextables.databricks.workspaceUrl" -> s"http://localhost:$serverPort")
    )

    provider.getCredentials()
    assert(credentialRequests.size == 1)

    // Rotation must NOT invalidate the cached AWS credentials — the cache is keyed by a fixed
    // ambient identity, not the transient token (same behavior as clientId for OAuth).
    ambientConf.set("spark.databricks.token", "rotating-token-2")
    provider.getCredentials()
    assert(credentialRequests.size == 1, "cached AWS credentials should be reused after token rotation")
  }

  // ==================== Failure modes ====================

  test("clear error when no auth is configured and no ambient token is present") {
    // SparkEnv IS active here (TestBase), but spark.databricks.token is absent — same outcome as
    // running outside Databricks.
    val ex = intercept[IllegalStateException] {
      UnityCatalogAWSCredentialProvider.fromConfig(
        new URI("s3://test-bucket/path"),
        Map("spark.indextables.databricks.workspaceUrl" -> s"http://localhost:$serverPort")
      )
    }
    assert(ex.getMessage.contains("databricks.apiToken"))
    assert(ex.getMessage.contains("spark.databricks.token"))
  }

  test("clear error when ambient token present but no workspace URL is resolvable") {
    ambientConf.set("spark.databricks.token", "ambient-token-abc")

    val ex = intercept[IllegalStateException] {
      UnityCatalogAWSCredentialProvider.fromConfig(
        new URI("s3://test-bucket/path"),
        Map.empty[String, String]
      )
    }
    assert(ex.getMessage.contains("workspaceUrl"))
  }

  test("empty ambient token is treated as absent") {
    ambientConf.set("spark.databricks.token", "   ")

    val ex = intercept[IllegalStateException] {
      UnityCatalogAWSCredentialProvider.fromConfig(
        new URI("s3://test-bucket/path"),
        Map("spark.indextables.databricks.workspaceUrl" -> s"http://localhost:$serverPort")
      )
    }
    assert(ex.getMessage.contains("databricks.apiToken"))
  }
}
