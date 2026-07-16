# Cloud Configuration

S3, Azure, and Unity Catalog configuration for IndexTables4Spark.

---

## S3 Configuration

```scala
// Basic auth
spark.indextables.aws.accessKey: <key>
spark.indextables.aws.secretKey: <secret>

// Custom credential provider
spark.indextables.aws.credentialsProviderClass: "com.example.MyProvider"

// Upload performance
spark.indextables.s3.maxConcurrency: 4 (parallel uploads)
spark.indextables.s3.partSize: "64M"
```

---

## Azure Configuration

```scala
// Account key auth
spark.indextables.azure.accountName: <account>
spark.indextables.azure.accountKey: <key>

// OAuth Service Principal
spark.indextables.azure.tenantId: <tenant>
spark.indextables.azure.clientId: <client>
spark.indextables.azure.clientSecret: <secret>

// Supports: abfss://, wasbs://, abfs:// URLs
// Uses ~/.azure/credentials file or environment variables
```

### Basic Azure Write/Read
```scala
// Session-level config
spark.conf.set("spark.indextables.azure.accountName", "mystorageaccount")
spark.conf.set("spark.indextables.azure.accountKey", "your-account-key")

// Write (supports abfss://, wasbs://, abfs://)
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save("abfss://container/path")

// Read
val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load("abfss://container/path")
```

### Azure OAuth (Service Principal)
```scala
spark.conf.set("spark.indextables.azure.accountName", "account")
spark.conf.set("spark.indextables.azure.tenantId", "tenant-id")
spark.conf.set("spark.indextables.azure.clientId", "client-id")
spark.conf.set("spark.indextables.azure.clientSecret", "client-secret")

df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .save("abfss://container@account.dfs.core.windows.net/path")
```

---

## Unity Catalog Integration (Databricks)

Access Unity Catalog-managed S3 paths using temporary credentials from the Databricks API.

**Provider class:** `io.indextables.spark.auth.unity.UnityCatalogAWSCredentialProvider`

### Configuration Properties

| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| `spark.indextables.databricks.workspaceUrl` | No* | - | Databricks workspace URL (*auto-resolved on Databricks compute) |
| `spark.indextables.databricks.apiToken` | No* | - | Databricks API token (PAT or OAuth) (*see auth modes below) |
| `spark.indextables.databricks.clientId` | No | - | OAuth2 client ID (machine-to-machine auth) |
| `spark.indextables.databricks.clientSecret` | No | - | OAuth2 client secret (machine-to-machine auth) |
| `spark.indextables.databricks.dbutilsTokenRefresh.enabled` | No | false | Opt-in: on a Databricks driver, seed + auto-refresh the apiToken from the dbutils notebook-context token (see auth modes) |
| `spark.indextables.databricks.dbutilsTokenRefresh.intervalSeconds` | No | 1800 | Refresh cadence for the dbutils token (under its ~1h TTL) |
| `spark.indextables.databricks.credential.refreshBuffer.minutes` | No | 40 | Minutes before expiration to refresh |
| `spark.indextables.databricks.cache.maxSize` | No | 100 | Maximum cached credential entries |
| `spark.indextables.databricks.fallback.enabled` | No | true | Fallback to READ if READ_WRITE fails |
| `spark.indextables.databricks.retry.attempts` | No | 3 | Retry attempts on API failure |

### Authentication Modes

Resolved in priority order:

1. **OAuth2 client credentials** — `clientId` + `clientSecret` both set. Tokens are exchanged at the
   workspace OIDC endpoint and refreshed automatically.
2. **Static API token** — `apiToken` set (personal access token or pre-issued OAuth token).
3. **Ambient cluster token** (zero-config, Databricks compute only) — when neither of the above is
   configured and the job runs on a Databricks cluster, the provider falls back to the cluster-scoped
   service token Databricks injects into SparkConf (`spark.databricks.token`). Because these UC API
   calls originate from within the Databricks compute plane, they are not subject to the metastore's
   `external_access_enabled` gate (no `EXTERNAL_ACCESS_DISABLED_ON_METASTORE` 403). In this mode the
   workspace URL is also auto-resolved from `spark.databricks.workspaceUrl` when not explicitly set,
   and the token is re-read on every call to tolerate Databricks token rotation. Note that UC
   authorization is evaluated against the job's run-as identity, which needs the appropriate
   privileges on the external location.

Outside Databricks compute, either `apiToken` or `clientId`/`clientSecret` must be configured.

#### dbutils token refresh (opt-in, requires the SQL extensions)

On SINGLE_USER Databricks clusters `spark.databricks.token` is absent, so the ambient mode above does not
apply. There, the dbutils notebook-context token is available **on the driver** and — unlike an external
OAuth token — bypasses the external-access gate, but it has a short (~1h) TTL. Setting
`spark.indextables.databricks.dbutilsTokenRefresh.enabled=true` (with `IndexTables4SparkExtensions`
registered) makes the extension read that token on the driver, publish it as
`spark.indextables.databricks.apiToken` (tagged `apiToken.source=dbutils`), auto-resolve the workspace URL
from `spark.databricks.workspaceUrl`, and start a background thread that refreshes it before expiry. The
provider then re-reads the freshest token on every call rather than pinning it.

This is entirely opt-in and additive — with the flag off (the default) nothing changes, and it never
overrides an explicitly configured `apiToken`. It never takes priority over OAuth: if `clientId`/`clientSecret`
are configured they still win, so on Databricks omit them when you want the dbutils token to be used.

**Limitation:** dbutils and the driver session conf are driver-only; the token reaches executors only by
being serialized into each plan's config map. Streaming micro-batches and multi-job pipelines re-plan and
therefore pick up refreshed tokens, but a **single job/plan that runs longer than the token TTL** cannot
refresh its executors' token and will fail once it expires. Chunk such workloads into sub-jobs that complete
under the TTL.

### Session-Level Configuration
```scala
spark.conf.set("spark.indextables.databricks.workspaceUrl", "https://myworkspace.cloud.databricks.com")
spark.conf.set("spark.indextables.databricks.apiToken", sys.env("DATABRICKS_TOKEN"))
spark.conf.set("spark.indextables.aws.credentialsProviderClass",
  "io.indextables.spark.auth.unity.UnityCatalogAWSCredentialProvider")

// Read from Unity Catalog-managed path
val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .load("s3://unity-catalog-bucket/path")

// Write to Unity Catalog-managed path
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .save("s3://unity-catalog-bucket/new-path")
```

### Per-Operation Configuration
```scala
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.databricks.workspaceUrl", "https://myworkspace.cloud.databricks.com")
  .option("spark.indextables.databricks.apiToken", token)
  .option("spark.indextables.aws.credentialsProviderClass",
    "io.indextables.spark.auth.unity.UnityCatalogAWSCredentialProvider")
  .save("s3://bucket/path")
```

### Key Features
- **Driver-side resolution:** Credentials resolved on driver, executors use pre-resolved credentials directly (no network calls to Databricks from workers)
- **Multi-user caching:** Credentials cached per API token + path
- **Automatic fallback:** If READ_WRITE fails (403), automatically falls back to READ credentials
- **Expiration-aware:** Refreshes credentials 40 minutes before expiration (configurable)
- **No SDK dependency:** Uses HTTP API directly, no Databricks SDK required
- **Credential priority:** Explicit credentials always take precedence over provider class
