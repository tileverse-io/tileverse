# Authentication Setup

This guide details how to configure credentials for cloud storage and secure HTTP endpoints in `tileverse-storage` 2.0.

## Two configuration paths

For every backend you have a choice of two paths:

- **Properties-driven**: pass a `Properties` map (or a `StorageConfig`) to `StorageFactory.open(URI, Properties)` /
  `StorageFactory.openRangeReader(URI, Properties)`. This is what GeoTools, GeoServer, and Spring property-binding setups use.
- **SDK-injection**: build your own SDK client (`HttpClient`, `S3Client`, `BlobServiceClient`, GCS `Storage`) and pass it
  through `XxxStorageProvider.open(URI, sdkClient)`. The returned `Storage` *borrows* the client; closing the `Storage`
  does NOT close the client. Use this when you need configuration that the Properties surface can't express
  (custom retry policies, application-managed credential providers, custom HTTP transports, test fakes).

The patterns below show both paths for each authentication mode.

## HTTP

### Basic Auth

=== "Properties"
    ```java
    URI root = URI.create("https://secure.example.com/");
    Properties props = new Properties();
    props.setProperty("storage.http.username", "user");
    props.setProperty("storage.http.password", "pass");

    try (Storage storage = StorageFactory.open(root, props);
            RangeReader reader = storage.openRangeReader("data.bin")) {
        // ...
    }
    ```

=== "SDK-injection"
    ```java
    HttpAuthentication auth = new BasicAuthentication("user", "pass");
    HttpClient client = HttpClient.newHttpClient();

    try (Storage storage = HttpStorageProvider.open(
                URI.create("https://secure.example.com/"), client, auth);
            RangeReader reader = storage.openRangeReader("data.bin")) {
        // ...
    }
    ```

### Bearer Tokens (OAuth 2.0 / JWT)

=== "Properties"
    ```java
    Properties props = new Properties();
    props.setProperty("storage.http.bearer-token", System.getenv("API_TOKEN"));

    try (RangeReader reader = StorageFactory.openRangeReader(
            URI.create("https://api.example.com/data.bin"), props)) {
        // ...
    }
    ```

=== "SDK-injection"
    ```java
    HttpAuthentication auth = new BearerTokenAuthentication(System.getenv("API_TOKEN"));
    try (Storage storage = HttpStorageProvider.open(
                URI.create("https://api.example.com/"),
                HttpClient.newHttpClient(),
                auth);
            RangeReader reader = storage.openRangeReader("data.bin")) {
        // ...
    }
    ```

### API Keys (Custom Headers)

=== "Properties"
    ```java
    Properties props = new Properties();
    props.setProperty("storage.http.api-key-headername", "X-API-Key");
    props.setProperty("storage.http.api-key", "abc123xyz");
    // optional value prefix:
    // props.setProperty("storage.http.api-key-value-prefix", "ApiKey ");

    try (RangeReader reader = StorageFactory.openRangeReader(
            URI.create("https://api.provider.com/data"), props)) {
        // ...
    }
    ```

=== "SDK-injection"
    ```java
    HttpAuthentication auth = new ApiKeyAuthentication("X-API-Key", "abc123xyz", null);
    try (Storage storage = HttpStorageProvider.open(uri, HttpClient.newHttpClient(), auth);
            RangeReader reader = storage.openRangeReader("data.bin")) { /* ... */ }
    ```

### Self-signed certificates

For development against an HTTPS endpoint with a self-signed certificate:

=== "Properties"
    ```java
    Properties props = new Properties();
    props.setProperty("storage.http.trust-all-certificates", "true");
    // ... plus any other auth properties as above
    ```

=== "SDK-injection"
    Build the `HttpClient` yourself with a trust-all `SSLContext` and pass it via
    `HttpStorageProvider.open(URI, HttpClient[, HttpAuthentication])`.

## AWS S3

The S3 backend uses the AWS SDK v2 default credential chain unless you override it.

### Default discovery order

The chain looks for credentials in this order (standard AWS behavior):

1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. System properties (`aws.accessKeyId`, `aws.secretAccessKey`)
3. Web Identity Token (EKS/K8s)
4. `~/.aws/credentials` file (default profile, or the one set by `AWS_PROFILE`)
5. EC2 Instance Profile / ECS Task Role

If none of these match and you haven't supplied static keys, the client falls back to anonymous access (useful for
public buckets).

### Static access keys

=== "Properties"
    ```java
    Properties props = new Properties();
    props.setProperty("storage.s3.aws-access-key-id", System.getenv("AWS_ACCESS_KEY_ID"));
    props.setProperty("storage.s3.aws-secret-access-key", System.getenv("AWS_SECRET_ACCESS_KEY"));
    props.setProperty("storage.s3.region", "us-west-2");

    try (RangeReader reader = StorageFactory.openRangeReader(
            URI.create("s3://my-bucket/map.pmtiles"), props)) {
        // ...
    }
    ```

=== "SDK-injection"
    ```java
    S3Client s3 = S3Client.builder()
        .region(Region.US_WEST_2)
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create(accessKey, secretKey)))
        .build();

    try (Storage storage = S3StorageProvider.open(URI.create("s3://my-bucket/"), s3);
            RangeReader reader = storage.openRangeReader("map.pmtiles")) {
        // ...
    }
    ```

### Named profile

=== "Properties"
    ```java
    Properties props = new Properties();
    props.setProperty("storage.s3.default-credentials-profile", "production");
    // optional:
    // props.setProperty("storage.s3.use-default-credentials-provider", "true");
    ```

=== "SDK-injection"
    ```java
    S3Client s3 = S3Client.builder()
        .credentialsProvider(ProfileCredentialsProvider.create("production"))
        .region(Region.US_EAST_1)
        .build();
    Storage storage = S3StorageProvider.open(URI.create("s3://my-bucket/"), s3);
    ```

### Assume Role (STS) and other custom credential providers

The Properties surface only carries scalar values, so custom credential providers (STS assume-role chains,
SAML/SSO, AWS IAM Identity Center) go through SDK-injection:

```java
StsAssumeRoleCredentialsProvider roleProvider = StsAssumeRoleCredentialsProvider.builder()
    .stsClient(StsClient.builder().region(Region.US_EAST_1).build())
    .refreshRequest(req -> req
        .roleArn("arn:aws:iam::123456789012:role/CrossAccountAccess")
        .roleSessionName("tileverse-session"))
    .build();

S3Client s3 = S3Client.builder()
    .credentialsProvider(roleProvider)
    .region(Region.US_EAST_1)
    .build();

try (Storage storage = S3StorageProvider.open(URI.create("s3://external-bucket/"), s3);
        RangeReader reader = storage.openRangeReader("data.bin")) {
    // ...
}
```

### Full feature set (`read`, multipart upload, presigned URLs)

The 1-arg `S3StorageProvider.open(URI, S3Client)` overload is sync-only; capabilities that need the CRT async client,
transfer manager, or presigner throw `UnsupportedCapabilityException`. For full feature parity, build the bundle:

```java
S3ClientBundle bundle = S3ClientBundle.of(syncClient, asyncClient, transferManager, presigner);
try (Storage storage = S3StorageProvider.open(URI.create("s3://my-bucket/"), bundle)) {
    // read / multipart / presignGet / presignPut all work
}
```

The Properties path always goes through the SPI's full-bundle code path, so `StorageFactory.open(uri, props)` returns a
`Storage` with all four SDK objects available.

## Azure Blob Storage

### SAS token (recommended for limited access)

=== "Properties"
    ```java
    Properties props = new Properties();
    props.setProperty("storage.azure.sas-token", "sv=2020-08-04&ss=b&srt=o&sp=r&se=2024-01-01...");

    try (RangeReader reader = StorageFactory.openRangeReader(
            URI.create("https://account.blob.core.windows.net/container/blob"), props)) {
        // ...
    }
    ```

=== "SDK-injection"
    ```java
    BlobServiceClient client = new BlobServiceClientBuilder()
        .endpoint("https://account.blob.core.windows.net")
        .sasToken("sv=2020-08-04&ss=b&srt=o&sp=r&se=2024-01-01...")
        .buildClient();

    try (Storage storage = AzureBlobStorageProvider.open(
                URI.create("https://account.blob.core.windows.net/container/"), client);
            RangeReader reader = storage.openRangeReader("blob")) {
        // ...
    }
    ```

### Connection string (Azurite / dev access)

=== "Properties"
    ```java
    Properties props = new Properties();
    props.setProperty("storage.azure.connection-string",
        "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...");
    ```

=== "SDK-injection"
    ```java
    BlobServiceClient client = new BlobServiceClientBuilder()
        .connectionString("DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...")
        .buildClient();
    Storage storage = AzureBlobStorageProvider.open(uri, client);
    ```

### Account key

=== "Properties"
    ```java
    Properties props = new Properties();
    props.setProperty("storage.azure.account-key", System.getenv("AZURE_ACCOUNT_KEY"));
    ```
    The account name comes from the URI host; the key is paired with it server-side.

=== "SDK-injection"
    ```java
    StorageSharedKeyCredential cred = new StorageSharedKeyCredential(accountName, accountKey);
    BlobServiceClient client = new BlobServiceClientBuilder()
        .endpoint("https://%s.blob.core.windows.net".formatted(accountName))
        .credential(cred)
        .buildClient();
    Storage storage = AzureBlobStorageProvider.open(uri, client);
    ```

### Managed Identity / DefaultAzureCredential

For applications running on Azure infrastructure (VMs, App Service, AKS) — or anywhere the Azure default credential
chain works — there's no scalar property to carry; use SDK-injection:

```java
TokenCredential credential = new DefaultAzureCredentialBuilder().build();

BlobServiceClient client = new BlobServiceClientBuilder()
    .endpoint("https://account.blob.core.windows.net")
    .credential(credential)
    .buildClient();

try (Storage storage = AzureBlobStorageProvider.open(
            URI.create("https://account.blob.core.windows.net/container/"), client);
        RangeReader reader = storage.openRangeReader("blob")) {
    // ...
}
```

### Retry tuning

The four retry-policy scalars are exposed as Properties:

```java
Properties props = new Properties();
props.setProperty("storage.azure.max-retries", "5");
props.setProperty("storage.azure.retry-delay", "PT5S");        // ISO-8601 duration
props.setProperty("storage.azure.max-retry-delay", "PT2M");
props.setProperty("storage.azure.try-timeout", "PT60S");
```

For a fully customised `RequestRetryOptions` (secondary host, custom retry policy class), build the
`BlobServiceClient` yourself and use SDK-injection.

## Google Cloud Storage

### Application Default Credentials (ADC)

This is the default. The library automatically looks for:

1. `GOOGLE_APPLICATION_CREDENTIALS` environment variable pointing at a service account JSON.
2. `gcloud auth application-default login` credentials.
3. Attached Service Account on GCE / GKE / Cloud Run / Cloud Functions.

```java
Properties props = new Properties();
props.setProperty("storage.gcs.default-credentials-chain", "true");
// optional — disambiguates billing when multiple projects are accessible:
// props.setProperty("storage.gcs.project-id", "my-project");

try (RangeReader reader = StorageFactory.openRangeReader(
        URI.create("gs://my-bucket/data.bin"), props)) {
    // ...
}
```

### Service account key (explicit JSON file)

For cases where ADC isn't appropriate, use SDK-injection:

```java
try (FileInputStream input = new FileInputStream("/path/to/key.json")) {
    ServiceAccountCredentials creds = ServiceAccountCredentials.fromStream(input);
    com.google.cloud.storage.Storage gcs = StorageOptions.newBuilder()
        .setCredentials(creds)
        .setProjectId("my-project")
        .build()
        .getService();

    try (Storage storage = GoogleCloudStorageProvider.open(URI.create("gs://my-bucket/"), gcs);
            RangeReader reader = storage.openRangeReader("data.bin")) {
        // ...
    }
}
```

### Anonymous (public buckets)

```java
Properties props = new Properties();
props.setProperty("storage.gcs.default-credentials-chain", "false");

try (RangeReader reader = StorageFactory.openRangeReader(
        URI.create("gs://gcp-public-data-landsat/.../some.tif"), props)) {
    // ...
}
```

### fake-gcs-server (emulators)

Either rely on URI-pattern detection (`http(s)://host/storage/v1/b/...`) or set `storage.gcs.host` explicitly:

```java
Properties props = new Properties();
props.setProperty("storage.gcs.host", "http://localhost:4443");

try (Storage storage = StorageFactory.open(URI.create("gs://my-bucket/"), props);
        RangeReader reader = storage.openRangeReader("data.bin")) {
    // ...
}
```

When a host override is in effect, credentials default to anonymous (the emulator typically doesn't validate them).

## Sensitive parameters

Parameters carrying secrets are flagged with `password=true` on their `StorageParameter` declaration. Tools that render
configuration UIs (GeoServer, datastore wizards) use this flag to mask the input. Currently flagged:

- `storage.http.password`, `storage.http.bearer-token`, `storage.http.api-key`
- `storage.s3.aws-secret-access-key`
- `storage.azure.account-key`, `storage.azure.sas-token`, `storage.azure.connection-string`

There is no equivalent flag for SDK-injected credentials — the caller controls what gets logged.
