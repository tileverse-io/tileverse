# Authentication Setup

This guide details how to configure credentials for cloud storage and secure HTTP endpoints in `tileverse-storage` 2.0.

## Two configuration paths

For every backend you have a choice of two paths:

- **Properties-driven**: pass a `Properties` map (or a `StorageConfig`) to `StorageFactory.open(URI, Properties)` and
  call `Storage.openRangeReader` (or any other `Storage` method) on the returned handle. This is what GeoTools,
  GeoServer, and Spring property-binding setups use.
- **SDK-injection**: build your own SDK client (`HttpClient`, `S3AsyncClient`, `BlobServiceClient`, GCS `Storage`) and pass it
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

    URI root = URI.create("https://api.example.com/");
    try (Storage storage = StorageFactory.open(root, props);
            RangeReader reader = storage.openRangeReader("data.bin")) {
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

    URI root = URI.create("https://api.provider.com/");
    try (Storage storage = StorageFactory.open(root, props);
            RangeReader reader = storage.openRangeReader("data")) {
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

The S3 backend takes the first credentials configured in this order:

1. `storage.s3.anonymous=true`: unsigned requests, for public buckets.
2. `storage.s3.aws-access-key-id` together with `storage.s3.aws-secret-access-key`: static keys.
3. `storage.s3.default-credentials-profile`: that profile only, whatever the environment sets.
4. Nothing set: the AWS SDK v2 default credentials provider chain.

### Default discovery order

The default chain looks for credentials in this order (standard AWS behavior):

1. System properties (`aws.accessKeyId`, `aws.secretAccessKey`)
2. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
3. Web Identity Token (EKS/K8s)
4. `~/.aws/credentials` and `~/.aws/config` (the profile named by `AWS_PROFILE`, or `default`)
5. ECS container credentials
6. EC2 instance profile

The chain never falls back to anonymous access: public buckets need `storage.s3.anonymous=true`. To pick a profile
per environment, leave `storage.s3.default-credentials-profile` unset and set `AWS_PROFILE`.

### Static access keys

=== "Properties"
    ```java
    Properties props = new Properties();
    props.setProperty("storage.s3.aws-access-key-id", System.getenv("AWS_ACCESS_KEY_ID"));
    props.setProperty("storage.s3.aws-secret-access-key", System.getenv("AWS_SECRET_ACCESS_KEY"));
    props.setProperty("storage.s3.region", "us-west-2");

    try (Storage storage = StorageFactory.open(URI.create("s3://my-bucket/"), props);
            RangeReader reader = storage.openRangeReader("map.pmtiles")) {
        // ...
    }
    ```

=== "SDK-injection"
    ```java
    S3AsyncClient s3 = S3AsyncClient.builder()
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
    ```

=== "SDK-injection"
    ```java
    S3AsyncClient s3 = S3AsyncClient.builder()
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

S3AsyncClient s3 = S3AsyncClient.builder()
    .credentialsProvider(roleProvider)
    .region(Region.US_EAST_1)
    .build();

try (Storage storage = S3StorageProvider.open(URI.create("s3://external-bucket/"), s3);
        RangeReader reader = storage.openRangeReader("data.bin")) {
    // ...
}
```

### Presigned URLs

A `Storage` over a caller-supplied `S3AsyncClient` reads and writes; presigned URLs throw
`UnsupportedCapabilityException`. Open the `Storage` from a URI or a `StorageConfig` to presign.

## Azure Blob Storage

### SAS token (recommended for limited access)

=== "Properties"
    ```java
    Properties props = new Properties();
    props.setProperty("storage.azure.sas-token", "sv=2020-08-04&ss=b&srt=o&sp=r&se=2024-01-01...");

    try (Storage storage = StorageFactory.open(
                URI.create("https://account.blob.core.windows.net/container/"), props);
            RangeReader reader = storage.openRangeReader("blob")) {
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

This is the default: with `storage.gcs.anonymous` unset, the library looks for:

1. `GOOGLE_APPLICATION_CREDENTIALS` environment variable pointing at a service account JSON.
2. `gcloud auth application-default login` credentials.
3. Attached Service Account on GCE / GKE / Cloud Run / Cloud Functions.

Opening a `Storage` fails when none is found; the credentials never fall back to anonymous access.

```java
Properties props = new Properties();
// optional — disambiguates billing when multiple projects are accessible:
// props.setProperty("storage.gcs.project-id", "my-project");

try (Storage storage = StorageFactory.open(URI.create("gs://my-bucket/"), props);
        RangeReader reader = storage.openRangeReader("data.bin")) {
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
props.setProperty("storage.gcs.anonymous", "true");

try (Storage storage = StorageFactory.open(URI.create("gs://gcp-public-data-landsat/"), props);
        RangeReader reader = storage.openRangeReader(".../some.tif")) {
    // ...
}
```

### fake-gcs-server (emulators)

Either rely on URI-pattern detection (`http(s)://host/storage/v1/b/...`) or set `storage.gcs.endpoint` explicitly. The value is a full endpoint URL with scheme, not a bare hostname:

```java
Properties props = new Properties();
props.setProperty("storage.gcs.endpoint", "http://localhost:4443");
props.setProperty("storage.gcs.anonymous", "true");

try (Storage storage = StorageFactory.open(URI.create("gs://my-bucket/"), props);
        RangeReader reader = storage.openRangeReader("data.bin")) {
    // ...
}
```

An endpoint override keeps the credentials rules of the default endpoint: emulators need `storage.gcs.anonymous=true`,
and a private or restricted endpoint can use Application Default Credentials.

The earlier key `storage.gcs.host` is still accepted: it is promoted to `storage.gcs.endpoint` when a configuration is loaded.

## Requester Pays buckets

Some publicly accessible buckets are configured for **Requester Pays**: the bucket owner pays for storage, and the
**requester** pays for egress and per-operation costs. Hitting such a bucket without the protocol-level opt-in returns
`403 Forbidden` (S3) or `400 UserProjectMissing` (GCS).

!!! warning "Authentication is required"
    Requester Pays cannot be combined with anonymous access on either S3 or GCS. The cloud needs an authenticated
    identity to determine who to bill. Configure real credentials (default chain, named profile, static key, or
    SDK injection) before enabling these parameters.

    - S3: AWS rejects anonymous (unsigned) requests against Requester Pays buckets unconditionally. Do not set
      `storage.s3.requester-pays=true` together with `storage.s3.anonymous=true`.
    - GCS: the requester must be an authenticated principal that holds `serviceusage.services.use` on the project
      named in `storage.gcs.user-project`. Anonymous calls cannot satisfy either requirement.

### S3

Set `storage.s3.requester-pays=true` to add `x-amz-request-payer: requester` to every request:

```java
Properties props = new Properties();
props.setProperty("storage.s3.requester-pays", "true");
// Requester Pays buckets reject unsigned requests. With no credential parameter set, the AWS default credentials
// chain supplies the requester's credentials (not the bucket owner's).
props.setProperty("storage.s3.region", "us-east-1");

URI uri = URI.create("s3://noaa-nexrad-level2/2024/01/01/KAMA/");
try (Storage storage = StorageFactory.open(uri, props)) {
    // stat / list / openRangeReader / ... succeed and the requester is billed
}
```

The flag is silently ignored by regular buckets, so it is safe to set unconditionally for callers that may target both.

### GCS

Set `storage.gcs.user-project=<billing-project-id>` to attach `userProject=<id>` to every request, alongside an
authenticated credentials chain that has `serviceusage.services.use` on that project:

```java
Properties props = new Properties();
props.setProperty("storage.gcs.user-project", "my-billing-project");
// Requester Pays needs authenticated credentials on the billing project: leave storage.gcs.anonymous unset.

URI uri = URI.create("gs://requester-pays-public-bucket/path/data.bin");
try (Storage storage = StorageFactory.open(uri, props)) {
    // ...
}
```

Azure has no equivalent billing model and is not affected.

## Sensitive parameters

Parameters carrying secrets are flagged with `password=true` on their `StorageParameter` declaration. Tools that render
configuration UIs (GeoServer, datastore wizards) use this flag to mask the input. Currently flagged:

- `storage.http.password`, `storage.http.bearer-token`, `storage.http.api-key`
- `storage.s3.aws-secret-access-key`
- `storage.azure.account-key`, `storage.azure.sas-token`, `storage.azure.connection-string`

There is no equivalent flag for SDK-injected credentials — the caller controls what gets logged.
