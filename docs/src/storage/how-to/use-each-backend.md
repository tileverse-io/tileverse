# Use each backend

This page shows the smallest end-to-end snippet for each supported backend, and what is particular to each. All snippets use the same `Storage` API; only the URI changes.

## Local filesystem

```java
import io.tileverse.storage.ReadHandle;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageEntry;
import io.tileverse.storage.StorageFactory;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

try (Storage storage = StorageFactory.open(URI.create("file:///data/cache/"))) {
    storage.put("hello.txt", "world".getBytes(StandardCharsets.UTF_8));
    try (Stream<StorageEntry> entries = storage.list("**/*.txt")) {
        entries.forEach(entry -> System.out.println(entry.key()));
    }
    try (ReadHandle r = storage.read("hello.txt")) {
        byte[] read = r.content().readAllBytes();
    }
}
```

A local reader opens a file channel on its first read and closes it after `storage.file.idle-timeout` (default 60 seconds) with no read in progress. It holds a file descriptor only while the channel is open, and a closed reader never reopens one. Closing a `FileStorage` leaves the readers obtained from it usable.

Range reads of local files respond to thread interrupts. A read on an interrupted thread fails with a `StorageException`, and the interrupt flag stays set. An interrupt during a read closes the file channel shared by the readers of that file; the other readers continue on a new channel. Streaming reads and writes do not respond to interrupts.

## HTTP / HTTPS (read-only)

```java
import io.tileverse.storage.ReadHandle;
import io.tileverse.storage.ReadOptions;
import io.tileverse.storage.RangeReader;
import java.nio.ByteBuffer;

try (Storage storage = StorageFactory.open(URI.create("https://example.com/data/"))) {
    // Range read via the existing RangeReader API
    try (RangeReader reader = storage.openRangeReader("file.bin")) {
        ByteBuffer buf = reader.readRange(0, 1024);
    }
    // Streaming read with offset
    try (ReadHandle r = storage.read("file.bin", ReadOptions.fromOffset(1024))) {
        byte[] tail = r.content().readAllBytes();
    }
    // Listing and writes throw UnsupportedCapabilityException
}
```

The HTTP backend does not follow redirects: a 3xx answer fails the read with `Redirect not followed`. Object stores and CDNs often answer a presigned or edge URL with a 302; open the final URL instead.

## AWS S3

```java
import io.tileverse.storage.WriteOptions;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;

// General-purpose bucket
try (Storage storage = StorageFactory.open(URI.create("s3://my-bucket/datasets/v3/"))) {
    storage.put("year=2024/file.parquet", Path.of("/local/file.parquet"), WriteOptions.defaults());
    try (Stream<StorageEntry> entries = storage.list("year=2024/**")) {
        entries.forEach(e -> System.out.println(e.key()));
    }
}

// S3 Express One Zone (Directory Bucket): same code, bucket name encodes the AZ
try (Storage storage = StorageFactory.open(
        URI.create("s3://my-bucket--usw2-az1--x-s3/datasets/"))) {
    // Capabilities reflect Express constraints: 5-min presign max, no versioning
    Optional<Duration> maxTtl = storage.capabilities().maxPresignTtl();
    System.out.println(maxTtl); // Optional[PT5M]
}
```

Authentication uses the AWS default credential chain unless you set `storage.s3.anonymous=true`, the `storage.s3.aws-access-key-id` / `storage.s3.aws-secret-access-key` pair, or `storage.s3.default-credentials-profile`. Public buckets need `storage.s3.anonymous=true`.

S3 Express One Zone differs from general-purpose buckets in three ways: its ETags are random strings rather than MD5 digests, its presigned URLs last at most 5 minutes, and its listings are not in lexicographic order.

### S3-compatible services

The S3 backend serves any service speaking the S3 API. Address the service in one of two forms:

- Keep the URL of the service and name the provider: `http(s)://endpoint/bucket/key` with `storage.provider=s3`. The endpoint comes from the URL. With no provider named, the same URL opens the read-only HTTP backend.
- Keep a canonical `s3://bucket/key` URI and point it at the service with `storage.s3.endpoint`, as below. Path-style addressing is enabled automatically.

Credentials and region come from the same parameters as for AWS:

```java
import java.util.Properties;

Properties props = new Properties();
props.setProperty("storage.s3.endpoint", "http://localhost:9000"); // MinIO
props.setProperty("storage.s3.aws-access-key-id", "minioadmin");
props.setProperty("storage.s3.aws-secret-access-key", "minioadmin");
props.setProperty("storage.s3.region", "us-east-1");

try (Storage storage = StorageFactory.open(URI.create("s3://my-bucket/datasets/"), props)) {
    storage.put("file.parquet", Path.of("/local/file.parquet"), WriteOptions.defaults());
}
```

See [Configure - Custom endpoint](configure.md#custom-endpoint-minio-and-other-s3-compatible-services) for how `storage.s3.endpoint` interacts with `force-path-style`, `region`, and credentials.

Tested or known to work:

| Service | Service URL | Notes |
| :--- | :--- | :--- |
| **MinIO** | `http://host:9000/bucket/key` | Self-hosted. |
| **Garage** | `http://host:3900/bucket/key` | Self-hosted; the test suite runs against it. |
| **Ceph RADOS Gateway (RGW)** | `https://rgw.example.com/bucket/key` | Common in OpenStack and on-premises deployments. |
| **OpenStack Swift (via `s3api` middleware)** | `https://swift.example.com/bucket/key` | The native Swift API is not supported. |
| **Cloudflare R2** | `https://<account>.r2.cloudflarestorage.com/bucket/key` | Set `storage.s3.region=auto`. |
| **Backblaze B2** | `https://s3.<region>.backblazeb2.com/bucket/key` | |
| **Wasabi** | `https://s3.<region>.wasabisys.com/bucket/key` | |
| **DigitalOcean Spaces** | `https://<region>.digitaloceanspaces.com/bucket/key` | |
| **IBM Cloud Object Storage** | `https://s3.<region>.cloud-object-storage.appdomain.cloud/bucket/key` | Use the public S3 endpoint, not the legacy Swift one. |
| **Linode Object Storage** | `https://<region>.linodeobjects.com/bucket/key` | |
| **OVHcloud Object Storage (S3 endpoint)** | `https://s3.<region>.cloud.ovh.net/bucket/key` | OVH offers both S3 and Swift endpoints; use the S3 one. |

Native protocols such as OpenStack Swift, Azure Files (SMB), HDFS or FTP are out of scope; [Why tileverse-storage?](../explanation/why.md#when-not-to-use-this-library) explains why.

## Azure Blob Storage

```java
import java.time.Duration;

try (Storage storage = StorageFactory.open(URI.create("az://myaccount/my-container/datasets/"))) {

    // Generate a SAS-based presigned URL valid for 15 minutes
    if (storage.capabilities().presignedUrls()) {
        URI presigned = storage.presignGet("file.bin", Duration.ofMinutes(15));
    }
}
```

Authentication uses `DefaultAzureCredential` by default (env vars, managed identity, Azure CLI). Provide `storage.azure.connection-string`, `storage.azure.account-key` or `storage.azure.sas-token` for explicit credentials, or `storage.azure.anonymous=true` for a public container.

`az://account/container` resolves to the public `https://<account>.blob.core.windows.net` endpoint. The `https://` URL of a container opens this backend when `storage.provider=azure` is set; the endpoint then comes from the URL. Set `storage.azure.endpoint` to redirect an `az://` URI to an emulator, a sovereign cloud (Azure Government, Azure China), or a custom domain. The value is the full Blob service endpoint, including the account where the service expects it in the path (the Azurite emulator embeds the account in the path):

```java
Properties props = new Properties();
props.setProperty("storage.azure.endpoint", "http://127.0.0.1:10000/devstoreaccount1"); // Azurite
props.setProperty("storage.azure.connection-string", "UseDevelopmentStorage=true");

try (Storage storage = StorageFactory.open(URI.create("az://devstoreaccount1/my-container/"), props)) {
    storage.put("file.bin", new byte[100]);
}
```

A connection string naming `BlobEndpoint` or `EndpointSuffix` sets the endpoint itself, and overrides `storage.azure.endpoint`.

## Azure Data Lake Storage Gen2 (HNS)

```java
// abfs:// URI form
try (Storage storage = StorageFactory.open(URI.create(
        "abfss://my-fs@myaccount.dfs.core.windows.net/data/"))) {

    storage.put("a/b/c/file.bin", new byte[100]);

    // Atomic move (HNS only)
    if (storage.capabilities().atomicMove()) {
        storage.move("a/b/c/file.bin", "a/b/d/file.bin");
    }
}

// https:// URL of the dfs endpoint: name the provider
Properties props = new Properties();
props.setProperty("storage.provider", "azure-datalake");
try (Storage storage = StorageFactory.open(URI.create(
        "https://myaccount.dfs.core.windows.net/my-fs/data/"), props)) {
    // ...
}
```

## Google Cloud Storage

```java
import java.nio.file.Path;

// Flat keyspace bucket
try (Storage storage = StorageFactory.open(URI.create("gs://my-bucket/datasets/"))) {
    storage.put("year=2024/file.parquet", Path.of("/local/file.parquet"), WriteOptions.defaults());

    // Hierarchical Namespace bucket: same code path; HNS auto-detected at open.
    System.out.println("Real directories: " + storage.capabilities().realDirectories());
    System.out.println("Atomic move: " + storage.capabilities().atomicMove());
}
```

Authentication uses Google application default credentials (`GOOGLE_APPLICATION_CREDENTIALS` env var, `gcloud auth application-default login`, GCE/GKE/Cloud Run metadata). Opening fails when none is found. Public buckets need `storage.gcs.anonymous=true`.
## Pattern syntax

The single argument to `Storage.list(String pattern)` is a shell-style glob (or a plain prefix). The longest non-glob prefix seeds the listing; `**` (or any `/` in the glob portion) makes the walk recursive; the pattern filters results client-side with DuckDB-style glob rules: `*` and `?` stay within one path segment, `**` crosses `/`, and `**/` also matches zero directories.

```java
// Pure prefix: immediate children of the prefix
storage.list("");                          // root listing
storage.list("data/");                      // immediate children of data/

// Glob characters at the leaf: immediate children with filter
storage.list("data/*.parquet");             // depth-1 .parquet under data/

// ** anywhere: recursive walk
storage.list("data/**");                    // every descendant of data/
storage.list("data/**/*.parquet");          // every .parquet at any depth under data/
storage.list("**.parquet");                 // every .parquet anywhere

// Brace alternation: the '/' in the glob portion implies recursion to reach the matched depth
storage.list("data/{a,b}/file.txt");        // walks data/, matches data/a/file.txt or data/b/file.txt
```

For pagination and metadata-fetch tuning, use the two-arg overload:

```java
import io.tileverse.storage.ListOptions;

storage.list("data/**", ListOptions.builder().pageSize(500).build());
```

## Conditional writes (compare-and-swap)

```java
import io.tileverse.storage.WriteOptions;
import io.tileverse.storage.PreconditionFailedException;

// Create-only (atomic put-if-absent)
try {
    storage.put("locks/leader", new byte[]{1},
            WriteOptions.builder().ifNotExists(true).build());
} catch (PreconditionFailedException e) {
    // Someone else won the race
}

// Compare-and-swap on ETag / generation
String currentEtag = storage.stat("config.json").orElseThrow().etag().orElseThrow();
try {
    storage.put("config.json", newConfigBytes,
            WriteOptions.builder().ifMatchEtag(currentEtag).build());
} catch (PreconditionFailedException e) {
    // Someone else updated the object since we read it
}
```
