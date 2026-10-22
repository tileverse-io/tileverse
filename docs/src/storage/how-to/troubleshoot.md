# Troubleshoot

This page starts with common error messages and their usual causes. The sections after the table cover each problem.

## Common error messages

| Error | Likely cause | See |
|-------|--------------|-----|
| `UnsupportedClassVersionError` | Java older than 17 | Use Java 17 or later |
| `No suitable provider found for URI: ...` | The backend module of the URI scheme is missing | [Missing backend module](#missing-backend-module) |
| `ClassNotFoundException` for a cloud SDK class | The backend module is missing | [Missing backend module](#missing-backend-module) |
| `Unable to load credentials from any of the providers in the chain` | No AWS credentials found | [S3 credentials](#s3-credentials) |
| `AccessDeniedException: access denied for key '...'` | A missing permission, or a missing object | [Access denied](#access-denied) |
| `Authentication failed for URI: ..., status code: 403` | The `http(s)` URL of a private bucket, opened by the HTTP backend | Set `storage.provider` to `s3`, `azure` or `gcs`, or use the native scheme (`s3://`, `az://`, `gs://`) |
| `BlobStorageException: AuthenticationFailed` | Wrong Azure credentials, or an expired SAS token | [Azure authentication](#azure-authentication) |
| `No Application Default Credentials found for Google Cloud Storage` | No Google credentials found | [GCS credentials](#gcs-credentials) |
| `NotFoundException` | The object does not exist | Check the key and the URI of the container |
| `TransientStorageException: Connection timeout after ...` | A network timeout | [Connection timeouts](#connection-timeouts) |
| `Connection Manager failed to acquire a connection within the defined timeout` | Too many S3 requests in flight | [S3 requests waiting for a connection](#s3-requests-waiting-for-a-connection) |
| `Response missing required ETag header` | A caller-built `crtBuilder` S3 client against an S3-compatible service | [S3-compatible services without an ETag header](#s3-compatible-services-without-an-etag-header) |
| `IOException: Too many open files` | Many local readers in use at once | [Too many open files](#too-many-open-files) |
| `Stale file handle` | An NFS or SMB export changed under the reader | [Stale NFS file handles](#stale-nfs-file-handles) |
| `OutOfMemoryError` | Several `CacheManager` instances, or caching a streaming workload | [Memory and the cache](#memory-and-the-cache) |

## Dependencies

### Dependency conflicts

Maven or Gradle reports conflicts with the AWS, Azure, or Google Cloud SDKs. Import `io.tileverse:tileverse-bom`: it imports the cloud SDK versions used to build and test the library. Declare it before any other cloud SDK BOM, because Maven takes a managed version from the first BOM declaring it.

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>io.tileverse</groupId>
            <artifactId>tileverse-bom</artifactId>
            <version>2.1.0</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>
```

### Missing backend module

Each cloud backend lives in its own module. Without it, a URI of its scheme fails to open with `No suitable provider found for URI`, and code that uses its SDK types fails with `ClassNotFoundException`. Add `tileverse-storage-s3`, `tileverse-storage-azure`, `tileverse-storage-gcs`, or `tileverse-storage-all` for all three. [Getting Started](../../getting-started.md) lists the modules.

## Credentials and access

### S3 credentials

With no credential parameter, the S3 backend uses the AWS default credential chain, and reports `Unable to load credentials ...` when the chain finds none. Provide credentials in one of these ways:

- Environment variables:
  ```bash
  export AWS_ACCESS_KEY_ID=your-access-key
  export AWS_SECRET_ACCESS_KEY=your-secret-key
  export AWS_REGION=us-west-2
  ```
- The credentials file, `~/.aws/credentials`:
  ```ini
  [default]
  aws_access_key_id = your-access-key
  aws_secret_access_key = your-secret-key
  ```
- An IAM role, on EC2, ECS or EKS. Nothing to configure.

The chain never falls back to anonymous access. To read a public bucket, set `storage.s3.anonymous=true`. [Authenticate](authenticate.md#aws-s3) lists all the options.

### Access denied

`AccessDeniedException` means the identity lacks a permission on the object, such as `s3:GetObject` on S3. On S3, a missing object also returns 403 when the identity may not list the bucket.

On AWS, a request sent to the wrong region is sent again to the region of the bucket. Presigned URLs and S3-compatible services need the right region: set `storage.s3.region`.

### Azure authentication

`AuthenticationFailed` means the credentials do not match the account. Check the connection string or the account key, and the expiry of a SAS token. The `se` field of the token holds its expiry time:

```bash
echo "$SAS_TOKEN" | tr '&' '\n' | grep '^se='
```

### GCS credentials

With `storage.gcs.anonymous` unset, opening a GCS `Storage` fails with `No Application Default Credentials found` when no credentials are found. Set up Application Default Credentials:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
# or, for your own account:
gcloud auth application-default login
```

To read a public bucket, set `storage.gcs.anonymous=true`. An `AccessDeniedException` caused by a GCS error 403 means the identity lacks a permission on the bucket.

## Performance and memory

### Slow reads

Each remote read pays a round trip. Cache the reads, with `CachingRangeReader.of(reader)` or with `storage.caching.enabled=true` on the configuration passed to `StorageFactory.open`. The file backend ignores that parameter. For parts of a file read again and again, such as a header or an index, declare block-aligned regions. [Configure for performance](configure.md) describes both.

### Memory and the cache

The shared range cache holds at most 20% of the maximum heap, weighed by cached bytes, and an entry expires 60 seconds after its last access. The cache stays empty until readers are wrapped in a `CachingRangeReader` or `storage.caching.enabled` is set.

- Each `CacheManager.newInstance()` owns a cache with a budget of its own. Readers built without an explicit manager share the default one: keep it that way.
- A job reading a file front to back gains nothing from the cache, and fills it with bytes read once. Use the reader returned by `Storage.openRangeReader` directly.
- `BlockAlignedRangeReader.alignWholeFile()` turns each read inside the file into cached blocks. Declare only the regions read repeatedly, and let tile or row payloads pass through as exact ranges.

### Low cache hit rate

The cache keys on the exact `(offset, length)` of a read. Two reads share an entry only when they ask for the same range, or when they fall in the same block of a block-aligned region. An entry unused for 60 seconds expires, and a hit rate measured across idle periods drops for that reason alone.

```java
import io.tileverse.cache.CacheStats;
import io.tileverse.storage.cache.CachingRangeReader;

if (reader instanceof CachingRangeReader cachingReader) {
    CacheStats stats = cachingReader.getCacheStats();
    System.out.printf("hit rate %.2f%%, %d hits, %d misses, %d evictions, %d entries%n",
            stats.hitRate() * 100, stats.hitCount(), stats.missCount(), stats.evictionCount(), stats.entryCount());
}
```

The statistics cover the shared cache of the `CacheManager`, across the readers using it, not this reader alone.

## Network

### Connection timeouts

The HTTP backend reports `TransientStorageException: Connection timeout after 5000 milliseconds`. Raise `storage.http.timeout-millis`, 5000 by default:

```java
Properties props = new Properties();
props.setProperty("storage.http.timeout-millis", "30000");

try (Storage storage = StorageFactory.open(parent, props);
        RangeReader reader = storage.openRangeReader(leaf)) {
    // ...
}
```

For S3, raise the `io.tileverse.storage.s3-http-client.connection-timeout` system property, 2 seconds by default; see [System Properties](configure.md#system-properties). For a client setting without a parameter, build the SDK client yourself and pass it to the provider, as described in [Configure for performance](configure.md#provider-specific-tuning).
### Proxy

The HTTP and S3 backends use the proxy settings of the JVM:

```bash
-Dhttps.proxyHost=proxy.company.com -Dhttps.proxyPort=8080
```

The S3 backend also reads the `http_proxy`, `https_proxy` and `no_proxy` environment variables, in lowercase only. For another backend, configure the proxy on its SDK client and pass the client to the provider, as described in [Authenticate](authenticate.md#two-configuration-paths).

### TLS certificates

For an HTTPS endpoint with a self-signed certificate or a private CA, the HTTP, Azure and GCS backends use the trust store of the JVM. Add the certificate to it:

```bash
keytool -import -alias custom-cert -file cert.crt -keystore $JAVA_HOME/lib/security/cacerts
```

The S3 backend uses the trust store of the operating system instead. For a MinIO or Ceph service with a private CA, install the CA in the operating system. The Java SDK does not read `AWS_CA_BUNDLE`.

During development, `storage.http.trust-all-certificates=true` disables the certificate checks of the HTTP backend; the other backends have no such setting. Never use it in production.

### S3-compatible services without an ETag header

A gateway exporting an existing tree of files may serve objects without an `ETag` header: the objects placed directly in its backend have none, and the objects written through the S3 API have one. A `Storage` opened from a URI or a `StorageConfig` reads both. A client built with `S3AsyncClient.crtBuilder()` and passed to `S3StorageProvider.open(URI, S3AsyncClient)` fails with `Response missing required ETag header`. Pass a client built with `S3AsyncClient.builder()` instead.

### S3 requests waiting for a connection

S3 requests fail with `Connection Manager failed to acquire a connection within the defined timeout`. The S3 storages opened from a URI or a `StorageConfig` send their requests through one connection pool per host, shared by the process. A request waits 30 seconds for a free connection, then fails. The SDK retries some of those failures, up to 3 times each; when many requests fail together, most of them get no retry.

Raise the pool size, `io.tileverse.storage.s3-http-client.max-concurrency`, or keep fewer requests in flight against the host: lower `storage.batch.max-in-flight-fetches` (default 8) or the number of concurrent readers. A multipart upload keeps an eighth of the pool in flight while it runs. [System Properties](configure.md#system-properties) gives the default pool size.

## Local files

### Too many open files

A server opening many local readers fails with `IOException: Too many open files`. A local reader holds a file descriptor only while its channel is open: from the first read until `storage.file.idle-timeout` (default 60 seconds) elapses with no read in progress. Descriptors pile up when many readers are read within the same minute, or when readers are opened and never closed.

- Close readers when a request is done with them. A closed reader never reopens its channel.
- Shorten the idle timeout, for example `storage.file.idle-timeout=PT10S`.
- Raise the descriptor limit (`ulimit -n`) when the working set is that large.

### Stale NFS file handles

Reads of a file on an NFS or SMB mount fail after the export was remounted or the file was replaced on the server. The operating system reports `Stale file handle` (Linux) or `Stale NFS file handle` (macOS, BSD).

Range reads recover by themselves: the reader drops the stale channel, opens the file again by its real path, and resumes after the bytes already read, up to three attempts per call. A read that still fails reports `Read failed after 3 attempts`: the mount stays stale.

An `AccessDeniedException` on a local file means the process cannot read it: check the permissions of the file.

## Debug logging

Set the log level of `io.tileverse.storage` to `DEBUG` in your logging configuration. The SDKs log under `software.amazon.awssdk` (S3), `com.azure` (Azure) and `com.google.cloud` (GCS).

`-Djdk.httpclient.HttpClient.log=requests,headers` logs the requests and headers of the JDK HTTP client, used by the HTTP and Azure backends.

## Getting help

Search the [GitHub issues](https://github.com/tileverse-io/tileverse/issues). To report a problem, include the library version, the Java version, the full error message and stack trace, and a minimal example.
