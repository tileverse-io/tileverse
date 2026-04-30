# Quick Start

This page shows the smallest end-to-end snippet for each supported backend. All snippets use the same `Storage` API; only the URI changes.

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

Authentication uses the AWS default credential chain unless you provide `storage.s3.aws-access-key-id` / `storage.s3.aws-secret-access-key` in a `StorageConfig`.

## Azure Blob Storage

```java
import java.time.Duration;

try (Storage storage = StorageFactory.open(URI.create(
        "https://myaccount.blob.core.windows.net/my-container/datasets/"))) {

    // Generate a SAS-based presigned URL valid for 15 minutes
    if (storage.capabilities().presignedUrls()) {
        URI presigned = storage.presignGet("file.bin", Duration.ofMinutes(15));
    }
}
```

Authentication uses `DefaultAzureCredential` by default (env vars, managed identity, Azure CLI). Provide `storage.azure.account-key` or `storage.azure.sas-token` in `StorageConfig` for explicit credentials.

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

// https:// URI form to the dfs endpoint also works
try (Storage storage = StorageFactory.open(URI.create(
        "https://myaccount.dfs.core.windows.net/my-fs/data/"))) {
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

Authentication uses Google application default credentials (`GOOGLE_APPLICATION_CREDENTIALS` env var, `gcloud auth application-default login`, GCE/GKE/Cloud Run metadata).

## Pattern syntax

The single argument to `Storage.list(String pattern)` is a shell-style glob (or a plain prefix). The longest non-glob prefix seeds the listing; `**` (or any `/` in the glob portion) makes the walk recursive; the glob portion filters results client-side via the JDK matcher (`FileSystem.getPathMatcher("glob:...")`).

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

## Resource cleanup

Always close `Storage`, the listing `Stream`, and any `InputStream` / `OutputStream` returned by Storage methods. The internal SDK client cache is reference-counted, so closing one `Storage` instance does not affect siblings against the same account.

```java
try (Storage storage = StorageFactory.open(uri);
        Stream<StorageEntry> listing = storage.list("**")) {
    listing.forEach(entry -> /* ... */);
}
```
