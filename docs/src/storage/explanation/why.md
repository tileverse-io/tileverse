# Why tileverse-storage?

Java has no shortage of cloud-storage abstractions. None of them fits the constraints of the GeoTools, GeoServer, GeoServer Cloud and imageio-ext ecosystem at the same time. `tileverse-storage` is the I/O layer for that stack, not a general-purpose blob library. This page lists those constraints, to help you decide whether they match yours.

## The audience

`tileverse-storage` is built for a specific class of consumer:

- **Server-side, often inside Spring.** GeoServer is Spring-based, GeoServer Cloud uses Spring Boot, and many GeoTools datastores run in either or standalone.
- **Cloud-optimized geospatial formats.** PMTiles, Cloud Optimized GeoTIFF (COG), GeoParquet and FlatGeobuf read object storage mostly with byte-range requests, not whole-object downloads.
- **One curated set of cloud SDKs.** [`cloud-dependencies-bom`](https://github.com/tileverse-io/cloud-dependencies-bom) pins the versions of the AWS, Azure and Google Cloud Storage SDKs, and of Jackson. It also excludes the default HTTP transports of those SDKs, because they pull in conflicting Netty versions: `netty-nio-client` and `apache-client` from the AWS SDK, `azure-core-http-netty` from the Azure SDK, and the gRPC transport from the GCS client. Each storage module picks its own HTTP client: the AWS CRT HTTP client for S3, the JDK HTTP client for Azure, and the HTTP/JSON API for GCS. A deployment that combines several cloud SDKs has no Netty in its dependency graph. `imageio-ext`, `geowebcache`, `geoserver` and `geoserver cloud` consume the same BOM. An abstraction with its own dependency tree (Hadoop, Micronaut, a Spring Boot starter) would bring the Netty conflict back.
- **No dependency injection framework.** The library works in Spring and Spring Boot applications, in standalone GeoTools applications, and in command-line tools, without imposing a container on the host.

If your workload does not share these constraints, the library may not be the right tool: see [When not to use this library](#when-not-to-use-this-library).

## What's missing in the existing options

About a dozen libraries solve some of the same problems. None of them solves all of them for this audience.

| Library | What it gives you | Why it doesn't fit |
| :--- | :--- | :--- |
| **Apache Hadoop `FileSystem`** (S3A, ABFS, GCS) | A mature, widely used multi-cloud abstraction. | Brings the whole Hadoop dependency tree, about 80 MB of transitive dependencies, and the GeoServer ecosystem avoids it. [Parquetry](https://github.com/tileverse-io/parquetry) reads GeoParquet through `tileverse-storage` without it. |
| **Apache jclouds `BlobStore`** | The closest in scope: S3, Azure, GCS, Swift. | A heavyweight Guice-based bootstrap, slowed development, and no byte-range or caching API. |
| **Micronaut Object Storage** (`io.micronaut.objectstorage`) | A modern, slim API similar to `Storage`. | Built around the Micronaut `ApplicationContext`: a Spring application would run two DI containers and annotation processors. `listObjects()` returns a `Set<String>`, with no metadata and no glob. No range reads. |
| **Spring Cloud Resource** (`S3Resource`, `BlobResource`, `GoogleStorageResource`) | First-class in Spring Boot; one `Resource` per blob. | One blob at a time: no listing, no range reads, no presigning in the core. Does not work outside Spring. |
| **JDK NIO `FileSystemProvider`** (`s3-nio-spi`, `google-cloud-nio`, `azure-storage-blob-nio`) | The standard `Path` and `Files` API. | Cloud semantics (no real directories, presigned URLs, conditional writes, multipart uploads, capability differences) don't fit `Path` and `Files`; each provider adds its own non-standard extensions. |
| **Apache Commons VFS** | Many protocols (FTP, SFTP, HTTP, S3 through a third party). | File-system oriented, not cloud-first; lacks modern authentication flows and conditional writes. |
| **Apache Iceberg `FileIO` / Apache Parquet `InputFile`** | A minimal byte-oriented I/O interface. | One blob only: no listing, copy, delete or presigning. Designed as an injection point, not a complete API. |
| **The cloud SDKs directly** (AWS SDK v2, Azure SDK, GCS Java client) | Full control, no abstraction. | Each consumer writes its own caching, range fetching, retries and listing. `tileverse-storage` exists to share that code. |

## What `tileverse-storage` does differently

### `RangeReader` is a first-class type

Cloud-optimized formats read a small header, jump to a directory, then issue many small byte-range requests per query. None of the general-purpose abstractions above has a thread-safe byte-range reader at its core. A `RangeReader` composes with `CachingRangeReader`, an in-memory cache of exact ranges, and with `BlockAlignedRangeReader`, a decorator fetching the declared regions as whole blocks.

### Capabilities are a record

`StorageCapabilities` is a record describing what a backend supports: `rangeReads`, `list`, `realDirectories`, `bulkDelete`, `bulkDeleteBatchLimit`, `serverSideCopy`, `atomicMove`, `presignedUrls`, `maxPresignTtl`, `versioning`, `strongReadAfterWrite`, and more. Callers can check it before calling, or catch the `UnsupportedCapabilityException` thrown by an unsupported operation. Hadoop's `hasPathCapabilities` comes closest; none of the other libraries describes capabilities as a typed record.

### Listings tell files, prefixes and directories apart

```java
sealed interface StorageEntry {
    record File(String key, long size, Instant lastModified, ...) implements StorageEntry {}
    record Prefix(String key) implements StorageEntry {}      // synthetic prefix on flat object stores
    record Directory(String key, ...) implements StorageEntry {}  // real directory on FS / ADLS Gen2 HNS / GCS HNS
}
```

Most abstractions either present everything as a directory (Hadoop) or use a single metadata type (jclouds). The sealed hierarchy keeps the difference visible, and pattern matching over it is exhaustive.

### No dependency injection

`StorageFactory.open(URI)` finds a `StorageProvider` through the standard `ServiceLoader`: no Spring context, no Micronaut bean discovery, no annotation processor. The library works the same inside Spring, Spring Boot, Micronaut, command-line tools, and libraries with no container at all.

### Caching is configuration

The `storage.caching.enabled` parameter (off by default) makes `StorageFactory` wrap each reader opened by the `Storage` in a `CachingRangeReader`. The backends never deal with caching themselves.

## Adapters to other APIs

Other ecosystems can reach `tileverse-storage` through thin adapters. [Parquetry](https://github.com/tileverse-io/parquetry), a separate project, reads Parquet and GeoParquet through `tileverse-storage` without Hadoop. `tileverse-storage-core` ships adapters to JDK and ImageIO types: `RangeReader.get()` returns a `SeekableByteChannel`, and the `io.tileverse.storage.adapters` package provides an ImageIO `ImageInputStream` and `java.io.DataInput` views over channels. Adapters to a NIO `FileSystemProvider`, Spring `Resource`, Micronaut `ObjectStorageOperations` or Iceberg `FileIO` would each be a thin module; none exists yet.

## When *not* to use this library

Pick something else when:

- **Your microservice uploads and downloads whole user attachments.** Use Spring Cloud Resource, Micronaut Object Storage, or the cloud SDK directly. The range-read and caching code would go unused.
- **You work inside a Hadoop-based pipeline (Spark, Flink, Hive).** Use `org.apache.hadoop.fs.FileSystem` with the standard cloud connectors.
- **You need a transactional document store or a queue.** This is a blob abstraction, not a database.
- **Your application is Micronaut-native, and a `Set<String>` listing is enough.** Micronaut Object Storage is well supported and integrated with the Micronaut runtime.
- **Your data sits behind a protocol other than S3** (native OpenStack Swift, Azure Files over SMB, HDFS through WebHDFS, FTP or SFTP). These are out of scope: most deployments also offer an S3-compatible endpoint, and the S3 backend serves it (Ceph RGW, MinIO, Swift through the `s3api` middleware, R2, B2, Wasabi, IBM COS, OVHcloud). See the [list of S3-compatible services](../how-to/use-each-backend.md#s3-compatible-services).
