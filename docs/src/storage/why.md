# Why tileverse-storage?

Java has no shortage of cloud-storage abstractions. Why introduce another one?

The short answer: **none of the existing options fit the constraints of the GeoTools / GeoServer / GeoServer Cloud / imageio-ext ecosystem at the same time.** `tileverse-storage` is the smallest abstraction that does — designed as the I/O substrate that the rest of the geo stack can build on, not as a general-purpose blob library.

This page makes that case explicitly so adopters can decide whether the library fits *their* constraints, and so the project's scope stays disciplined.

## The audience

`tileverse-storage` is built for a specific class of consumer:

- **Server-side, embedded in a Spring or Spring Boot application.** GeoServer is Spring-based; GeoServer Cloud uses Spring Boot; many GeoTools datastores are loaded into either or run standalone.
- **Cloud-optimized geospatial formats.** PMTiles, Cloud Optimized GeoTIFF (COG), GeoParquet, FlatGeobuf — all of which hit object storage with **byte-range reads** as the dominant access pattern, not whole-object downloads.
- **Curated cloud-SDK dependency graph.** [`cloud-dependencies-bom`](https://github.com/tileverse-io/cloud-dependencies-bom) curates versions of the cloud-storage SDKs (AWS SDK v2, Azure SDK, Google Cloud Storage Java client, Caffeine, Jackson) and — load-bearing detail — *excludes* Netty from those SDKs' transitive graphs. The AWS S3 SDK and Azure SDK ship with default HTTP clients (`netty-nio-client`, `azure-core-http-netty`) that pull in conflicting Netty versions; the BOM excludes both and substitutes JDK-native HTTP clients (and AWS's CRT client) so a deployment that combines multiple cloud SDKs has no Netty in its transitive graph at all. This BOM is consumed by `imageio-ext`, `geowebcache`, `geoserver`, `geoserver cloud`, and `tileverse-storage`, so a deployment that combines them lands on a single, mutually-compatible, Netty-free set of SDK versions. Pulling in an abstraction that brings its own dependency surface (Hadoop, Micronaut, a Spring Boot starter) silently reintroduces exactly the Netty conflict the BOM exists to eliminate.
- **No DI framework assumption.** The library has to work in Spring apps, Spring Boot apps, GeoTools standalone apps, and command-line ingest tools — without forcing a specific DI container on the host.

If your workload doesn't match all of these, the library may not be the right tool — see [When *not* to use this library](#when-not-to-use-this-library) below.

## What's missing in the existing options

There are about a dozen libraries that solve some of the same problems. None of them solve all of them at the same time, *for this audience*.

| Library | What it gives you | Why it doesn't fit |
| :--- | :--- | :--- |
| **Apache Hadoop `FileSystem`** (S3A, ABFS, GCS) | Mature multi-cloud abstraction; battle-tested. | Drags the entire Hadoop dependency tree (~80 MB transitive). Explicit non-goal for the GeoServer ecosystem; the GeoParquet branch of this project removes Hadoop precisely to escape it. |
| **Apache jclouds `BlobStore`** | Closest direct competitor in scope; supports S3, Azure, GCS, Swift. | Heavyweight Guice-based bootstrap; development has slowed; no first-class byte-range or caching API. |
| **Micronaut Object Storage** (`io.micronaut.objectstorage`) | Modern, slim, similar shape to `Storage`. | Built around `ApplicationContext` and Micronaut DI. Pulling it into a Spring/Spring Boot app brings two DI containers and annotation processors. `listObjects()` returns `Set<String>` (no metadata, no glob). No range reads. |
| **Spring Cloud Resource** (`S3Resource`, `BlobResource`, `GoogleStorageResource`) | First-class in Spring Boot; per-blob `Resource` abstraction. | Per-blob only — no listing, no range reads, no presign in the core. Won't work outside Spring contexts. |
| **JDK NIO `FileSystemProvider`** (`s3-nio-spi`, `google-cloud-nio`, `azure-storage-blob-nio`) | Standard `Path`/`Files` API. | Cloud semantics (no real directories, presigned URLs, conditional writes, multipart uploads, capability differences) don't fit `Path`/`Files` cleanly; each provider invents non-standard escape hatches. |
| **Apache Commons VFS** | Multi-protocol (FTP, SFTP, HTTP, S3 via 3rd party). | File-system-oriented; not cloud-first; missing modern auth flows and conditional-write semantics. |
| **Apache Iceberg `FileIO` / Apache Parquet `InputFile`** | Minimal byte-oriented IO interface. | Single-blob abstraction only — no bucket-level operations (list, copy, delete, presign). Designed as an injection point, not a complete API. |
| **Per-cloud SDKs directly** (AWS SDK v2, Azure SDK, GCS Java client) | Maximum control, no abstraction overhead. | Every consumer reimplements caching, range-fetch, retry, and listing logic. This is exactly what `tileverse-storage` consolidates — and is what the project's prior shape (separate `tileverse-rangereader-*` modules) had become. |

## What `tileverse-storage` does differently

The library exists because the intersection of constraints — Spring/no-DI, range-read-first, Hadoop-free, BOM-curated, server-side — has no off-the-shelf solution. The design distinctives are:

### 1. `RangeReader` is first-class, not an afterthought

Cloud-optimized formats (PMTiles, COG, GeoParquet, FlatGeobuf) read 4 KB headers, jump to a directory, then issue hundreds of small byte-range requests per query. None of the general-purpose abstractions above expose a thread-safe, decorator-friendly byte-range reader at the core type. `RangeReader` is composable with `CachingRangeReader` (in-memory), `BlockAlignedRangeReader` (cloud-pricing-friendly block alignment), and `DiskCachingRangeReader` (persistent across runs). The decorator stack delivers measurable cost reductions on real-world cloud workloads.

### 2. Capabilities are an explicit, queryable record — not an exception roulette

`StorageCapabilities` is a sealed `record` describing what each backend supports: `rangeReads`, `list`, `realDirectories`, `bulkDelete`, `bulkDeleteBatchLimit`, `serverSideCopy`, `atomicMove`, `presignedUrls`, `maxPresignTtl`, `versioning`, `strongReadAfterWrite`. Callers can interrogate up front or rely on `UnsupportedCapabilityException` from the relevant call. Hadoop's `hasPathCapabilities` is the closest analog elsewhere — none of the other libraries expose this surface as a typed, discoverable record.

### 3. Sealed `StorageEntry` honestly models flat-vs-hierarchical reality

```java
sealed interface StorageEntry {
    record File(String key, long size, Instant lastModified, ...) implements StorageEntry {}
    record Prefix(String key) implements StorageEntry {}      // synthetic prefix on flat object stores
    record Directory(String key, ...) implements StorageEntry {}  // real directory on FS / ADLS Gen2 HNS / GCS HNS
}
```

Most abstractions either pretend everything is a directory (Hadoop) or collapse to a single metadata type (jclouds). The sealed hierarchy lets pattern-matching be exhaustive while keeping the underlying truth visible to callers who need it.

### 4. DI-agnostic — `ServiceLoader`, no `ApplicationContext`

`StorageFactory.open(URI)` selects a `StorageProvider` via standard `ServiceLoader`. No Spring context, no Micronaut bean discovery, no annotation processor. The library works identically inside Spring, Spring Boot, Micronaut, plain CLI tools, and library code that has no DI host at all. This is the single biggest fit-gap between `tileverse-storage` and the existing options for our audience.

### 5. Auto-caching is declarative configuration, not a backend concern

Caching is wired in through `StorageFactory` at open time, driven by `storage.caching.*` parameters. Backends never reach for `CachingRangeReader` themselves; the decorator concern stays out of the implementation. `CachingStorage` is a `Storage` decorator that wraps every `openRangeReader` call with a caller-supplied `UnaryOperator<RangeReader>`, and the rest of the surface delegates straight through.

### 6. Consumes the same cloud-SDK BOM as the rest of the ecosystem

`tileverse-storage` consumes [`cloud-dependencies-bom`](https://github.com/tileverse-io/cloud-dependencies-bom) — the same curation surface used by `imageio-ext`, `geowebcache`, `geoserver`, and `geoserver cloud`. The BOM does two jobs: it pins versions of the cloud-storage SDKs (AWS SDK v2, Azure SDK, GCS Java client, Caffeine, Jackson), and it *excludes* Netty entirely. The AWS and Azure SDKs ship with conflicting Netty-based HTTP clients by default (`netty-nio-client` and `azure-core-http-netty`); the BOM excludes both and substitutes JDK-native HTTP clients (and AWS's CRT client), so a deployment that combines multiple cloud SDKs has no Netty in its transitive graph at all.

A deployment that uses any of the consuming projects already agrees on this version set; adding `tileverse-storage` doesn't open a second SDK curation front. By contrast, adopting Hadoop's `FileSystem`, Micronaut Object Storage, or a Spring Boot storage starter would each bring an alternate dependency surface — and they would silently reintroduce Netty into the transitive graph, undoing the exclusion the BOM exists to enforce.

## Adapter strategy: foundation, not competitor

The defense above could read as "we're competing with all of these libraries." We're not. `tileverse-storage` is positioned as a **substrate** that other ecosystems can adapt to, via thin adapter modules. Some of these are planned, some are easy to write on demand:

| Adapter | Direction | Status |
| :--- | :--- | :--- |
| `parquet-java` `InputFile` / `OutputFile` / `SeekableInputStream` | `Storage` + `RangeReader` → Parquet IO | In progress on the GeoParquet branch — closes the loop on the "Parquet without Hadoop" goal. |
| JDK NIO `FileSystemProvider` | `Storage` → `Path` / `Files` | Plausible follow-up; a `tileverse-storage-nio` module would let any NIO-speaking library read tileverse-managed storage with no further glue. |
| Spring `Resource` / `WritableResource` | `Storage` → Spring Resource | Mechanical adapter; useful for Spring app code that already speaks the Resource API. |
| Micronaut `ObjectStorageOperations` | `Storage` → Micronaut OS | Mechanical adapter; useful for Micronaut consumers who want the tileverse caching/range-reader stack. |
| Apache Iceberg `FileIO` | `Storage` → Iceberg IO | Possible; would let Iceberg readers operate on tileverse-managed storage without Hadoop. |

This is the same pattern Iceberg used for parquet-java: keep the substrate clean and Hadoop-free, then provide adapters where ecosystem fit demands.

## When *not* to use this library

`tileverse-storage` is a sharp tool, not a Swiss Army knife. Pick something else when:

- **You're building a microservice that uploads/downloads whole user-supplied attachments.** Use Spring Cloud Resource, Micronaut Object Storage, or the per-cloud SDK directly. The range-read and caching machinery is dead weight for that workload.
- **You're already inside a Hadoop-based pipeline (Spark, Flink, Hive).** Use `org.apache.hadoop.fs.FileSystem` with the standard cloud connectors. Bridging to `tileverse-storage` from inside Hadoop is friction with no upside.
- **You need a transactional document store or a queue.** This is a blob abstraction, not a database.
- **Your application is Micronaut-native and you're happy with the `Set<String>` listing surface.** Micronaut Object Storage is well-supported and integrated with the Micronaut runtime.
- **Your data lives behind a non-S3-compatible protocol (native OpenStack Swift, Azure Files / SMB, HDFS-via-WebHDFS, FTP/SFTP, Cassandra, etc.).** These are out of scope by design — the audience is overwhelmingly served by S3-compatible endpoints, and S3-compatibility is supported transparently for any service that returns `x-amz-*` headers (Ceph RGW, MinIO, Swift via the `s3api` middleware, R2, B2, Wasabi, IBM COS, OVHcloud, etc.). See the [S3-compatible endpoints](index.md#s3-compatible-endpoints) section for the list.

## Summary

`tileverse-storage` exists because the GeoServer / GeoTools / imageio-ext ecosystem has a specific shape — server-side, Spring-rooted, cloud-optimized-format-heavy, BOM-curated, Hadoop-free — and no existing abstraction fits that shape without bringing constraints the audience has explicitly rejected.

The design distinctives — range-read-first, capability records, sealed entry hierarchy, DI-agnostic, declarative caching — exist to delete the per-consumer caching, listing, and range-fetch code that `tileverse-pmtiles`, `tileverse-vectortiles`, and the upcoming GeoParquet reader would otherwise each reimplement.

Adapter modules to the major adjacent ecosystems (parquet-java, NIO, Spring Resource, Micronaut, Iceberg) make the library a foundation rather than a competitor — the same playbook Apache Iceberg used to escape Hadoop, scoped to the geospatial Java stack.
