# Cloud Storage Integration

Learn how to efficiently access PMTiles from cloud storage providers.

## Overview

PMTiles is designed to work efficiently with cloud object storage. By using HTTP range requests, you can serve tiles directly from S3, Azure Blob Storage, or Google Cloud Storage without a specialized tile server.

## Amazon S3

### Basic S3 Access

If the bucket allows the SDK's default credential chain (IAM role, `~/.aws/credentials`, env vars), `PMTilesReader.open(URI)` is a one-liner:

```java
import io.tileverse.pmtiles.PMTilesReader;
import java.net.URI;
import java.util.Optional;

try (PMTilesReader reader = PMTilesReader.open(URI.create("s3://my-bucket/world.pmtiles"))) {
    Optional<ByteBuffer> tile = reader.getTile(10, 885, 412);
}
```

When you need explicit configuration (region pinning, credentials, endpoint override), open the parent {@link Storage} with `Properties` and ask it for the reader:

```java
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;
import java.util.Properties;

Properties props = new Properties();
props.setProperty("storage.s3.region", "us-west-2");

URI bucket = URI.create("s3://my-bucket/");
URI leaf = URI.create("s3://my-bucket/world.pmtiles");
try (Storage storage = StorageFactory.open(bucket, props);
        RangeReader s3Reader = storage.openRangeReader(leaf);
        PMTilesReader reader = new PMTilesReader(s3Reader)) {
    Optional<ByteBuffer> tile = reader.getTile(10, 885, 412);
}
```

### With Caching

```java
import io.tileverse.storage.cache.CachingRangeReader;

try (Storage storage = StorageFactory.open(bucket, props);
        RangeReader baseReader = storage.openRangeReader(leaf);
        RangeReader cachedReader = CachingRangeReader.builder(baseReader)
            .maximumSize(1000)
            .withBlockAlignment()
            .build();
        PMTilesReader reader = new PMTilesReader(cachedReader)) {
    // Cached reads
    Optional<ByteBuffer> tile = reader.getTile(10, 885, 412);
}
```

## Azure Blob Storage

```java
Properties azureProps = new Properties();
azureProps.setProperty("storage.azure.connection-string", connectionString);

URI container = URI.create("https://account.blob.core.windows.net/tiles/");
URI leaf = URI.create("https://account.blob.core.windows.net/tiles/world.pmtiles");
try (Storage storage = StorageFactory.open(container, azureProps);
        RangeReader azureReader = storage.openRangeReader(leaf);
        PMTilesReader reader = new PMTilesReader(azureReader)) {
    Optional<ByteBuffer> tile = reader.getTile(10, 885, 412);
}
```

## Google Cloud Storage

```java
try (PMTilesReader reader = PMTilesReader.open(URI.create("gs://my-bucket/world.pmtiles"))) {
    Optional<ByteBuffer> tile = reader.getTile(10, 885, 412);
}
```

## Pre-built SDK clients (escape hatch)

For Spring-managed SDK clients, custom retry policies, or test fakes that the
Properties-driven `StorageFactory` route can't express, each backend provider
exposes a public static factory `XxxStorageProvider.open(URI, sdkClient)` that
returns a `Storage`. The returned `Storage` borrows the supplied client (close
is a no-op), so the caller retains lifetime control:

```java
@Bean Storage tiles(S3Client springS3) {
    return S3StorageProvider.open(URI.create("s3://my-bucket/tiles/"), springS3);
}

// elsewhere:
try (RangeReader r = storage.openRangeReader("00/00.pmtiles");
        PMTilesReader reader = new PMTilesReader(r)) { ... }
```

## Performance Optimization

### Multi-Level Caching

Combine memory and disk caching for optimal performance:

```java
import io.tileverse.storage.cache.DiskCachingRangeReader;

try (Storage storage = StorageFactory.open(parent, props);
        RangeReader baseReader = storage.openRangeReader(leaf);
        RangeReader diskCached = DiskCachingRangeReader.builder(baseReader)
            .cacheDirectory(Path.of("/tmp/tile-cache"))
            .maximumCacheSize(10_000_000_000L)  // 10 GB
            .build();
        RangeReader memoryCached = CachingRangeReader.builder(diskCached)
            .maximumSize(1000)
            .build();
        PMTilesReader reader = new PMTilesReader(memoryCached)) {
    // Optimized access
}
```

### Block Alignment

Use block-aligned reads to minimize cloud storage requests:

```java
import io.tileverse.storage.block.BlockAlignedRangeReader;

RangeReader alignedReader = BlockAlignedRangeReader.builder(baseReader)
    .blockSize(65536)  // 64 KB blocks
    .build();
```

## Cost Optimization

1. **Enable caching** to reduce request counts
2. **Use CDN** in front of object storage
3. **Choose appropriate storage class** (Standard vs. Infrequent Access)
4. **Monitor request patterns** and adjust caching strategy

## See Also

- [Range Reader Authentication](../../storage/rangereader/user-guide/authentication.md)
- [Range Reader Performance](../../storage/rangereader/developer-guide/performance.md)
