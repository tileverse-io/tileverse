# Cloud Storage Integration

Learn how to efficiently access PMTiles from cloud storage providers.

## Overview

PMTiles is designed to work efficiently with cloud object storage. By using HTTP range requests, you can serve tiles directly from S3, Azure Blob Storage, or Google Cloud Storage without a specialized tile server.

## Amazon S3

### Basic S3 Access

```java
import io.tileverse.rangereader.s3.S3RangeReader;
import software.amazon.awssdk.regions.Region;

RangeReader s3Reader = S3RangeReader.builder()
    .uri(URI.create("s3://my-bucket/world.pmtiles"))
    .region(Region.US_WEST_2)
    .build();

try (PMTilesReader reader = new PMTilesReader(s3Reader)) {
    Optional<byte[]> tile = reader.getTile(10, 885, 412);
}
```

### With Caching

```java
import io.tileverse.rangereader.cache.CachingRangeReader;

RangeReader cachedReader = CachingRangeReader.builder(s3Reader)
    .maximumSize(1000)
    .withBlockAlignment()
    .build();

try (PMTilesReader reader = new PMTilesReader(cachedReader)) {
    // Cached reads
    Optional<byte[]> tile = reader.getTile(10, 885, 412);
}
```

## Azure Blob Storage

```java
import io.tileverse.rangereader.azure.AzureBlobRangeReader;

RangeReader azureReader = AzureBlobRangeReader.builder()
    .connectionString(connectionString)
    .containerName("tiles")
    .blobPath("world.pmtiles")
    .build();

try (PMTilesReader reader = new PMTilesReader(azureReader)) {
    Optional<byte[]> tile = reader.getTile(10, 885, 412);
}
```

## Google Cloud Storage

```java
import io.tileverse.rangereader.gcs.GoogleCloudStorageRangeReader;

RangeReader gcsReader = GoogleCloudStorageRangeReader.builder()
    .uri(URI.create("gs://my-bucket/world.pmtiles"))
    .build();

try (PMTilesReader reader = new PMTilesReader(gcsReader)) {
    Optional<byte[]> tile = reader.getTile(10, 885, 412);
}
```

## Performance Optimization

### Multi-Level Caching

Combine memory and disk caching for optimal performance:

```java
// Disk cache
RangeReader diskCached = DiskCachingRangeReader.builder(s3Reader)
    .cacheDirectory(Path.of("/tmp/tile-cache"))
    .maximumCacheSize(10_000_000_000L)  // 10 GB
    .build();

// Memory cache on top
RangeReader memoryCached = CachingRangeReader.builder(diskCached)
    .maximumSize(1000)
    .build();

try (PMTilesReader reader = new PMTilesReader(memoryCached)) {
    // Optimized access
}
```

### Block Alignment

Use block-aligned reads to minimize cloud storage requests:

```java
RangeReader alignedReader = BlockAlignedRangeReader.builder(s3Reader)
    .blockSize(65536)  // 64 KB blocks
    .build();
```

## Cost Optimization

1. **Enable caching** to reduce request counts
2. **Use CDN** in front of object storage
3. **Choose appropriate storage class** (Standard vs. Infrequent Access)
4. **Monitor request patterns** and adjust caching strategy

## See Also

- [Range Reader Authentication](../../rangereader/user-guide/authentication.md)
- [Range Reader Performance](../../rangereader/developer-guide/performance.md)
