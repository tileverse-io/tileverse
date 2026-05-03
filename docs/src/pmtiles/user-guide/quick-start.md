# PMTiles Quick Start

Get started with Tileverse PMTiles in under 5 minutes.

## Installation

First, add the dependencies:

=== "Maven"

    ```xml
    <dependencies>
      <dependency>
        <groupId>io.tileverse.pmtiles</groupId>
        <artifactId>tileverse-pmtiles</artifactId>
        <version>2.0.0</version>
      </dependency>
      <dependency>
        <groupId>io.tileverse.storage</groupId>
        <artifactId>tileverse-storage-all</artifactId>
        <version>2.0.0</version>
      </dependency>
    </dependencies>
    ```

=== "Gradle"

    ```gradle
    dependencies {
        implementation 'io.tileverse.pmtiles:tileverse-pmtiles:2.0.0'
        implementation 'io.tileverse.storage:tileverse-storage-all:2.0.0'
    }
    ```

## Reading Tiles from a Local File

`PMTilesReader.open(URI)` is a one-line entry point that opens the parent {@link Storage}, gets a `RangeReader` for the leaf, and bundles them so that closing the reader releases both. The same call works for any URI scheme (`file:`, `http(s):`, `s3:`, `gs:`, Azure URLs).

```java
import io.tileverse.pmtiles.PMTilesReader;
import io.tileverse.pmtiles.PMTilesHeader;
import java.net.URI;
import java.nio.file.Path;
import java.util.Optional;

public class QuickStart {
    public static void main(String[] args) throws Exception {
        URI uri = Path.of("world.pmtiles").toUri();
        try (PMTilesReader reader = PMTilesReader.open(uri)) {
            // Read the header to get metadata
            PMTilesHeader header = reader.getHeader();
            System.out.println("Tile Format: " + header.tileType());
            System.out.println("Min Zoom: " + header.minZoom());
            System.out.println("Max Zoom: " + header.maxZoom());

            // Get a specific tile (zoom=10, x=885, y=412)
            Optional<ByteBuffer> tileData = reader.getTile(10, 885, 412);

            if (tileData.isPresent()) {
                System.out.printf("Tile found! Size: %d bytes%n", tileData.get().remaining());
            } else {
                System.out.println("Tile not found");
            }
        }
    }
}
```

## Reading from HTTP

```java
try (PMTilesReader reader = PMTilesReader.open(URI.create("https://example.com/tiles.pmtiles"))) {
    Optional<ByteBuffer> tile = reader.getTile(10, 885, 412);
    // Process tile...
}
```

## Reading from S3

For backends that need configuration (region, credentials, endpoint overrides), open a `Storage` explicitly with the configured `Properties`, then ask it for a reader. This is the general-purpose two-resource pattern.

```java
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;
import java.net.URI;
import java.util.Properties;

Properties props = new Properties();
props.setProperty("storage.s3.region", "us-west-2");

URI bucket = URI.create("s3://my-bucket/");
URI leaf = URI.create("s3://my-bucket/world.pmtiles");

try (Storage storage = StorageFactory.open(bucket, props);
        RangeReader s3Reader = storage.openRangeReader(leaf);
        PMTilesReader reader = new PMTilesReader(s3Reader)) {
    Optional<ByteBuffer> tile = reader.getTile(10, 885, 412);
    // Process tile...
}
```

## With Caching

For better performance, especially with cloud storage, wrap the reader with caching before handing it to `PMTilesReader`:

```java
import io.tileverse.storage.cache.CachingRangeReader;

try (Storage storage = StorageFactory.open(URI.create("s3://my-bucket/"), props);
        RangeReader baseReader = storage.openRangeReader(URI.create("s3://my-bucket/world.pmtiles"));
        // Wrap the base reader with caching
        RangeReader cachedReader = CachingRangeReader.builder(baseReader)
            .maximumSize(1000)        // Cache up to 1000 ranges
            .withBlockAlignment()     // Optimize reads
            .build();
        PMTilesReader reader = new PMTilesReader(cachedReader)) {
    Optional<ByteBuffer> tile = reader.getTile(10, 885, 412);
}
```

## Processing Multiple Tiles

`PMTilesReader.open(URI)` is the simplest entry point — it opens the
parent `Storage` and `RangeReader` for you, and closing the reader
releases both.

```java
// Get tiles for a specific area
int zoom = 10;
int minX = 880, maxX = 890;
int minY = 410, maxY = 420;

try (PMTilesReader reader = PMTilesReader.open(URI.create("s3://my-bucket/world.pmtiles"))) {
    for (int x = minX; x <= maxX; x++) {
        for (int y = minY; y <= maxY; y++) {
            Optional<ByteBuffer> tile = reader.getTile(zoom, x, y);
            if (tile.isPresent()) {
                System.out.printf("Tile %d/%d/%d: %d bytes%n",
                    zoom, x, y, tile.get().remaining());
            }
        }
    }
}
```

## Next Steps

- **[Reading PMTiles](reading.md)**: Learn more about reading operations
- **[Writing PMTiles](writing.md)**: Create your own PMTiles archives
- **[Cloud Storage](cloud-storage.md)**: Deep dive into cloud storage integration
- **[Storage Guide](../../storage/index.md)** and **[Range Reader Guide](../../storage/rangereader/user-guide/index.md)**: Understand the underlying data access layer
