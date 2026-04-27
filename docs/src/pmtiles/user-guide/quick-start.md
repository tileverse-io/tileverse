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

```java
import io.tileverse.pmtiles.PMTilesReader;
import io.tileverse.pmtiles.PMTilesHeader;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.StorageFactory;
import java.nio.file.Path;
import java.util.Optional;

public class QuickStart {
    public static void main(String[] args) throws Exception {
        try (RangeReader rangeReader = StorageFactory.openRangeReader(Path.of("world.pmtiles").toUri());
                PMTilesReader reader = new PMTilesReader(rangeReader)) {
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
try (RangeReader httpReader = StorageFactory.openRangeReader(URI.create("https://example.com/tiles.pmtiles"));
        PMTilesReader reader = new PMTilesReader(httpReader)) {
    Optional<ByteBuffer> tile = reader.getTile(10, 885, 412);
    // Process tile...
}
```

## Reading from S3

```java
import java.util.Properties;

Properties props = new Properties();
props.setProperty("storage.s3.region", "us-west-2");

try (RangeReader s3Reader = StorageFactory.openRangeReader(
            URI.create("s3://my-bucket/world.pmtiles"), props);
        PMTilesReader reader = new PMTilesReader(s3Reader)) {
    Optional<ByteBuffer> tile = reader.getTile(10, 885, 412);
    // Process tile...
}
```

## With Caching

For better performance, especially with cloud storage, add caching:

```java
import io.tileverse.storage.cache.CachingRangeReader;

try (RangeReader baseReader = StorageFactory.openRangeReader(
            URI.create("s3://my-bucket/world.pmtiles"), props);
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

```java
// Get tiles for a specific area
int zoom = 10;
int minX = 880, maxX = 890;
int minY = 410, maxY = 420;

try (PMTilesReader reader = new PMTilesReader(rangeReader)) {
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
