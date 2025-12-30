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
        <version>1.1.0</version>
      </dependency>
      <dependency>
        <groupId>io.tileverse.rangereader</groupId>
        <artifactId>tileverse-rangereader-all</artifactId>
        <version>1.1.0</version>
      </dependency>
    </dependencies>
    ```

=== "Gradle"

    ```gradle
    dependencies {
        implementation 'io.tileverse.pmtiles:tileverse-pmtiles:1.1.0'
        implementation 'io.tileverse.rangereader:tileverse-rangereader-all:1.1.0'
    }
    ```

## Reading Tiles from a Local File

```java
import io.tileverse.pmtiles.PMTilesReader;
import io.tileverse.pmtiles.PMTilesHeader;
import io.tileverse.rangereader.RangeReader;
import io.tileverse.rangereader.file.FileRangeReader;
import java.nio.file.Path;
import java.util.Optional;

public class QuickStart {
    public static void main(String[] args) throws Exception {
        // Create a range reader for the PMTiles file
        RangeReader rangeReader = FileRangeReader.builder()
            .path(Path.of("world.pmtiles"))
            .build();

        // Open the PMTiles archive
        try (PMTilesReader reader = new PMTilesReader(rangeReader)) {
            // Read the header to get metadata
            PMTilesHeader header = reader.getHeader();
            System.out.println("Tile Format: " + header.tileType());
            System.out.println("Min Zoom: " + header.minZoom());
            System.out.println("Max Zoom: " + header.maxZoom());

            // Get a specific tile (zoom=10, x=885, y=412)
            Optional<ByteBuffer> tileData = reader.getTile(10, 885, 412);

            if (tileData.isPresent()) {
                System.out.printf("Tile found! Size: %d bytes%n",
                    tileData.get().remaining());
            } else {
                System.out.println("Tile not found");
            }
        }
    }
}
```

## Reading from HTTP

```java
import io.tileverse.rangereader.http.HttpRangeReader;

// Create HTTP range reader
RangeReader httpReader = HttpRangeReader.builder()
    .uri(URI.create("https://example.com/tiles.pmtiles"))
    .build();

try (PMTilesReader reader = new PMTilesReader(httpReader)) {
    Optional<ByteBuffer> tile = reader.getTile(10, 885, 412);
    // Process tile...
}
```

## Reading from S3

```java
import io.tileverse.rangereader.s3.S3RangeReader;
import software.amazon.awssdk.regions.Region;

// Create S3 range reader
RangeReader s3Reader = S3RangeReader.builder()
    .uri(URI.create("s3://my-bucket/world.pmtiles"))
    .region(Region.US_WEST_2)
    .build();

    try (PMTilesReader reader = new PMTilesReader(s3Reader)) {
        Optional<ByteBuffer> tile = reader.getTile(10, 885, 412);    // Process tile...
}
```

## With Caching

For better performance, especially with cloud storage, add caching:

```java
import io.tileverse.rangereader.cache.CachingRangeReader;

// Wrap the base reader with caching
RangeReader cachedReader = CachingRangeReader.builder(s3Reader)
    .maximumSize(1000)  // Cache up to 1000 ranges
    .withBlockAlignment()  // Optimize reads
    .build();

try (PMTilesReader reader = new PMTilesReader(cachedReader)) {
    // Subsequent reads will be cached
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
- **[Range Reader Guide](../../rangereader/user-guide/index.md)**: Understand the underlying data access layer
