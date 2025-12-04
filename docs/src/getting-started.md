# Getting Started

## Installation

Tileverse is available on Maven Central. You can use the Bill of Materials (BOM) to manage versions across all modules.

=== "Maven"

    Add the BOM to your `dependencyManagement` to ensure version consistency:

    ```xml
    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>io.tileverse</groupId>
                <artifactId>tileverse-bom</artifactId>
                <version>1.0.0</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
        </dependencies>
    </dependencyManagement>
    ```

    Then add specific modules to your `dependencies` block:

    ```xml
    <dependencies>
        <!-- Efficient data access (File, HTTP, S3, etc.) -->
        <dependency>
            <groupId>io.tileverse.rangereader</groupId>
            <artifactId>tileverse-rangereader-all</artifactId>
        </dependency>

        <!-- PMTiles support -->
        <dependency>
            <groupId>io.tileverse.pmtiles</groupId>
            <artifactId>tileverse-pmtiles</artifactId>
        </dependency>
    </dependencies>
    ```

=== "Gradle"

    ```kotlin
    dependencies {
        implementation(platform("io.tileverse:tileverse-bom:1.0.0"))
        
        implementation("io.tileverse.rangereader:tileverse-rangereader-all")
        implementation("io.tileverse.pmtiles:tileverse-pmtiles")
    }
    ```

## Version Compatibility

| Library Version | Java Version | Maven Version |
|----------------|--------------|---------------|
| 1.0.x | 17+ | 3.9+ |

## Basic Usage

### Reading a PMTiles Archive

This example demonstrates how to read a PMTiles file from a local path using the `FileRangeReader`.

```java
import io.tileverse.pmtiles.PMTilesReader;
import io.tileverse.rangereader.file.FileRangeReader;
import java.nio.file.Path;
import java.util.Optional;

public class Example {
    public void readTile() throws Exception {
        // Initialize the underlying data source
        var source = FileRangeReader.builder()
            .path(Path.of("data/planet.pmtiles"))
            .build();

        // Open the PMTiles reader
        try (var reader = new PMTilesReader(source::asByteChannel)) {
            // Fetch a specific tile (z=0, x=0, y=0)
            Optional<ByteBuffer> tile = reader.getTile(0, 0, 0);
            
            tile.ifPresent(buffer -> {
                buffer.flip(); // Important: flip the buffer before reading
                System.out.println("Found tile: " + buffer.remaining() + " bytes");
            });
        }
    }
}
```

### Cloud Storage Access

To access files on S3, swap the `FileRangeReader` for `S3RangeReader`. The upper-level `PMTilesReader` API remains unchanged.

```java
import io.tileverse.rangereader.s3.S3RangeReader;
import software.amazon.awssdk.regions.Region;
import java.net.URI;

var s3Source = S3RangeReader.builder()
    .uri(URI.create("s3://my-bucket/maps/planet.pmtiles"))
    .region(Region.US_EAST_1)
    .build();

// Wrap with caching for performance
var cachedSource = CachingRangeReader.builder(s3Source)
    .capacity(50_000_000) // 50MB cache
    .build();
    
var reader = new PMTilesReader(cachedSource::asByteChannel);
```

## Requirements

- **Java 17** or later.
- Maven 3.8+ or Gradle 7+.