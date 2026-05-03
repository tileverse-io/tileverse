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
                <version>2.0.0</version>
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
            <groupId>io.tileverse.storage</groupId>
            <artifactId>tileverse-storage-all</artifactId>
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
        implementation(platform("io.tileverse:tileverse-bom:2.0.0"))
        
        implementation("io.tileverse.storage:tileverse-storage-all")
        implementation("io.tileverse.pmtiles:tileverse-pmtiles")
    }
    ```

## Version Compatibility

| Library Version | Java Version | Maven Version |
|----------------|--------------|---------------|
| 2.0.x | 17+ | 3.9+ |

## Basic Usage

### Reading a PMTiles Archive

The simplest form is `PMTilesReader.open(URI)`, which splits the leaf URI
into parent + key, opens the underlying `Storage`, gets the `RangeReader`,
and bundles them all so closing the reader releases everything.

```java
import io.tileverse.pmtiles.PMTilesReader;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Optional;

public class Example {
    public void readTile() throws Exception {
        URI leaf = Path.of("data/planet.pmtiles").toUri();

        try (PMTilesReader reader = PMTilesReader.open(leaf)) {

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

To access files on S3, point `StorageFactory.open` at an `s3://` URI for the bucket (or bucket+prefix), then read the leaf URI through the Storage. The upper-level `PMTilesReader` API remains unchanged.

```java
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;
import io.tileverse.storage.cache.CachingRangeReader;
import java.net.URI;
import java.util.Properties;

Properties props = new Properties();
props.setProperty("storage.s3.region", "us-east-1");

URI bucket = URI.create("s3://my-bucket/maps/");
URI leaf = URI.create("s3://my-bucket/maps/planet.pmtiles");

try (Storage storage = StorageFactory.open(bucket, props);
        RangeReader s3Source = storage.openRangeReader(leaf);

        // Wrap with in-memory caching for performance
        RangeReader cachedSource = CachingRangeReader.builder(s3Source)
            .capacity(50_000_000) // 50MB cache
            .build();
        PMTilesReader reader = new PMTilesReader(cachedSource)) {
    // ...
}
```

## Requirements

- **Java 17** or later.
- Maven 3.8+ or Gradle 7+.
