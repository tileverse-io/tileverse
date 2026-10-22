# Getting Started

Tileverse is published on Maven Central. Import the BOM to manage the versions, then add the modules you need.

## Installation

=== "Maven"

    ```xml
    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>io.tileverse</groupId>
                <artifactId>tileverse-bom</artifactId>
                <version>2.1.0</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
        </dependencies>
    </dependencyManagement>

    <dependencies>
        <dependency>
            <groupId>io.tileverse.storage</groupId>
            <artifactId>tileverse-storage-all</artifactId>
        </dependency>
        <dependency>
            <groupId>io.tileverse.pmtiles</groupId>
            <artifactId>tileverse-pmtiles</artifactId>
        </dependency>
    </dependencies>
    ```

=== "Gradle"

    ```kotlin
    dependencies {
        implementation(platform("io.tileverse:tileverse-bom:2.1.0"))

        implementation("io.tileverse.storage:tileverse-storage-all")
        implementation("io.tileverse.pmtiles:tileverse-pmtiles")
    }
    ```

Without the BOM, give each dependency its version, `2.1.0`. Tileverse needs Java 17 or later, and Maven 3.9 or Gradle 7 or later.

## Modules

| Group | Artifact | Contents |
| :--- | :--- | :--- |
| `io.tileverse.storage` | `tileverse-storage-core` | The `Storage` and `RangeReader` APIs, with the local file and HTTP backends |
| `io.tileverse.storage` | `tileverse-storage-s3` | Amazon S3 and S3-compatible services |
| `io.tileverse.storage` | `tileverse-storage-azure` | Azure Blob Storage and Data Lake Storage Gen2 |
| `io.tileverse.storage` | `tileverse-storage-gcs` | Google Cloud Storage |
| `io.tileverse.storage` | `tileverse-storage-all` | The four storage modules above |
| `io.tileverse.pmtiles` | `tileverse-pmtiles` | The PMTiles reader and tile stores |
| `io.tileverse.versatiles` | `tileverse-versatiles` | The VersaTiles reader and tile stores |
| `io.tileverse.vectortiles` | `tileverse-vectortiles` | The Mapbox Vector Tiles codec |
| `io.tileverse.tilematrixset` | `tileverse-tilematrixset` | The OGC Tile Matrix Set model |
| `io.tileverse.tilestore` | `tileverse-tilestore` | The tile store API shared by the archive readers |

One application can combine the cloud storage modules: none of them depends on Netty, and their SDK versions come from one curated set.

## Next steps

- [Why tileverse-storage?](storage/explanation/why.md): who the storage library is built for, how it compares with other Java storage libraries, and when to use another one.
- [Read PMTiles](pmtiles/how-to/read-pmtiles.md): open an archive on a local disk, a web server or a cloud bucket, then read and decode its tiles.
- [Read Versatiles](versatiles/how-to/read-versatiles.md): the same for VersaTiles containers.
