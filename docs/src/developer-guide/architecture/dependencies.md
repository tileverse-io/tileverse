# Module Dependencies

Tileverse aims to minimize transitive dependencies while leveraging established libraries for core functionality.

## Core Dependencies

| Library | Usage | Module |
| :--- | :--- | :--- |
| **[JTS Topology Suite](https://github.com/locationtech/jts)** | Geometry model (Points, Polygons, etc.) | `vectortiles`, `pmtiles` |
| **[Caffeine](https://github.com/ben-manes/caffeine)** | High-performance in-memory caching | `rangereader` |
| **[Jackson](https://github.com/FasterXML/jackson)** | JSON parsing (metadata, configurations) | `pmtiles` |
| **[Google Protocol Buffers](https://github.com/protocolbuffers/protobuf)** | MVT binary encoding/decoding | `vectortiles` |
| **[SLF4J](https://www.slf4j.org/)** | Logging abstraction | All |

## Optional Dependencies

These are only required if you use specific features (e.g., cloud storage).

| Library | Usage | Module |
| :--- | :--- | :--- |
| **[AWS SDK for Java v2](https://github.com/aws/aws-sdk-java-v2)** | S3 Range Reader | `rangereader-s3` |
| **[Azure SDK for Java](https://github.com/Azure/azure-sdk-for-java)** | Azure Blob Range Reader | `rangereader-azure` |
| **[Google Cloud Java](https://github.com/googleapis/google-cloud-java)** | GCS Range Reader | `rangereader-gcs` |
| **[Apache Commons Compress](https://commons.apache.org/proper/commons-compress/)** | Zstd compression support | `pmtiles` |