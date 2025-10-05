# Tileverse Range Reader

A high-performance Java library for reading byte ranges from various data sources including local files, HTTP servers, and cloud storage services.

## The Cloud-Native Geospatial Challenge

The geospatial data landscape has fundamentally shifted from traditional "download-and-process" workflows to **cloud-native** patterns. Modern formats like **Cloud Optimized GeoTIFF (COG)**, **PMTiles**, **GeoParquet**, **Zarr**, and **FlatGeobuf** are explicitly designed to leverage **HTTP range requests**, allowing applications to fetch only the specific byte ranges needed for a query rather than downloading entire multi-gigabyte files.

However, the Java ecosystem has suffered from **significant fragmentation** in this space. Tileverse Range Reader provides the **missing architectural layer** that the Java geospatial ecosystem needs—a lightweight, extensible, and cloud-agnostic abstraction for range-based I/O operations.

## Key Features

- **Multiple Data Sources**: Local files, HTTP/HTTPS, Amazon S3, Azure Blob Storage, and Google Cloud Storage.
- **High Performance**: Multi-level caching, block alignment, and concurrent access.
- **Flexible Architecture**: Composable functionality through decorators and builder APIs.
- **Comprehensive Authentication**: Support for a wide range of authentication mechanisms.

## Getting Started

Choose your path based on your role:

<div class="grid cards" markdown>

-   :material-rocket-launch: **User Guide**

    ---

    Learn how to use the library in your applications.

    [:octicons-arrow-right-24: Get Started](user-guide/index.md)

-   :material-code-braces: **Developer Guide**

    ---

    Contribute to the project or understand the internals.

    [:octicons-arrow-right-24: Development](developer-guide/index.md)

</div>

## Requirements

- **Java 17+**: Minimum Java version required
- **Maven 3.9+**: For building from source
- **Docker**: For running benchmarks and integration tests

## License

Licensed under the Apache License, Version 2.0.
