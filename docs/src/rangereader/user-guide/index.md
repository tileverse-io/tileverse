# User Guide

Welcome to the Tileverse Range Reader User Guide! This section provides comprehensive guidance for developers who want to integrate the library into their applications.

## What You'll Learn

This guide covers everything you need to know to effectively use the Tileverse Range Reader library:

- **Installation**: How to add the library to your project
- **Quick Start**: Get up and running with basic examples
- **Configuration**: Advanced configuration options for optimal performance
- **Authentication**: Setting up authentication for cloud providers and HTTP sources
- **Troubleshooting**: Common issues and their solutions

## Real-World Use Cases

The Tileverse Range Reader library addresses critical needs in modern geospatial and data processing applications:

### 🌍 **Geospatial Data Processing**
- **Cloud Optimized GeoTIFF (COG)** reading for satellite imagery analysis
- **PMTiles** serving for high-performance web mapping
- **GeoParquet** processing for large-scale vector analytics
- **Zarr** access for multi-dimensional geospatial datasets
- **FlatGeobuf** streaming for efficient vector data queries

### 🏢 **Enterprise Applications**
- **Server-side tile rendering** without downloading entire datasets
- **Microservices architecture** with cloud-native data access
- **Data pipelines** that process specific portions of large files
- **Analytics platforms** performing random access on cloud-stored data

### 🚀 **Performance-Critical Systems**
- **Real-time mapping applications** requiring sub-second response times
- **Large-scale batch processing** with optimized I/O patterns
- **Mobile and embedded systems** with bandwidth constraints
- **Multi-tenant platforms** with shared caching strategies

## Target Audience

This guide is designed for:

- **Java Developers** building geospatial or data processing applications
- **Application Architects** designing cloud-native data access systems
- **DevOps Engineers** deploying and optimizing data-intensive applications
- **Performance Engineers** optimizing I/O patterns and caching strategies
- **Library Authors** building format readers on a common foundation

## Getting Started Quickly

If you're in a hurry, start with these essentials:

1. **[Installation](installation.md)** - Add the library to your project
2. **[Quick Start](quick-start.md)** - Basic usage examples
3. **[Configuration](configuration.md)** - Performance optimization

## Library Overview

The Tileverse Range Reader provides a unified interface for reading byte ranges from various sources:

```java
// The core interface - same for all data sources
public interface RangeReader extends Closeable {
    ByteBuffer readRange(long offset, int length) throws IOException;
    int readRange(long offset, int length, ByteBuffer target) throws IOException;
    long size() throws IOException;
    String getSourceIdentifier();
}
```

### Supported Data Sources

| Data Source | Module | Authentication |
|-------------|--------|----------------|
| **Local Files** | `core` | File system permissions |
| **HTTP/HTTPS** | `core` | Basic, Bearer, API Key, Digest, Custom |
| **Amazon S3** | `s3` | AWS credentials, IAM roles |
| **Azure Blob Storage** | `azure` | Connection strings, SAS tokens, Azure AD |
| **Google Cloud Storage** | `gcs` | Service accounts, ADC |

### Performance Features

The library includes several performance optimization features:

- **Memory Caching**: Fast access to recently used ranges
- **Disk Caching**: Persistent caching for large datasets
- **Block Alignment**: Optimized read patterns for cloud storage
- **Concurrent Access**: Thread-safe implementations

## Common Use Cases

### PMTiles and Tiled Data
Perfect for accessing tile data without loading entire files:

```java
// Read PMTiles header
ByteBuffer header = reader.readRange(0, 127);
header.flip();

// Read specific tiles based on tile index
ByteBuffer tileData = reader.readRange(tileOffset, tileLength);
tileData.flip();
```

### Large File Processing
Process large files in chunks:

```java
long fileSize = reader.size();
int chunkSize = 1024 * 1024; // 1MB chunks

for (long offset = 0; offset < fileSize; offset += chunkSize) {
    int length = (int) Math.min(chunkSize, fileSize - offset);
    ByteBuffer chunk = reader.readRange(offset, length);
    chunk.flip();
    // Process chunk
}
```

### Cloud Data Analysis
Efficiently access cloud-stored datasets:

```java
// Read dataset metadata
ByteBuffer metadata = reader.readRange(0, 1024);
metadata.flip();

// Read specific data sections
ByteBuffer section1 = reader.readRange(metadataSize, sectionLength);
section1.flip();
ByteBuffer section2 = reader.readRange(section1Offset, section1Length);
section2.flip();
```

## Performance Considerations

### Decorator Stacking Order

For optimal performance, stack decorators in this order:

```java
Application
    ↓
CachingRangeReader (memory cache - outermost)
    ↓  
DiskCachingRangeReader (persistent cache)
    ↓
BaseReader (S3, Azure, HTTP, etc.)
    ↓
Data Source
```

### Read Pattern Guidelines

| Data Source | Recommended Strategy | Rationale |
|-------------|----------------------|-----------|
| **Local Files** | Direct access | OS already provides efficient file caching |
| **HTTP** | Chunked reading (256 KB - 1 MB) | Reduce request overhead |
| **S3** | Large chunks (1 MB - 8 MB) | Minimize API calls, optimize for S3's performance characteristics |
| **Azure Blob** | Large chunks (1 MB - 4 MB) | Balance throughput and latency |
| **Google Cloud** | Large chunks (1 MB - 8 MB) | Optimize for GCS performance |

### Memory Management

Configure caching based on your available memory:

```java
// For memory-constrained environments
CachingRangeReader.builder(delegate)
    .maximumSize(100)  // Limit number of cached ranges
    .softValues()      // Allow GC to reclaim memory
    .build()

// For memory-rich environments  
CachingRangeReader.builder(delegate)
    .maxSizeBytes(512 * 1024 * 1024)  // 512MB cache
    .expireAfterAccess(30, TimeUnit.MINUTES)
    .build()
```

## Next Steps

Ready to get started? Choose your path:

<div class="grid cards" markdown>

-   :material-download: **Installation**

    ---

    Add the library to your Maven project

    [:octicons-arrow-right-24: Install](installation.md)

-   :material-rocket: **Quick Start**

    ---

    Basic examples for each data source

    [:octicons-arrow-right-24: Examples](quick-start.md)

-   :material-tune: **Configuration**

    ---

    Optimize performance for your use case

    [:octicons-arrow-right-24: Configure](configuration.md)

-   :material-shield-key: **Authentication**

    ---

    Set up secure access to your data

    [:octicons-arrow-right-24: Auth Setup](authentication.md)

</div>

## Need Help?

- Check the **[Troubleshooting](troubleshooting.md)** guide for common issues
- Review the **[Quick Start](quick-start.md)** for code examples
- Read the **[Configuration](configuration.md)** guide for advanced setup
- Visit our **[GitHub repository](https://github.com/tileverse-io/tileverse-rangereader)** for the latest updates