# Tileverse I/O

The Tileverse I/O module provides a flexible and extensible I/O abstraction layer for reading byte ranges from various data sources, including local files, HTTP servers, and cloud storage services.

## Overview

The core of this module is the `RangeReader` interface, which provides a simple API for reading arbitrary ranges of bytes from any source. This enables efficient access to specific portions of large files without loading the entire file into memory.

The module follows the **Single Responsibility Principle** by:

1. Defining a clear, focused interface (`RangeReader`) for range reading operations
2. Implementing concrete readers for specific data sources
3. Using the **Decorator Pattern** to add functionality like caching and block alignment
4. Providing a builder pattern for creating and configuring readers

## RangeReader Interface

The `RangeReader` interface is deliberately minimal:

```java
public interface RangeReader extends Closeable {
    /**
     * Reads a range of bytes.
     * 
     * @param offset The starting offset
     * @param length The number of bytes to read
     * @return A ByteBuffer containing the requested bytes
     * @throws IOException If an I/O error occurs
     */
    ByteBuffer readRange(long offset, int length) throws IOException;
    
    /**
     * Returns the total size of the resource.
     * 
     * @return The size in bytes
     * @throws IOException If an I/O error occurs
     */
    long size() throws IOException;
}
```

## Core Implementations

The module provides several base implementations:

- **FileRangeReader**: Reads ranges from local files using NIO channels
- **HttpRangeReader**: Reads ranges from HTTP/HTTPS URLs using range requests
- **S3RangeReader**: Reads ranges from Amazon S3 objects
- **AzureBlobRangeReader**: Reads ranges from Azure Blob Storage

Each implementation focuses solely on reading from its specific data source without any additional concerns like caching or optimizations.

## Decorator Pattern for Enhanced Functionality

Following the Single Responsibility Principle, additional functionality is added through decorators:

- **CachingRangeReader**: Caches exactly the byte ranges it is asked for, in memory
- **BlockAlignedRangeReader**: Expands reads inside declared byte regions to block boundaries, reducing the number of requests

This approach provides several benefits:

1. **Composability**: Decorators can be combined in different ways (e.g., caching + block alignment)
2. **Separation of Concerns**: Each decorator handles one specific enhancement
3. **Extensibility**: New decorators can be added without modifying existing code
4. **Transparency**: The decorators implement the same interface, making them interchangeable

### Example of Decorator Composition

```java
// Base reader for a specific source, obtained through Storage
try (Storage storage = StorageFactory.open(URI.create("s3://bucket/"));
        RangeReader baseReader = storage.openRangeReader("path/to/key")) {

    // Add in-memory caching closest to the source: it stores exact ranges
    RangeReader cachedReader = CachingRangeReader.builder(baseReader).build();

    // Add block alignment as the outermost decorator, declaring which region to align
    RangeReader reader = BlockAlignedRangeReader.builder(cachedReader)
            .blockSize(64 * 1024)
            .alignWholeFile()
            .build();
}
```

The order of decorators is important for optimal performance:

1. **Base Reader**: Provides the core functionality (reading from the source)
2. **Memory Cache**: Stores exactly the ranges it is asked for
3. **Block Alignment**: The outermost decorator; expands requests inside declared regions to block boundaries before they reach the cache

This layered approach offers several advantages:

- Small, frequently accessed ranges stay in memory for fastest access
- Block alignment ensures efficient reading from the source for the regions you declare
- Each decorator maintains a single responsibility following SOLID principles

Placing the cache above the aligner instead defeats the purpose: each request is fetched and
aligned again on every miss, and the cache stores each exact sub-range separately instead of
sharing the underlying block.

## Composing Readers for Different Sources

Every backend is opened through `Storage`, then decorated as needed:

```java
// File reader
try (Storage storage = StorageFactory.open(Path.of("/path/to").toUri());
        RangeReader fileReader = storage.openRangeReader(Path.of("/path/to/file.pmtiles").toUri())) {
    // ...
}

// S3 reader with region and in-memory caching
Properties props = new Properties();
props.setProperty("storage.s3.region", "us-west-2");
try (Storage storage = StorageFactory.open(URI.create("s3://bucket/"), props);
        RangeReader s3Reader = storage.openRangeReader(URI.create("s3://bucket/path/to/file.pmtiles"));
        RangeReader cachedReader = CachingRangeReader.builder(s3Reader).build()) {
    // ...
}

// Azure Blob reader with a connection string
Properties azureProps = new Properties();
azureProps.setProperty("storage.azure.connection-string", connectionString);
URI container = URI.create("https://account.blob.core.windows.net/container/");
URI blob = URI.create("https://account.blob.core.windows.net/container/path/to/blob.pmtiles");
try (Storage storage = StorageFactory.open(container, azureProps);
        RangeReader azureReader = storage.openRangeReader(blob)) {
    // ...
}
```

`StorageFactory.open` resolves the backend from the URI scheme; `Properties` hold per-backend
configuration (region, credentials, endpoints). Each decorator (`CachingRangeReader`,
`BlockAlignedRangeReader`) has its own builder, composed explicitly around the `RangeReader`
returned by `Storage.openRangeReader`.

## Optimizations

### In-Memory Caching (`CachingRangeReader`)

The `CachingRangeReader` decorator provides an in-memory cache of recently accessed ranges, which is valuable when:

- The same ranges are accessed multiple times in a short period
- Adjacent ranges are accessed sequentially
- Quick access is critical for performance

The memory cache is implemented using Caffeine, a high-performance caching library with features like:

- Automatic eviction based on size and access patterns
- Weak references to allow garbage collection when memory is constrained
- Thread-safety for concurrent access

### Block Alignment

The `BlockAlignedRangeReader` decorator optimizes access patterns inside byte regions you
declare (a header, an index array, or the whole file):

1. Reads fully inside a declared region expand to the blocks that cover them, fetched in a single batch call
2. Reads outside every declared region pass through untouched
3. Regions can be declared at construction or later at runtime, as a format reader discovers its own layout

This is especially beneficial for cloud storage, where:
- Each request has overhead (latency, authentication)
- Larger reads are more efficient than multiple small reads
- Services may have minimum read sizes or charge per request

Stack it above a `CachingRangeReader`: the cache then stores the aligned blocks under stable
keys, giving block-grain caching inside declared regions and exact-grain caching everywhere else.

## Usage Patterns

### Basic Usage

```java
// Open a Storage rooted at the parent URI and request a key from it
try (Storage storage = StorageFactory.open(URI.create("s3://my-bucket/"));
        RangeReader reader = storage.openRangeReader("data/header.bin")) {
    // Read a specific range (e.g., file header)
    ByteBuffer headerData = reader.readRange(0, 1024);

    // Process the data...
}
```

### Optimized Cloud Storage Access

```java
// Create an optimized reader for cloud storage
try (Storage storage = StorageFactory.open(URI.create("s3://bucket/"));
        RangeReader baseReader = storage.openRangeReader(URI.create("s3://bucket/path/to/file.pmtiles"));
        RangeReader cachedReader = CachingRangeReader.builder(baseReader).build();
        RangeReader reader = BlockAlignedRangeReader.builder(cachedReader)
                .blockSize(64 * 1024) // 64KB blocks
                .alignWholeFile()
                .build()) {

    // Caching behavior:

    // First read: not cached, reads block [0, 65536) from S3 and stores it in the memory cache
    ByteBuffer data1 = reader.readRange(1000, 100);

    // Second read: a different 64KB block, reads from S3 and caches it
    ByteBuffer data2 = reader.readRange(70_000, 200);

    // Third read: falls inside the first block, found in memory cache (fastest access)
    ByteBuffer data3 = reader.readRange(1050, 50);
}
```

Compose the decorators in this order for optimal performance:
1. Base reader (S3, HTTP, file, etc.)
2. Memory cache (stores exact ranges)
3. Block alignment (outermost; expands into block-sized reads before they reach the cache)

## Future Enhancements

Potential future enhancements include:

- **Parallel Range Reader**: Splits large ranges into chunks for parallel downloading
- **Prefetching Reader**: Speculatively reads ahead based on access patterns
- **Compressed Range Reader**: Handles transparent decompression
- **Rate-Limiting Reader**: Controls bandwidth usage for cloud storage
- **Metrics Collector**: Tracks performance statistics for debugging

## Design Principles

The module is designed with these principles in mind:

1. **Interface Segregation**: `RangeReader` focuses only on essential range reading operations
2. **Single Responsibility**: Each implementation and decorator has one clear purpose
3. **Open/Closed**: The system is open for extension (new readers, decorators) but closed for modification
4. **Dependency Inversion**: High-level code depends on the `RangeReader` abstraction, not concrete implementations
5. **Composition Over Inheritance**: Functionality is added through composition (decorators) rather than inheritance

## Cloud Storage Authentication and Access

This section provides detailed information on authentication methods available for cloud storage providers and how to configure them using the RangeReaderBuilder API.

### AWS S3 Authentication

The S3 implementation uses the AWS SDK for Java v2 and supports multiple authentication methods:

#### 1. Default Credentials Provider Chain

The simplest approach uses the default AWS credentials provider chain, which checks multiple sources for credentials in this order:

1. Java system properties
2. Environment variables
3. Web Identity Token credentials from the environment or container
4. AWS profile credentials (from ~/.aws/credentials)
5. Amazon ECS container credentials
6. Amazon EC2 Instance profile credentials

```java
// Uses the default credentials provider chain (environment variables, profile, etc.)
RangeReader reader = RangeReaderBuilder.create()
    .s3(URI.create("s3://mybucket/path/to/file.pmtiles"))
    .withRegion(Region.US_WEST_2)
    .build();
```

#### 2. Explicit Credentials Provider

You can specify a particular credentials provider:

```java
// Using profile credentials
ProfileCredentialsProvider credentialsProvider = 
    ProfileCredentialsProvider.builder()
        .profileName("my-profile")
        .build();

RangeReader reader = RangeReaderBuilder.create()
    .s3(URI.create("s3://mybucket/path/to/file.pmtiles"))
    .withCredentials(credentialsProvider)
    .withRegion(Region.US_WEST_2)
    .build();
```

Other common credential providers include:

```java
// Environment variables (AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY)
EnvironmentVariableCredentialsProvider envProvider = 
    EnvironmentVariableCredentialsProvider.create();

// Static credentials
StaticCredentialsProvider staticProvider = 
    StaticCredentialsProvider.create(AwsBasicCredentials.create(
        "YOUR_ACCESS_KEY_ID", 
        "YOUR_SECRET_ACCESS_KEY"));
        
// Web identity token (for EKS or federated identity)
WebIdentityTokenFileCredentialsProvider webIdProvider = 
    WebIdentityTokenFileCredentialsProvider.create();

// Session credentials (with temporary token)
StaticCredentialsProvider sessionProvider = 
    StaticCredentialsProvider.create(AwsSessionCredentials.create(
        "ACCESS_KEY_ID", 
        "SECRET_ACCESS_KEY", 
        "SESSION_TOKEN"));
```

#### 3. Custom Endpoint (S3-compatible services)

For S3-compatible services like MinIO, LocalStack, or Ceph, you can specify a custom endpoint:

```java
// MinIO or other S3-compatible service
RangeReader reader = RangeReaderBuilder.create()
    .s3(URI.create("s3://mybucket/path/to/file.pmtiles"))
    .withCredentials(StaticCredentialsProvider.create(
        AwsBasicCredentials.create("minio-access-key", "minio-secret-key")))
    .withEndpoint(URI.create("http://minio-server:9000"))
    .withForcePathStyle() // Important for many S3-compatible services
    .build();
```

#### 4. Region-Specific Configuration

You can specify a region directly or include it in the URI fragment:

```java
// Specify region in builder
RangeReader reader = RangeReaderBuilder.create()
    .s3(URI.create("s3://mybucket/path/to/file.pmtiles"))
    .withRegion(Region.EU_CENTRAL_1)
    .build();

// Or specify region in URI fragment
RangeReader reader = RangeReaderBuilder.create()
    .s3(URI.create("s3://mybucket/path/to/file.pmtiles#eu-central-1"))
    .build();
```

### Azure Blob Storage Authentication

The Azure implementation supports multiple authentication methods through the Azure Storage SDK for Java:

#### 1. Connection String

The simplest approach is using a connection string that includes all necessary information:

```java
// Using a connection string (includes account name, key, and endpoint)
RangeReader reader = RangeReaderBuilder.create()
    .azure()
    .withConnectionString("DefaultEndpointsProtocol=https;AccountName=mystorageaccount;AccountKey=accountKeyBase64;EndpointSuffix=core.windows.net")
    .withContainer("mycontainer")
    .withBlob("path/to/blob.pmtiles")
    .build();
```

Connection strings can be obtained from the Azure Portal and include the account name, account key, and endpoint information.

#### 2. Account Key Authentication

Use account name and key directly:

```java
// Using account credentials (name and key)
RangeReader reader = RangeReaderBuilder.create()
    .azure()
    .withAccountCredentials(
        "mystorageaccount", 
        "base64EncodedAccountKey")
    .withContainer("mycontainer")
    .withBlob("path/to/blob.pmtiles")
    .build();
```

This is useful when you prefer to keep the account name and key separate or need to rotate keys without changing connection strings.

#### 3. Shared Access Signature (SAS) Token

SAS tokens provide fine-grained, time-limited access to specific resources without sharing account keys:

```java
// Using SAS token authentication
RangeReader reader = RangeReaderBuilder.create()
    .azure()
    .withAccountName("mystorageaccount")
    .withSasToken("sv=2022-11-02&ss=b&srt=co&sp=r&se=2023-06-30T02:00:00Z&st=2023-05-01T18:00:00Z&spr=https&sig=XXXXX")
    .withContainer("mycontainer")
    .withBlob("path/to/blob.pmtiles")
    .build();
```

Key features of SAS token authentication:

- **Fine-grained permissions**: Specify exact operations allowed (read, write, delete, etc.)
- **Time-limited access**: Set start and expiry times for the token
- **Resource-specific**: Can be limited to specific containers or blobs
- **No account key sharing**: Improves security by not exposing account keys
- **Revocable**: Can be revoked before expiry by changing account keys or stored access policies

SAS tokens can be generated from:
- Azure Portal
- Azure Storage Explorer
- Azure CLI
- Azure SDKs

The minimum permission needed for RangeReader access is read (`sp=r`).

#### 4. Azure Active Directory (AAD) Authentication

For enterprise scenarios, AAD authentication is preferred:

```java
// Using Azure Active Directory authentication
TokenCredential credential = new DefaultAzureCredentialBuilder().build();

RangeReader reader = RangeReaderBuilder.create()
    .azure()
    .withTokenCredential(credential)
    .withContainer("mycontainer")
    .withBlob("path/to/blob.pmtiles")
    .build();
```

`DefaultAzureCredential` tries several authentication methods in sequence:
1. Environment variables
2. Managed Identity
3. Visual Studio Code credentials
4. Azure CLI credentials
5. IntelliJ credentials

You can also use specific credential types:

```java
// Using a service principal
TokenCredential servicePrincipal = new ClientSecretCredentialBuilder()
    .tenantId("tenant-id")
    .clientId("client-id")
    .clientSecret("client-secret")
    .build();

// Using managed identity (for Azure services)
TokenCredential managedIdentity = new ManagedIdentityCredentialBuilder()
    .clientId("user-assigned-client-id") // Optional, for user-assigned managed identity
    .build();
```

#### 5. Direct URI Access

For public blobs or when using SAS tokens in the URI:

```java
// Using a blob URI with embedded SAS token
URI containerUri = URI.create("https://account.blob.core.windows.net/container/?sv=2022-11-02&ss=b&srt=co&sp=r&sig=XXX");
Storage storage = StorageFactory.open(containerUri);
RangeReader reader = storage.openRangeReader("blob.pmtiles");
```

This will force using the Azure Blob Storage client instead of the regular HTTP client.


### Authentication Best Practices

1. **Use the least privilege principle**: Grant only the permissions needed (read-only for most RangeReader use cases)
2. **Prefer environment variables or credential providers** over hardcoding credentials
3. **Use temporary credentials** (SAS tokens, session tokens) when possible
4. **For production systems**:
   - AWS: Use IAM roles for EC2/ECS/Lambda or Web Identity Federation
   - Azure: Use Managed Identities or service principals with AAD authentication
5. **For development and testing**:
   - AWS: Use credential profiles
   - Azure: Use the Azure CLI credentials or emulator
6. **For public data**:
   - Consider making the blob/object publicly readable
   - Use pre-signed URLs or SAS tokens with short expiration for limited public access

### S3 Implementation Details

The S3 implementation:
- Built on AWS SDK for Java v2
- Supports different authentication methods through credential providers
- Handles region configuration and custom endpoints
- Enables path-style and virtual-hosted style endpoints
- Optimizes partial reads with byte range requests
- Supports S3-compatible storage systems

### Azure Blob Storage Implementation Details

The Azure implementation:
- Built on Azure Storage SDK for Java
- Supports comprehensive authentication options
- Handles SAS tokens (automatically prepending '?' if needed)
- Works with the Azure Storage Emulator (Azurite) for testing
- Translates RangeReader offsets/lengths to HTTP range headers
- Optimizes for blob-specific access patterns

## HTTP Range Reader Authentication

The `HttpRangeReader` in the core module provides authentication support, enabling secure access to remote resources via HTTP(S).

### Available Authentication Methods

The `HttpRangeReader` supports several authentication methods:

1. **Basic Authentication** - Username/password authentication using the HTTP Basic Auth scheme
2. **Bearer Token Authentication** - Token-based authentication using the Bearer scheme
3. **API Key Authentication** - Authentication using a custom header with an API key
4. **Custom Header Authentication** - Authentication using arbitrary custom headers
5. **Digest Authentication** - HTTP Digest Authentication (MD5, SHA-256, SHA-512)

### Usage Examples

#### Basic Authentication

```java
import io.tileverse.storage.rangereader.HttpRangeReader;
import io.tileverse.storage.rangereader.http.BasicAuthentication;

// Create a basic authentication object
BasicAuthentication auth = new BasicAuthentication("username", "password");

// Create an HTTP range reader with authentication
URI uri = URI.create("https://example.com/secure/data.bin");
HttpRangeReader reader = new HttpRangeReader(uri, auth);

// Use the reader to access secured content
ByteBuffer data = reader.readRange(0, 1024);
```

#### Bearer Token Authentication

```java
import io.tileverse.storage.rangereader.HttpRangeReader;
import io.tileverse.storage.rangereader.http.BearerTokenAuthentication;

// Create a bearer token authentication object
BearerTokenAuthentication auth = new BearerTokenAuthentication("your-token-here");

// Create an HTTP range reader with authentication
URI uri = URI.create("https://example.com/secure/data.bin");
HttpRangeReader reader = new HttpRangeReader(uri, auth);
```

#### API Key Authentication

```java
import io.tileverse.storage.rangereader.HttpRangeReader;
import io.tileverse.storage.rangereader.http.ApiKeyAuthentication;

// Create an API key authentication object (header name and value)
ApiKeyAuthentication auth = new ApiKeyAuthentication("X-API-Key", "your-api-key");

// Create an HTTP range reader with authentication
URI uri = URI.create("https://example.com/secure/data.bin");
HttpRangeReader reader = new HttpRangeReader(uri, auth);
```

#### Custom Header Authentication

```java
import io.tileverse.storage.rangereader.HttpRangeReader;
import io.tileverse.storage.rangereader.http.CustomHeaderAuthentication;
import java.util.Map;
import java.util.HashMap;

// Create a map of custom headers
Map<String, String> headers = new HashMap<>();
headers.put("X-Custom-Auth", "auth-value");
headers.put("X-Tenant-ID", "tenant-123");

// Create a custom header authentication object
CustomHeaderAuthentication auth = new CustomHeaderAuthentication(headers);

// Create an HTTP range reader with authentication
URI uri = URI.create("https://example.com/secure/data.bin");
HttpRangeReader reader = new HttpRangeReader(uri, auth);
```

### Using the RangeReaderBuilder

For a more fluent API, you can use the `RangeReaderBuilder`:

```java
import io.tileverse.storage.rangereader.RangeReader;
import io.tileverse.storage.rangereader.RangeReaderBuilder;

// Create a range reader with basic authentication
RangeReader reader = RangeReaderBuilder.create()
    .http(URI.create("https://example.com/secure/data.bin"))
    .withBasicAuth("username", "password")
    .build();

// Create a range reader with bearer token
RangeReader tokenReader = RangeReaderBuilder.create()
    .http(URI.create("https://example.com/secure/data.bin"))
    .withBearerToken("your-token")
    .build();

// Create a range reader with API key
RangeReader apiKeyReader = RangeReaderBuilder.create()
    .http(URI.create("https://example.com/secure/data.bin"))
    .withApiKey("X-API-Key", "your-api-key")
    .build();
```

### Security Considerations

- The `HttpRangeReader` supports SSL/TLS for secure connections
- By default, it accepts all SSL certificates, which is useful for development but not recommended for production
- For production use, configure proper certificate validation:
  ```java
  HttpRangeReader reader = new HttpRangeReader(uri, false, auth); // false = don't trust all certificates
  ```
- Authentication credentials are held in memory and are sent with each request
- Use secure connections (HTTPS) when transmitting credentials
