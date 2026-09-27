# Troubleshooting

Common issues and solutions when using the Tileverse Range Reader library.

## Installation Issues

### Dependency Conflicts

**Problem**: Maven/Gradle dependency conflicts with AWS, Azure, or Google Cloud SDKs.

**Solution**: Use the BOM (Bill of Materials) for version alignment:

```xml
<dependencyManagement>
    <dependencies>
        <!-- AWS BOM -->
        <dependency>
            <groupId>software.amazon.awssdk</groupId>
            <artifactId>bom</artifactId>
            <version>2.31.70</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
        
        <!-- Azure BOM -->
        <dependency>
            <groupId>com.azure</groupId>
            <artifactId>azure-sdk-bom</artifactId>
            <version>1.2.28</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>
```

### Java Version Issues

**Problem**: `UnsupportedClassVersionError` or similar Java version errors.

**Solution**: Ensure you're using Java 17 or higher:

```bash
java -version
# Should show version 17 or higher

# Set JAVA_HOME if needed
export JAVA_HOME=/path/to/java17
```

### Missing Module Errors

**Problem**: `ClassNotFoundException` for cloud provider classes.

**Solution**: Include the specific module dependency:

```xml
<!-- For S3 support -->
<dependency>
    <groupId>io.tileverse.storage</groupId>
    <artifactId>tileverse-storage-s3</artifactId>
    <version>2.0.0</version>
</dependency>
```

## Authentication Issues

### AWS S3 Authentication

**Problem**: `SdkClientException: Unable to load AWS credentials`

**Solutions**:

1. **Set environment variables**:
   ```bash
   export AWS_ACCESS_KEY_ID=your-access-key
   export AWS_SECRET_ACCESS_KEY=your-secret-key
   export AWS_DEFAULT_REGION=us-west-2
   ```

2. **Create AWS credentials file**:
   ```bash
   mkdir -p ~/.aws
   cat > ~/.aws/credentials << EOF
   [default]
   aws_access_key_id = your-access-key
   aws_secret_access_key = your-secret-key
   EOF
   ```

3. **Use IAM role** (on EC2/ECS):
   ```java
   // No explicit credentials needed - uses instance profile
   var props = new Properties();
   props.setProperty("storage.s3.region", "us-west-2");
   try (var storage = StorageFactory.open(URI.create("s3://bucket/"), props);
           var reader = storage.openRangeReader("key")) {
       // ...
   }
   ```

**Problem**: `S3Exception: Access Denied (Service: S3, Status Code: 403)`

**Solutions**:

1. **Check bucket permissions**:
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": ["s3:GetObject"],
         "Resource": "arn:aws:s3:::your-bucket/*"
       }
     ]
   }
   ```

2. **Verify object exists**:
   ```bash
   aws s3 ls s3://your-bucket/your-key
   ```

3. **Check region**:
   ```java
   // Ensure region matches bucket region
   var props = new Properties();
   props.setProperty("storage.s3.region", "us-west-2");  // Correct region
   try (var storage = StorageFactory.open(URI.create("s3://bucket/"), props);
           var reader = storage.openRangeReader("key")) {
       // ...
   }
   ```

### Azure Blob Storage Authentication

**Problem**: `BlobStorageException: AuthenticationFailed`

**Solutions**:

1. **Verify connection string**:
   ```java
   var connectionString = "DefaultEndpointsProtocol=https;" +
       "AccountName=youraccount;" +
       "AccountKey=yourkey;" +
       "EndpointSuffix=core.windows.net";
   ```

2. **Check SAS token expiration**:
   ```bash
   # Decode SAS token to check expiry
   echo "sv=2020-08-04&se=2024-12-31..." | base64 -d
   ```

3. **Test connectivity**:
   ```bash
   az storage blob list --account-name youraccount --container-name yourcontainer
   ```

### Google Cloud Storage Authentication

**Problem**: `GoogleCloudStorageException: 403 Forbidden`

**Solutions**:

1. **Set service account key**:
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
   ```

2. **Test authentication**:
   ```bash
   gcloud auth application-default login
   gsutil ls gs://your-bucket/
   ```

3. **Check service account permissions**:
   ```bash
   gcloud projects get-iam-policy your-project-id
   ```

## Performance Issues

### Slow Read Performance

**Problem**: Range reads are slower than expected.

**Solutions**:

1. **Enable caching**:
   ```java
   RangeReader reader = CachingRangeReader.of(baseReader);
   ```
   or set `storage.caching.enabled=true` on the configuration passed to `StorageFactory.open` to cache every reader opened by that `Storage`.

### High Memory Usage

**Problem**: Application uses too much memory.

**Solutions**:

1. **Know the cache budget**: the shared range cache is bounded to 20% of the maximum heap, weighed by cached bytes, and an entry expires 60 seconds after its last access. The cache stays empty until readers are wrapped in a `CachingRangeReader` or `storage.caching.enabled` is set.

2. **Keep one `CacheManager`**: every `CacheManager.newInstance()` owns a cache with its own full budget. Readers built without an explicit manager share the default one.

3. **Skip caching for streaming reads**: a job reading a file front to back gets nothing back from the cache and fills it with bytes read once. Use the reader returned by `Storage.openRangeReader` directly.

4. **Align only what repeats**: `BlockAlignedRangeReader.alignWholeFile()` turns every read inside the file into whole cached blocks. Declare only the regions read repeatedly (a header, an index) and let tile or row payloads pass through as exact ranges.

### Cache Not Working

**Problem**: Cache statistics show low hit rates.

**Solutions**:

1. **Check cache configuration**:
   ```java
   if (reader instanceof CachingRangeReader cachingReader) {
       var stats = cachingReader.getCacheStats();
       System.out.println("Hit rate: " + stats.hitRate());
       System.out.println("Miss count: " + stats.missCount());
   }
   ```

2. **Declare block-aligned regions for nearby reads**:
   ```java
   // Good: reads inside a declared BlockAlignedRangeReader region collapse onto shared blocks
   RangeReader aligned = BlockAlignedRangeReader.builder(reader)
       .blockSize(4096)
       .alignRegion(0, 10 * 1024)
       .build();
   for (int i = 0; i < 10; i++) {
       aligned.readRange(i * 1024, 1024);  // cache-friendly: shares 4KB blocks
   }

   // A plain CachingRangeReader only helps a request that repeats the exact same range
   reader.readRange(100, 500);
   reader.readRange(1500, 300);
   ```

3. **Use appropriate read patterns**:
   ```java
   // A repeated read must ask for the same (offset, length): the cache keys on the exact range
   RangeReader reader = CachingRangeReader.of(baseReader);
   
   // Read in consistent chunks
   int chunkSize = 64 * 1024;  // 64KB chunks
   for (int i = 0; i < 10; i++) {
       reader.readRange(i * chunkSize, chunkSize);  // Cache-friendly
   }
   ```

4. **Mind the expiry**: an entry unused for 60 seconds is evicted. A hit rate measured across idle periods drops for that reason alone.

## Network Issues

### Connection Timeouts

**Problem**: `SocketTimeoutException` or connection timeouts.

**Solutions**:

1. **Increase timeouts via Properties**:
   ```java
   var props = new Properties();
   props.setProperty("storage.http.timeout-millis", "30000");

   try (var storage = StorageFactory.open(parent, props);
           var reader = storage.openRangeReader(leaf)) {
       // ...
   }
   ```

2. **For deeper customization, build the HttpClient and inject it**:
   ```java
   HttpClient client = HttpClient.newBuilder()
       .connectTimeout(Duration.ofSeconds(30))
       .build();

   try (var storage = HttpStorageProvider.open(parent, client);
           var reader = storage.openRangeReader(leaf)) {
       // ...
   }
   ```

3. **For S3, configure the client and inject it**:
   ```java
   var s3Client = S3Client.builder()
       .overrideConfiguration(ClientOverrideConfiguration.builder()
           .apiCallTimeout(Duration.ofMinutes(2))
           .apiCallAttemptTimeout(Duration.ofSeconds(30))
           .build())
       .build();

   try (var storage = S3StorageProvider.open(URI.create("s3://bucket/"), s3Client);
           var reader = storage.openRangeReader("key")) {
       // ...
   }
   ```

### Proxy Configuration

**Problem**: Cannot connect through corporate proxy.

**Solutions**:

1. **Set system properties**:
   ```bash
   -Dhttp.proxyHost=proxy.company.com
   -Dhttp.proxyPort=8080
   -Dhttps.proxyHost=proxy.company.com
   -Dhttps.proxyPort=8080
   ```

2. **Configure AWS SDK proxy**:
   ```java
   var proxyConfig = ProxyConfiguration.builder()
       .endpoint(URI.create("http://proxy.company.com:8080"))
       .username("proxyuser")
       .password("proxypass")
       .build();
   
   var s3Client = S3Client.builder()
       .overrideConfiguration(ClientOverrideConfiguration.builder()
           .proxyConfiguration(proxyConfig)
           .build())
       .build();
   ```

### SSL/TLS Issues

**Problem**: SSL certificate validation errors.

**Solutions**:

1. **For development only - disable SSL verification**:
   ```java
   // NOT recommended for production
   var props = new Properties();
   props.setProperty("storage.http.trust-all-certificates", "true");
   try (var storage = StorageFactory.open(parent, props);
           var reader = storage.openRangeReader(leaf)) {
       // ...
   }
   ```

2. **Add custom certificate to truststore**:
   ```bash
   keytool -import -alias custom-cert -file cert.crt -keystore $JAVA_HOME/lib/security/cacerts
   ```

### S3-Compatible Endpoints Without an ETag Header

**Problem**: reads from an S3-compatible service fail with `Response missing required ETag header`, while the
same bytes served over HTTP work. Typical of a gateway that exports an existing tree of files: an object placed
directly in its backend is served without the header, while an object written through the S3 API has one.

**Solution**: none needed. Only the AWS CRT client demands the header. Reads of such an endpoint run on the sync
client instead, from the first rejection on, and an endpoint that sends the header keeps the CRT client.

Those reads give up the CRT client's parallel transfer; batched reads still run their fetches concurrently on the
shared executor.

## File System Issues

### File Access Permissions

**Problem**: `AccessDeniedException` when reading local files.

**Solutions**:

1. **Check file permissions**:
   ```bash
   ls -la /path/to/file
   chmod 644 /path/to/file  # Make readable
   ```

2. **Verify file exists**:
   ```java
   Path filePath = Path.of("/path/to/file");
   if (!Files.exists(filePath)) {
       throw new FileNotFoundException("File not found: " + filePath);
   }
   if (!Files.isReadable(filePath)) {
       throw new IOException("File not readable: " + filePath);
   }
   ```

### Too Many Open Files

**Problem**: `IOException: Too many open files` from a server that opens many local readers.

A local reader holds a file descriptor only while its channel is open: from the first read until
`storage.file.idle-timeout` (default 60 seconds) elapses with no read in progress. Descriptors pile
up when many readers are read within the same minute, or when readers are opened and never closed.

**Solutions**:

1. **Close readers** when a request is done with them; a closed reader never reopens its channel.
2. **Shorten the idle timeout** (`storage.file.idle-timeout=PT10S`) to release descriptors sooner.
3. **Raise the descriptor limit** (`ulimit -n`) when the working set is legitimately large.

### Stale NFS File Handles

**Problem**: reads of a file on an NFS or SMB mount fail after the export was remounted or the file
was replaced on the server; the OS reports `Stale file handle` (Linux) or `Stale NFS file handle`
(macOS, BSD).

**Solution**: none needed for range reads. The reader retires the stale channel, opens a fresh one by
the file's real path and resumes after the bytes already read, three attempts per call. The idle close
keeps a long-lived server from holding handles across remounts in the first place. A read that fails
after the third attempt reports `Read failed after 3 attempts`, which points at a mount that stays
stale.

## Debugging Tips

### Enable Debug Logging

```java
// Add to your application startup
System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "DEBUG");
System.setProperty("org.slf4j.simpleLogger.log.io.tileverse.storage", "DEBUG");

// For AWS SDK
System.setProperty("org.slf4j.simpleLogger.log.software.amazon.awssdk", "DEBUG");

// For Azure SDK
System.setProperty("org.slf4j.simpleLogger.log.com.azure", "DEBUG");
```

### Monitor Cache Performance

```java
public void monitorCache(RangeReader reader) {
    if (reader instanceof CachingRangeReader cachingReader) {
        var stats = cachingReader.getCacheStats();
        
        System.out.println("Cache Statistics:");
        System.out.println("  Hit Rate: " + String.format("%.2f%%", stats.hitRate() * 100));
        System.out.println("  Requests: " + stats.requestCount());
        System.out.println("  Hits: " + stats.hitCount());
        System.out.println("  Misses: " + stats.missCount());
        System.out.println("  Evictions: " + stats.evictionCount());
        System.out.println("  Entries: " + stats.entryCount());
    }
}
```

### Test Connectivity

```java
public void testConnectivity(URI uri) {
    try {
        var reader = createReader(uri);
        long size = reader.size().orElseThrow();
        System.out.println("Successfully connected to " + uri + ", size: " + size);
        reader.close();
    } catch (Exception e) {
        System.err.println("Failed to connect to " + uri + ": " + e.getMessage());
        e.printStackTrace();
    }
}
```

### Profile Performance

```java
public void profileReads(RangeReader reader) {
    int numReads = 100;
    int blockSize = 64 * 1024;
    
    long startTime = System.nanoTime();
    
    for (int i = 0; i < numReads; i++) {
        try {
            reader.readRange(i * blockSize, blockSize);
        } catch (IOException e) {
            System.err.println("Read failed at offset " + (i * blockSize));
        }
    }
    
    long endTime = System.nanoTime();
    double durationMs = (endTime - startTime) / 1_000_000.0;
    
    System.out.println("Read " + numReads + " blocks in " + durationMs + "ms");
    System.out.println("Average: " + (durationMs / numReads) + "ms per read");
}
```

## Getting Help

If you're still experiencing issues:

1. **Check the logs** for detailed error messages
2. **Search GitHub issues** for similar problems
3. **Create a minimal reproduction** case
4. **Submit an issue** with:
   - Library version
   - Java version
   - Operating system
   - Complete error message and stack trace
   - Minimal code example

## Common Error Messages

| Error | Likely Cause | Solution |
|-------|--------------|----------|
| `ClassNotFoundException` | Missing module dependency | Add required module to dependencies |
| `Access Denied (403)` | Authentication/authorization | Check credentials and permissions |
| `NoSuchFileException` | File not found | Verify file/object exists |
| `SocketTimeoutException` | Network timeout | Increase timeout or check connectivity |
| `OutOfMemoryError` | Several `CacheManager` instances, or caching a streaming workload | The shared cache holds at most 20% of the maximum heap per manager; keep one manager and skip caching for reads done once |
| `Response missing required ETag header` | S3-compatible endpoint serving a file it never received through the S3 API | Handled automatically; reads fall back to the sync client |
| `UnsupportedClassVersionError` | Wrong Java version | Use Java 17 or higher |