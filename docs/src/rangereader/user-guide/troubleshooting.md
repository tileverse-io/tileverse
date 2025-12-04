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
    <groupId>io.tileverse.rangereader</groupId>
    <artifactId>tileverse-rangereader-s3</artifactId>
    <version>1.1.0</version>
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
   var reader = S3RangeReader.builder()
       .uri(URI.create("s3://bucket/key"))
       .region(Region.US_WEST_2)
       .build();
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
   var reader = S3RangeReader.builder()
       .uri(URI.create("s3://bucket/key"))
       .region(Region.US_WEST_2)  // Correct region
       .build();
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
   var reader = CachingRangeReader.builder(baseReader)
       .maximumSize(1000)
       .build();
   ```

2. **Use disk caching for persistent storage**:
   ```java
   // For large datasets, use disk caching
   var reader = DiskCachingRangeReader.builder(cloudReader)
       .maxCacheSizeBytes(1024 * 1024 * 1024)  // 1GB cache
       .build();
   ```

### High Memory Usage

**Problem**: Application uses too much memory.

**Solutions**:

1. **Use soft references in cache**:
   ```java
   var reader = CachingRangeReader.builder(baseReader)
       .softValues()  // Allow GC to reclaim memory
       .build();
   ```

2. **Limit cache size**:
   ```java
   var reader = CachingRangeReader.builder(baseReader)
       .maximumSize(100)  // Limit entries
       .maxSizeBytes(64 * 1024 * 1024)  // 64MB limit
       .build();
   ```

3. **Use disk caching instead**:
   ```java
   var reader = DiskCachingRangeReader.builder(baseReader)
       .maxCacheSizeBytes(1024 * 1024 * 1024)  // 1GB on disk
       .build();
   ```

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

2. **Ensure consistent read patterns**:
   ```java
   // Good: Consistent block-aligned reads
   for (int i = 0; i < 10; i++) {
       reader.readRange(i * 1024, 1024);  // Cache-friendly
   }
   
   // Bad: Random, unaligned reads
   reader.readRange(100, 500);   // Won't benefit from caching
   reader.readRange(1500, 300);
   ```

3. **Use appropriate read patterns**:
   ```java
   // Ensure consistent read patterns to improve cache hits
   var reader = CachingRangeReader.builder(baseReader)
       .maximumSize(1000)
       .build();
   
   // Read in consistent chunks
   int chunkSize = 64 * 1024;  // 64KB chunks
   for (int i = 0; i < 10; i++) {
       reader.readRange(i * chunkSize, chunkSize);  // Cache-friendly
   }
   ```

## Network Issues

### Connection Timeouts

**Problem**: `SocketTimeoutException` or connection timeouts.

**Solutions**:

1. **Increase timeouts**:
   ```java
   var reader = HttpRangeReader.builder()
       .uri(uri)
       .connectTimeout(Duration.ofSeconds(30))
       .readTimeout(Duration.ofMinutes(5))
       .build();
   ```

2. **Configure retries**:
   ```java
   var reader = HttpRangeReader.builder()
       .uri(uri)
       .maxRetries(3)
       .retryDelay(Duration.ofSeconds(1))
       .build();
   ```

3. **For S3, configure client**:
   ```java
   var s3Client = S3Client.builder()
       .overrideConfiguration(ClientOverrideConfiguration.builder()
           .apiCallTimeout(Duration.ofMinutes(2))
           .apiCallAttemptTimeout(Duration.ofSeconds(30))
           .build())
       .build();
   
   var reader = S3RangeReader.builder()
       .client(s3Client)
       .bucket("bucket")
       .key("key")
       .build();
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
   var reader = HttpRangeReader.builder()
       .uri(uri)
       .trustAllCertificates(true)
       .build();
   ```

2. **Add custom certificate to truststore**:
   ```bash
   keytool -import -alias custom-cert -file cert.crt -keystore $JAVA_HOME/lib/security/cacerts
   ```

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

### Disk Cache Issues

**Problem**: Disk cache not working or filling up disk.

**Solutions**:

1. **Check disk space**:
   ```bash
   df -h /tmp/rangereader-cache
   ```

2. **Configure cache location**:
   ```java
   var reader = DiskCachingRangeReader.builder(baseReader)
       .cacheDirectory("/var/cache/rangereader")  // Custom location
       .maxCacheSizeBytes(5L * 1024 * 1024 * 1024)  // 5GB limit
       .build();
   ```

3. **Enable cleanup on close**:
   ```java
   var reader = DiskCachingRangeReader.builder(baseReader)
       .deleteOnClose()  // Clean up when done
       .build();
   ```

## Debugging Tips

### Enable Debug Logging

```java
// Add to your application startup
System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "DEBUG");
System.setProperty("org.slf4j.simpleLogger.log.io.tileverse.rangereader", "DEBUG");

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
        System.out.println("  Size: " + stats.estimatedSize());
    }
}
```

### Test Connectivity

```java
public void testConnectivity(URI uri) {
    try {
        var reader = createReader(uri);
        long size = reader.size();
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
| `OutOfMemoryError` | Large cache or buffer usage | Reduce cache size or use disk caching |
| `UnsupportedClassVersionError` | Wrong Java version | Use Java 17 or higher |