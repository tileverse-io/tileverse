# Testing

Comprehensive testing strategy and guidelines for the Tileverse Range Reader library.

## Testing Strategy

The project employs a multi-layered testing approach:

- **Unit Tests** (`*Test.java`): Fast, isolated tests
- **Integration Tests** (`*IT.java`): End-to-end tests with real services
- **Performance Tests** (`*PerformanceTest.java`): Throughput and latency analysis
- **Benchmarks**: JMH-based comprehensive performance testing

## Test Categories

### Unit Tests

Fast tests that verify individual components in isolation:

```bash
# Run all unit tests (recommended)
make test-unit

# Module-specific unit tests
make test-core     # Core module only
make test-s3       # S3 module only
make test-azure    # Azure module only
make test-gcs      # GCS module only

# Direct Maven commands for specific test classes/methods
./mvnw test -Dtest="CachingRangeReaderTest"                    # Specific class
./mvnw test -Dtest="CachingRangeReaderTest#testBasicCaching"   # Specific method
./mvnw test -pl src/core -Dtest="FileRangeReaderTest"         # Class in specific module
```

#### Example Unit Test

```java
@Test
void testBasicFileReading() throws IOException {
    Path testFile = Files.createTempFile("test", ".bin");
    Files.write(testFile, "Hello, World!".getBytes());
    
    try (var reader = FileRangeReader.builder()
            .path(testFile)
            .build()) {
        
        ByteBuffer result = reader.readRange(0, 5);
        assertEquals("Hello", new String(result.array(), 0, result.remaining()));
    }
    
    Files.deleteIfExists(testFile);
}
```

### Integration Tests

End-to-end tests using TestContainers for realistic scenarios:

```bash
# Run all integration tests (recommended)
make test-it

# Module-specific integration tests
make test-core-it  # Core integration tests (HTTP with Nginx)
make test-s3-it    # S3 integration tests (LocalStack + MinIO)
make test-azure-it # Azure integration tests (Azurite)
make test-gcs-it   # GCS integration tests

# With TestContainers reuse for faster execution
export TESTCONTAINERS_REUSE_ENABLE=true
make test-it

# Direct Maven commands for integration tests
./mvnw verify -pl src/s3                # All S3 integration tests
./mvnw verify -pl src/azure             # All Azure integration tests
./mvnw verify -pl src/core              # All core integration tests
./mvnw verify                           # All integration tests

# For specific integration test classes (rarely needed)
./mvnw test -pl src/s3 -Dtest="S3RangeReaderIT"        # Specific S3 test
./mvnw test -pl src/azure -Dtest="AzureBlobRangeReaderIT" # Specific Azure test

# Note: Module-specific make targets run ALL integration tests in that module
# This is usually what you want for comprehensive testing
```

#### TestContainers Setup

All integration tests extend a common base class:

```java
@Testcontainers(disabledWithoutDocker = true)
public class S3RangeReaderIT extends AbstractRangeReaderIT {
    
    @Container
    static LocalStackContainer localstack = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:3.2.0"))
        .withServices(LocalStackContainer.Service.S3);
    
    @Override
    protected RangeReader createBaseReader() throws IOException {
        return S3RangeReader.builder()
            .endpointOverride(localstack.getEndpoint())
            .region(Region.of(localstack.getRegion()))
            .build();
    }
}
```

### Performance Tests

Measure performance characteristics under various conditions:

```bash
# Run performance tests (recommended)
make perf-test

# Direct Maven commands
./mvnw test -Dtest="*PerformanceTest"                                   # All performance tests
./mvnw test -Dtest="RangeReaderPerformanceTest" -Dperformance.iterations=1000  # With custom parameters
./mvnw test -pl src/core -Dtest="*PerformanceTest"                    # Module-specific
```

#### Example Performance Test

```java
@Test
void testLargeFilePerformance() throws IOException {
    Path largeFile = createLargeTestFile(100 * 1024 * 1024); // 100MB
    
    try (var reader = FileRangeReader.builder()
            .path(largeFile)
            .build()) {
        
        long startTime = System.nanoTime();
        
        // Read 1000 random ranges
        for (int i = 0; i < 1000; i++) {
            long offset = ThreadLocalRandom.current().nextLong(largeFile.toFile().length() - 1024);
            reader.readRange(offset, 1024);
        }
        
        long endTime = System.nanoTime();
        double durationMs = (endTime - startTime) / 1_000_000.0;
        
        System.out.println("1000 reads took " + durationMs + "ms");
        assertTrue(durationMs < 10000, "Performance regression detected");
    }
}
```

## Base Test Classes

### AbstractRangeReaderIT

All integration tests extend this base class to ensure consistent behavior:

```java
public abstract class AbstractRangeReaderIT {
    protected static final int TEST_FILE_SIZE = 10 * 1024 * 1024; // 10MB
    
    protected abstract RangeReader createBaseReader() throws IOException;
    
    @Test
    void testBasicRangeReading() throws IOException {
        try (RangeReader reader = createBaseReader()) {
            ByteBuffer data = reader.readRange(0, 1024);
            assertEquals(1024, data.remaining());
        }
    }
    
    @Test
    void testBoundaryConditions() throws IOException {
        try (RangeReader reader = createBaseReader()) {
            long size = reader.size();
            
            // Test reading at EOF
            ByteBuffer data = reader.readRange(size - 10, 20);
            assertEquals(10, data.remaining());
            
            // Test reading beyond EOF
            ByteBuffer empty = reader.readRange(size + 100, 1024);
            assertEquals(0, empty.remaining());
        }
    }
    
    // More common test cases...
}
```

## TestContainers Integration

### Available Test Containers

| Service | Container | Purpose |
|---------|-----------|---------|
| **S3** | `localstack/localstack:3.2.0` | AWS S3 API emulation |
| **MinIO** | `minio/minio:latest` | S3-compatible storage |
| **Azure** | `mcr.microsoft.com/azure-storage/azurite:latest` | Azure Blob Storage |
| **HTTP** | `nginx:alpine` | HTTP server with authentication |

### Container Configuration Examples

#### LocalStack (S3)

```java
@Container
static LocalStackContainer localstack = new LocalStackContainer(
        DockerImageName.parse("localstack/localstack:3.2.0"))
    .withServices(LocalStackContainer.Service.S3)
    .withEnv("DEBUG", "1");

@BeforeAll
static void setupS3() throws IOException {
    S3Client s3Client = S3Client.builder()
        .endpointOverride(localstack.getEndpoint())
        .region(Region.of(localstack.getRegion()))
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create(
                localstack.getAccessKey(), 
                localstack.getSecretKey())))
        .build();
    
    s3Client.createBucket(CreateBucketRequest.builder()
        .bucket("test-bucket")
        .build());
    
    // Upload test file
    s3Client.putObject(
        PutObjectRequest.builder()
            .bucket("test-bucket")
            .key("test-file.bin")
            .build(),
        RequestBody.fromFile(testFile));
}
```

#### MinIO

```java
@Container
static MinIOContainer minio = new MinIOContainer("minio/minio:latest");

@BeforeAll
static void setupMinIO() throws IOException {
    S3Client s3Client = S3Client.builder()
        .endpointOverride(URI.create(minio.getS3URL()))
        .region(Region.US_EAST_1)
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create(
                minio.getUserName(), 
                minio.getPassword())))
        .forcePathStyle(true)
        .build();
    
    // Create bucket and upload test data
}
```

#### Azurite (Azure Blob Storage)

```java
@Container
static GenericContainer<?> azurite = new GenericContainer<>("mcr.microsoft.com/azure-storage/azurite:latest")
    .withExposedPorts(10000)
    .withCommand("azurite-blob", "--blobHost", "0.0.0.0");

@BeforeAll
static void setupAzure() throws IOException {
    String connectionString = String.format(
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;" +
        "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;" +
        "BlobEndpoint=http://%s:%d/devstoreaccount1;",
        azurite.getHost(), azurite.getMappedPort(10000));
    
    BlobServiceClient blobClient = new BlobServiceClientBuilder()
        .connectionString(connectionString)
        .buildClient();
    
    // Create container and upload test data
}
```

## Test Utilities

### TestUtil Class

Common utilities for creating test data:

```java
public class TestUtil {
    
    public static Path createTempTestFile(int sizeBytes) throws IOException {
        Path testFile = Files.createTempFile("rangereader-test", ".bin");
        
        // Create deterministic test data
        byte[] data = new byte[sizeBytes];
        Random random = new Random(42); // Fixed seed for reproducibility
        random.nextBytes(data);
        
        Files.write(testFile, data);
        return testFile;
    }
    
    public static void verifyRangeContent(ByteBuffer actual, byte[] expected, 
                                         int offset, int length) {
        assertEquals(length, actual.remaining());
        
        for (int i = 0; i < length; i++) {
            assertEquals(expected[offset + i], actual.get(i),
                "Mismatch at position " + i);
        }
    }
    
    public static byte[] generateTestData(int size, long seed) {
        byte[] data = new byte[size];
        Random random = new Random(seed);
        random.nextBytes(data);
        return data;
    }
}
```

## Test Data Management

### Consistent Test Data

All tests use the same deterministic test data:

```java
public class AbstractRangeReaderIT {
    protected static final int TEST_FILE_SIZE = 10 * 1024 * 1024; // 10MB
    protected static final long TEST_DATA_SEED = 42L;
    
    protected static byte[] createExpectedData() {
        return TestUtil.generateTestData(TEST_FILE_SIZE, TEST_DATA_SEED);
    }
    
    @Test
    void testRangeConsistency() throws IOException {
        byte[] expectedData = createExpectedData();
        
        try (RangeReader reader = createBaseReader()) {
            // Test various ranges
            verifyRange(reader, expectedData, 0, 1024);
            verifyRange(reader, expectedData, 5000, 2048);
            verifyRange(reader, expectedData, TEST_FILE_SIZE - 1000, 1000);
        }
    }
    
    private void verifyRange(RangeReader reader, byte[] expected, 
                           int offset, int length) throws IOException {
        ByteBuffer actual = reader.readRange(offset, length);
        TestUtil.verifyRangeContent(actual, expected, offset, length);
    }
}
```

## Benchmarks with JMH

### Running Benchmarks

```bash
# Build and run benchmarks (recommended)
make build-benchmarks  # Build benchmark JAR
make benchmarks        # Run all benchmarks

# Specific benchmark types
make benchmarks-file   # Run file-based benchmarks only
make benchmarks-gc     # Run benchmarks with GC profiling

# Build cloud benchmarks (requires TestContainers)
make benchmarks-cloud

# Direct execution
java -jar benchmarks/target/benchmarks.jar                    # All benchmarks
java -jar benchmarks/target/benchmarks.jar FileRangeReader    # Specific benchmark
java -jar benchmarks/target/benchmarks.jar -prof gc           # With profiling
java -jar benchmarks/target/benchmarks.jar -f 3 -wi 5 -i 10   # Custom parameters
```

### Example Benchmark

```java
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class FileRangeReaderBenchmark {
    
    private RangeReader reader;
    private ByteBuffer buffer;
    
    @Setup
    public void setup() throws IOException {
        Path testFile = TestUtil.createTempTestFile(100 * 1024 * 1024);
        reader = FileRangeReader.builder()
            .path(testFile)
            .build();
        buffer = ByteBuffer.allocate(64 * 1024);
    }
    
    @Benchmark
    public int sequentialReads() throws IOException {
        buffer.clear();
        return reader.readRange(ThreadLocalRandom.current().nextLong(1024 * 1024), 
                               64 * 1024, buffer);
    }
    
    @TearDown
    public void tearDown() throws IOException {
        reader.close();
    }
}
```

## Testing Best Practices

### Test Organization

```java
class CachingRangeReaderTest {
    
    @Nested
    @DisplayName("Basic Functionality")
    class BasicFunctionality {
        @Test void testCacheHit() { }
        @Test void testCacheMiss() { }
    }
    
    @Nested
    @DisplayName("Configuration")
    class Configuration {
        @Test void testMaximumSize() { }
        @Test void testExpiration() { }
    }
    
    @Nested
    @DisplayName("Error Handling")
    class ErrorHandling {
        @Test void testDelegateFailure() { }
        @Test void testInvalidParameters() { }
    }
}
```

### Parameterized Tests

```java
@ParameterizedTest
@ValueSource(ints = {100, 1000, 10000})
void testVariousCacheSizes(int cacheSize) throws IOException {
    try (var reader = CachingRangeReader.builder(baseReader)
            .maximumSize(cacheSize)
            .build()) {
        
        ByteBuffer data = reader.readRange(100, 500);
        assertEquals(500, data.remaining());
    }
}
```

### Test Resource Management

```java
@TempDir
Path tempDir;

@Test
void testWithTempDirectory() throws IOException {
    Path testFile = tempDir.resolve("test.bin");
    Files.write(testFile, "test data".getBytes());
    
    try (var reader = FileRangeReader.builder()
            .path(testFile)
            .build()) {
        // Test operations
    }
    // File automatically cleaned up by @TempDir
}
```

## Continuous Integration

### GitHub Actions Testing

The project runs comprehensive tests in CI using Makefile targets:

```yaml
# .github/workflows/pr-validation.yml
jobs:
  build:
    strategy:
      matrix:
        java-version: ['17', '21', '24']
    steps:
      - name: Run unit tests
        run: make test-unit
        
  integration-tests:
    strategy:
      matrix:
        java-version: ['17', '21', '24']
        test-group: ['core', 's3', 'azure', 'gcs']
    steps:
      - name: Run integration tests
        run: make test-${{ matrix.test-group }}-it
      
  quality:
    steps:
      - name: Check formatting
        run: make lint
      - name: Full verification
        run: make verify
```

### Test Parallelization

```bash
# TestContainers reuse for faster integration tests (recommended)
export TESTCONTAINERS_REUSE_ENABLE=true
make test-it

# Module-specific integration tests with reuse
export TESTCONTAINERS_REUSE_ENABLE=true
make test-s3-it

# Direct Maven commands for parallel execution
./mvnw test -Dparallel=classes -DthreadCount=4  # Parallel unit tests
```

## Debugging Tests

### Test Logging

```java
// Enable debug logging for tests
@TestMethodOrder(OrderAnnotation.class)
class DebugTest {
    
    @BeforeEach
    void setupLogging() {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "DEBUG");
        System.setProperty("org.slf4j.simpleLogger.log.io.tileverse.rangereader", "TRACE");
    }
}
```

### IDE Test Configuration

#### IntelliJ IDEA

```
Run Configuration:
- Working directory: $MODULE_WORKING_DIR$
- VM options: -ea -Dtestcontainers.reuse.enable=true
- Environment variables: TESTCONTAINERS_REUSE_ENABLE=true
```

#### Eclipse

```
Run Configuration:
- Arguments tab → VM arguments: -ea
- Environment tab → Add: TESTCONTAINERS_REUSE_ENABLE=true
```

## Test Coverage

### Measuring Coverage

```bash
# Generate coverage report
./mvnw test jacoco:report

# View coverage report
open target/site/jacoco/index.html
```

### Coverage Goals

- **Line Coverage**: > 85%
- **Branch Coverage**: > 80%
- **Method Coverage**: > 90%

## Next Steps

- **[Performance](performance.md)**: Learn about performance testing and optimization
- **[Contributing](contributing.md)**: Guidelines for contributing tests
- **[Building](building.md)**: Build system and test execution