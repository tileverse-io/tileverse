# Contributing

Guidelines for contributing to the Tileverse Range Reader library.

## Our Mission: Unifying the Java Geospatial Ecosystem

The Tileverse Range Reader project addresses a critical architectural gap in the Java ecosystem. We're building the **unified I/O foundation** that the geospatial community has needed—a lightweight, extensible abstraction comparable to Python's **fsspec** library.

### Strategic Impact

Your contributions help solve real ecosystem problems:

- **Reduce fragmentation**: Every major Java geospatial library has implemented its own incompatible I/O solution
- **Lower barriers**: Make it easier for developers to build cloud-native format readers
- **Improve performance**: Enable consistent optimization patterns across the ecosystem
- **Foster innovation**: Provide a stable foundation for next-generation geospatial tools

### Ecosystem Integration Goals

We're actively working toward broader ecosystem adoption:

1. **Format Library Integration**: Collaborating with projects like `imageio-ext` and `netCDF-Java` to consolidate around our unified API
2. **Community Standards**: Proposing our patterns as community standards through organizations like LocationTech and OSGeo
3. **Cloud-Native Formats**: Building reference implementations for **PMTiles**, **GeoParquet**, **FlatGeobuf**, and other emerging formats
4. **Framework Support**: Ensuring compatibility with popular frameworks like GeoTools, Apache SIS, and GeoTrellis

## Getting Started

### Prerequisites

- **Java 21+** for development (Java 17+ for runtime)
- **Maven 3.9+** for building and dependency management
- **Docker** for integration tests with TestContainers
- **Git** for version control
- **IDE** with Java support (IntelliJ IDEA, Eclipse, or VS Code)

### Development Setup

1. **Fork and Clone**:
   ```bash
   # Fork the repository on GitHub, then:
   git clone https://github.com/YOUR_USERNAME/tileverse-rangereader.git
   cd tileverse-rangereader
   
   # Add upstream remote
   git remote add upstream https://github.com/tileverse-io/tileverse-rangereader.git
   ```

2. **Build and Test**:
   ```bash
   # Build all modules
   ./mvnw clean compile
   
   # Run unit tests
   ./mvnw test
   
   # Run integration tests (requires Docker)
   ./mvnw verify
   
   # Apply code formatting
   ./mvnw spotless:apply
   ```

3. **IDE Setup**:
   - Import as Maven project
   - Configure code style (Palantir Java Format)
   - Enable annotation processing
   - Set up run configurations for tests

## Contribution Types

### Code Contributions

#### New Features
- Add support for new storage backends
- Implement new optimization decorators
- Enhance existing functionality
- Add integration with cloud providers

#### Bug Fixes
- Fix incorrect behavior
- Resolve performance issues
- Address security vulnerabilities
- Improve error handling

#### Performance Improvements
- Optimize hot code paths
- Improve caching strategies
- Reduce memory usage
- Enhance network efficiency

### Documentation Contributions

#### User Documentation
- Usage examples and tutorials
- Configuration guides
- Troubleshooting information
- Best practices

#### Developer Documentation
- API documentation improvements
- Architecture explanations
- Contributing guidelines
- Performance analysis

### Testing Contributions

#### Test Coverage
- Unit tests for new functionality
- Integration tests for cloud providers
- Performance benchmarks
- Edge case validation

#### Test Infrastructure
- TestContainers improvements
- CI/CD enhancements
- Test data generation
- Benchmark automation

## Code Contribution Process

### 1. Planning Your Contribution

Before starting work:

1. **Check existing issues** for similar work
2. **Create an issue** if one doesn't exist
3. **Discuss your approach** with maintainers
4. **Get feedback** on design decisions

### 2. Development Workflow

#### Create a Feature Branch

```bash
# Update your fork
git checkout main
git pull upstream main
git push origin main

# Create feature branch
git checkout -b feature/your-feature-name
```

#### Make Your Changes

Follow these guidelines:

- **Small, focused commits** with clear messages
- **Incremental development** with working code at each step
- **Follow existing patterns** and conventions
- **Write tests first** when possible (TDD)

#### Commit Message Format

Use conventional commit format:

```
type(scope): brief description

Longer description if needed

Fixes #123
```

Examples:
```
feat(s3): add support for S3-compatible endpoints
fix(cache): resolve memory leak in disk cache
docs(api): improve JavaDoc for RangeReader interface
test(azure): add integration tests for Azure authentication
```

### 3. Code Quality Standards

#### Code Style

The project uses automated formatting and quality checks:

```bash
# Apply code formatting
./mvnw spotless:apply

# Check formatting
./mvnw spotless:check

# Run all quality checks
./mvnw validate
```

#### Required Elements

1. **License Headers**: All Java files must include Apache 2.0 license headers
2. **JavaDoc**: Public APIs require comprehensive documentation
3. **Tests**: All new functionality must have tests
4. **Thread Safety**: All implementations must be thread-safe

#### Code Review Checklist

- [ ] Code follows project conventions
- [ ] All tests pass locally
- [ ] New functionality has tests
- [ ] Public APIs have JavaDoc
- [ ] Performance impact considered
- [ ] Thread safety maintained
- [ ] Error handling appropriate

### 4. Testing Requirements

#### Unit Tests

All new functionality requires unit tests:

```java
@Test
void testNewFeature() throws IOException {
    // Given
    RangeReader reader = createTestReader();
    
    // When
    ByteBuffer result = reader.readRange(0, 1024);
    
    // Then
    assertEquals(1024, result.remaining());
    // Additional assertions...
}
```

#### Integration Tests

New data sources require integration tests:

```java
@Testcontainers
public class NewDataSourceIT extends AbstractRangeReaderIT {
    
    @Container
    static GenericContainer<?> testContainer = new GenericContainer<>("test-image:latest")
        .withExposedPorts(8080);
    
    @Override
    protected RangeReader createBaseReader() throws IOException {
        return NewDataSourceReader.builder()
            .endpoint(testContainer.getHost())
            .port(testContainer.getMappedPort(8080))
            .build();
    }
}
```

#### Performance Tests

Performance-sensitive changes need benchmarks:

```java
@Test
void testPerformanceRegression() throws IOException {
    RangeReader reader = createOptimizedReader();
    
    long startTime = System.nanoTime();
    for (int i = 0; i < 1000; i++) {
        reader.readRange(i * 1024, 1024);
    }
    long endTime = System.nanoTime();
    
    double durationMs = (endTime - startTime) / 1_000_000.0;
    assertTrue(durationMs < 1000, "Performance regression detected");
}
```

### 5. Pull Request Process

#### Before Submitting

```bash
# Rebase on latest main
git fetch upstream
git rebase upstream/main

# Run full test suite
./mvnw clean verify

# Check formatting and quality
./mvnw validate

# Push to your fork
git push origin feature/your-feature-name
```

#### Pull Request Description

Include in your PR description:

```markdown
## Summary
Brief description of changes

## Changes Made
- List of specific changes
- Any breaking changes
- Performance impact

## Testing
- Types of tests added
- How to test the changes
- Test coverage information

## Documentation
- Documentation updated
- Examples provided
- Breaking changes documented

Fixes #issue-number
```

#### Review Process

1. **Automated Checks**: CI must pass
2. **Code Review**: At least one maintainer approval
3. **Testing**: Integration tests in CI environment
4. **Documentation**: Verify docs are updated
5. **Merge**: Squash and merge when approved

## Development Guidelines

### Architecture Principles

#### 1. Decorator Pattern

All decorators follow the same pattern:

```java
public class MyDecorator extends AbstractRangeReader {
    private final RangeReader delegate;
    
    public MyDecorator(RangeReader delegate) {
        this.delegate = delegate;
    }
    
    @Override
    protected int readRangeNoFlip(long offset, int length, ByteBuffer target) 
            throws IOException {
        // Add decoration logic here
        return delegate.readRange(offset, length, target);
    }
    
    @Override
    public void close() throws IOException {
        delegate.close();
    }
    
    public static Builder builder(RangeReader delegate) {
        return new Builder(delegate);
    }
}
```

#### 2. Builder Pattern

All readers provide fluent builders:

```java
public static class Builder {
    private String endpoint;
    private Duration timeout = Duration.ofSeconds(30);
    
    public Builder endpoint(String endpoint) {
        this.endpoint = endpoint;
        return this;
    }
    
    public Builder timeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }
    
    public MyRangeReader build() {
        return new MyRangeReader(endpoint, timeout);
    }
}
```

#### 3. Thread Safety

All implementations must be thread-safe:

```java
public class ThreadSafeReader extends AbstractRangeReader {
    private final AtomicLong requestCount = new AtomicLong();
    private final ConcurrentHashMap<String, String> cache = new ConcurrentHashMap<>();
    
    @Override
    protected int readRangeNoFlip(long offset, int length, ByteBuffer target) 
            throws IOException {
        requestCount.incrementAndGet();
        // Thread-safe implementation
    }
}
```

### Performance Considerations

#### Memory Management

- Use off-heap storage for large caches
- Implement proper cleanup in `close()` methods
- Avoid memory leaks in long-running applications

#### Network Optimization

- Minimize network requests through block alignment
- Use connection pooling for HTTP clients
- Implement retry logic with exponential backoff

#### Caching Strategy

- Design cache keys for optimal hit rates
- Implement cache eviction policies
- Monitor cache performance metrics

### Error Handling

#### Exception Types

Use specific exception types:

```java
// Good: Specific exception types
public class AuthenticationFailedException extends IOException {
    public AuthenticationFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}

// Avoid: Generic exceptions
throw new RuntimeException("Something went wrong");
```

#### Error Context

Provide helpful error messages:

```java
// Good: Contextual error information
throw new IOException(
    "Failed to read range [" + offset + ", " + (offset + length) + ") " +
    "from " + getSourceIdentifier() + ": " + e.getMessage(), e);

// Avoid: Unclear errors
throw new IOException("Read failed", e);
```

#### Retry Logic

Implement appropriate retry strategies:

```java
public ByteBuffer readWithRetry(long offset, int length) throws IOException {
    int attempts = 0;
    while (attempts < maxRetries) {
        try {
            return readRange(offset, length);
        } catch (IOException e) {
            if (isRetryable(e) && attempts < maxRetries - 1) {
                attempts++;
                sleep(calculateDelay(attempts));
                continue;
            }
            throw e;
        }
    }
}
```

## Community Guidelines

### Communication

- **Be respectful** and professional in all interactions
- **Ask questions** when you need clarification
- **Share knowledge** and help others learn
- **Provide constructive feedback** in code reviews

### Issue Reporting

When reporting bugs:

1. **Search existing issues** first
2. **Provide minimal reproduction** case
3. **Include environment details** (Java version, OS, etc.)
4. **Add relevant logs** and stack traces

### Feature Requests

For new features:

1. **Explain the use case** and motivation
2. **Discuss alternatives** you've considered
3. **Provide implementation ideas** if possible
4. **Consider compatibility** with existing APIs

## Recognition

### Contributors

We recognize contributions through:

- **Contributor list** in documentation
- **Release notes** acknowledgment  
- **GitHub contributor** statistics
- **Community spotlight** for major contributions

### Maintainer Path

Active contributors may be invited to become maintainers based on:

- **Consistent quality** contributions
- **Community engagement** and support
- **Technical expertise** in relevant areas
- **Commitment** to project goals

## Getting Help

### Resources

- **Documentation**: Start with user and developer guides
- **Examples**: Check the `examples/` directory
- **Tests**: Review existing tests for patterns
- **Issues**: Search GitHub issues for similar problems

### Contact

- **GitHub Issues**: For bugs and feature requests
- **Discussions**: For questions and ideas
- **Email**: For security issues and sensitive topics
- **Discord/Slack**: For real-time community chat (link in README)

### Mentorship

New contributors can get help through:

- **Good first issues**: Labeled issues for beginners
- **Mentoring**: Experienced contributors provide guidance
- **Pair programming**: Virtual sessions for complex features
- **Code review**: Learning through the review process

## Legal

### Contributor License Agreement

By contributing, you agree that:

- Your contributions are your original work
- You grant the project rights to use your contributions
- Your contributions are under the Apache 2.0 license
- You have authority to make the contribution

### Code of Conduct

This project follows a **Code of Conduct** that requires:

- **Respectful communication** with all participants
- **Constructive feedback** and criticism
- **Inclusive behavior** welcoming to all backgrounds
- **Professional conduct** in all project spaces

Violations can be reported to project maintainers and will be addressed according to established procedures.

## API Stability

### Stable APIs (Semantic Versioning)

The following APIs follow semantic versioning guarantees:

- **`RangeReader` interface**: Core contract that won't change incompatibly
- **`AbstractRangeReader` public methods**: Base implementation signatures  
- **Builder public APIs**: All builder methods and their behavior
- **Core decorator classes**: `CachingRangeReader`, `DiskCachingRangeReader`

Changes to these APIs require major version increments and migration guides.

### Experimental APIs (Subject to Change)

The following may change between minor versions:

- **Internal implementation details**: Package-private classes and methods
- **Benchmark and testing utilities**: Performance testing infrastructure
- **SPI interfaces**: Service provider interfaces may evolve
- **Configuration classes**: Internal configuration objects

When working with experimental APIs, expect potential changes and plan accordingly.

## Next Steps

Ready to contribute? Here's how to get started:

1. **Browse issues** labeled "good first issue"
2. **Join discussions** about features you're interested in
3. **Set up your development** environment
4. **Start with documentation** or small bug fixes
5. **Ask questions** and engage with the community

Your contributions help build the foundation for the next generation of cloud-native geospatial applications in Java. Welcome to the community!
EOF < /dev/null