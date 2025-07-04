# Building

Instructions for building the Tileverse Range Reader library from source, including compilation, packaging, code quality, and benchmarks.

For comprehensive testing information, see the **[Testing Guide](testing.md)**.

## Prerequisites

### Required Software

- **Java 21+**: For development (Java 17+ for runtime)
- **Maven 3.9+**: Build tool and dependency management
- **Git**: Version control

### Optional Tools

- **Docker**: For running integration tests with TestContainers
- **IDE**: IntelliJ IDEA, Eclipse, or VS Code with Java extensions

## Quick Start

### Clone and Build

```bash
# Clone the repository
git clone https://github.com/tileverse-io/tileverse-rangereader.git
cd tileverse-rangereader

# Build all modules
./mvnw clean compile

# Run tests
./mvnw test

# Package JARs
./mvnw package
```

### Using the Makefile (Recommended)

The project includes a comprehensive Makefile that wraps Maven commands for common development tasks:

```bash
# Show all available targets
make help

# Build everything (compile + test)
make

# Individual build targets
make clean         # Clean all build artifacts
make compile       # Compile all modules
make package       # Build and create JARs
make install       # Install to local repository

# Code quality
make format        # Format code (Spotless + SortPOM)
make lint          # Check code formatting

# Testing
make test          # Run all tests (unit + integration)
make test-unit     # Run unit tests only
make test-it       # Run integration tests only

# Module-specific testing
make test-core     # Core module unit tests
make test-s3       # S3 module unit tests  
make test-azure    # Azure module unit tests
make test-gcs      # GCS module unit tests

# Module-specific integration tests
make test-core-it  # Core integration tests
make test-s3-it    # S3 integration tests
make test-azure-it # Azure integration tests
make test-gcs-it   # GCS integration tests

# Benchmarks
make build-benchmarks # Build benchmark JAR
make benchmarks      # Run all benchmarks
make benchmarks-file # Run file-based benchmarks only
make benchmarks-gc   # Run benchmarks with GC profiling

# Development workflow
make verify        # Full verification (lint + test)
make quick-build   # Fast build without tests
make dev-setup     # Setup development environment
```

## Build Targets

### Compilation

```bash
# Compile all modules
./mvnw clean compile

# Compile specific module
./mvnw clean compile -pl src/core

# Compile with dependencies
./mvnw clean compile -pl src/s3 -am
```

### Testing

For comprehensive testing information including unit tests, integration tests, TestContainers setup, and performance testing, see the **[Testing Guide](testing.md)**.

Quick testing commands:

```bash
# Run all tests (recommended)
make test

# Test categories
make test-unit     # Unit tests only
make test-it       # Integration tests only (requires Docker)
make perf-test     # Performance tests

# Module-specific testing
make test-core     # Core module tests
make test-s3       # S3 module tests
make test-azure    # Azure module tests
make test-gcs      # GCS module tests

# Direct Maven commands
./mvnw test        # Unit tests
./mvnw verify      # All tests (unit + integration)
```

### Packaging

```bash
# Create JAR files
./mvnw package

# Skip tests during packaging
./mvnw package -DskipTests

# Create JAR with dependencies (fat JAR)
./mvnw package -Pshade
```

### Installation

```bash
# Install to local Maven repository
./mvnw install

# Install without tests
./mvnw install -DskipTests

# Install specific module
./mvnw install -pl src/core
```

## Code Quality

### Formatting

The project uses [Spotless](https://github.com/diffplug/spotless) with Palantir Java Format:

```bash
# Apply formatting (recommended)
make format

# Check formatting without applying changes
make lint

# Format only Java code
make format-java

# Format only POM files
make format-pom

# Direct Maven commands
./mvnw spotless:apply  # Apply Java formatting
./mvnw spotless:check  # Check Java formatting
./mvnw sortpom:sort    # Sort POM files
./mvnw sortpom:verify  # Check POM formatting
```

### License Headers

All Java files must include the Apache 2.0 license header:

```bash
# Check license headers
./mvnw license:check

# Add missing license headers
./mvnw license:format
```

### POM Organization

The project uses [SortPOM](https://github.com/Ekryd/sortpom) to maintain consistent POM structure:

```bash
# Check POM formatting
./mvnw sortpom:verify

# Sort POM files
./mvnw sortpom:sort
```

### Combined Quality Checks

```bash
# Run all quality checks
./mvnw validate

# Using qa profile (check only, no changes)
./mvnw -Pqa validate

# Using Makefile
make lint
```

## Benchmarks

For comprehensive benchmarking information including JMH setup, performance analysis, and benchmark examples, see the **[Testing Guide](testing.md#benchmarks-with-jmh)**.

Quick benchmark commands:

```bash
# Build and run benchmarks
make build-benchmarks  # Build benchmark JAR
make benchmarks        # Run all benchmarks
make benchmarks-gc     # Run with GC profiling

# Direct execution
java -jar benchmarks/target/benchmarks.jar
```

## IDE Configuration

### IntelliJ IDEA

1. **Import Project**:
   - File → Open → Select `pom.xml`
   - Choose "Open as Project"

2. **Configure Code Style**:
   - File → Settings → Editor → Code Style → Java
   - Scheme → Import Scheme → Eclipse XML Profile
   - Import `palantir-java-format.xml` (available in Spotless plugin)

3. **Enable Annotation Processing**:
   - File → Settings → Build → Compiler → Annotation Processors
   - Enable annotation processing

4. **Run Configurations**:
   ```
   # Unit Tests
   Working directory: $MODULE_WORKING_DIR$
   VM options: -ea
   
   # Integration Tests
   VM options: -ea -Dtestcontainers.reuse.enable=true
   ```

### Eclipse

1. **Import Project**:
   - File → Import → Existing Maven Projects
   - Select the root directory

2. **Code Formatting**:
   - Code formatting is automatically applied via the `spotless-maven-plugin` during Maven builds
   - Run `make format` or `./mvnw spotless:apply` to format code
   - The project uses Palantir Java Format (configured in the Maven POM)
   - No additional Eclipse plugins are required

### VS Code

1. **Install Extensions**:
   - Extension Pack for Java
   - Maven for Java (usually included in Extension Pack)

2. **Code Formatting**:
   - Code formatting is automatically applied via the `spotless-maven-plugin` during Maven builds
   - Run `make format` or `./mvnw spotless:apply` to format code
   - The Maven integration will handle code formatting through the build process
   - For manual formatting in the editor, VS Code will use its built-in Java formatter

3. **Optional Settings** (`.vscode/settings.json`):
   ```json
   {
     "java.configuration.updateBuildConfiguration": "automatic",
     "java.test.config.vmargs": ["-ea"]
   }
   ```

## Module Structure

### Core Module (`src/core`)

```bash
# Build core module
./mvnw clean compile -pl src/core

# Test core module
./mvnw test -pl src/core

# Core module structure
src/core/
├── src/main/java/io/tileverse/rangereader/
│   ├── RangeReader.java
│   ├── AbstractRangeReader.java
│   ├── file/FileRangeReader.java
│   ├── http/HttpRangeReader.java
│   ├── cache/
│   └── block/
└── src/test/java/
```

### Cloud Provider Modules

```bash
# Build S3 module
./mvnw clean compile -pl src/s3 -am

# Build Azure module
./mvnw clean compile -pl src/azure -am

# Build GCS module
./mvnw clean compile -pl src/gcs -am
```

### Aggregation Module (`src/all`)

```bash
# Build all module (includes all dependencies)
./mvnw clean compile -pl src/all -am

# This module provides:
# - RangeReaderBuilder (unified builder)
# - RangeReaderFactory
```

### Benchmarks Module

```bash
# Build benchmarks
./mvnw clean compile -pl benchmarks -am

# Requires all other modules
```

## CI-Friendly Versioning

The project uses Maven's CI-friendly versioning:

```bash
# Build with custom version
./mvnw clean package -Drevision=1.2.3

# Build snapshot
./mvnw clean package -Drevision=1.2.3-SNAPSHOT

# The version is controlled by the revision property
```

## Docker Integration

Integration tests require Docker for TestContainers. For detailed TestContainers setup, container configurations, and troubleshooting, see the **[Testing Guide](testing.md#testcontainers-integration)**.

```bash
# Ensure Docker is running
docker --version

# Run integration tests
make test-it
```

## Troubleshooting Build Issues

### Common Problems

**Maven not found**:
```bash
# Use Maven wrapper
./mvnw --version
```

**Java version issues**:
```bash
# Check Java version
java -version
javac -version

# Set JAVA_HOME
export JAVA_HOME=/path/to/java21
```

**Docker issues**:
```bash
# Check Docker is running
docker ps

# Pull required images manually
docker pull localstack/localstack:3.2.0
docker pull minio/minio:latest
```

**Permission issues on scripts**:
```bash
# Make scripts executable
chmod +x mvnw
chmod +x docs/structurizr/*.sh
```

**Out of memory during build**:
```bash
# Increase Maven memory
export MAVEN_OPTS="-Xmx2g -XX:MaxMetaspaceSize=512m"

# Or set in .mvn/jvm.config
echo "-Xmx2g" > .mvn/jvm.config
```

### Clean Build

If you encounter issues, try a clean build:

```bash
# Clean everything
./mvnw clean

# Remove local repository cache (if needed)
rm -rf ~/.m2/repository/io/tileverse/rangereader

# Fresh build
./mvnw clean compile test package
```

## Release Process

### Snapshot Builds

```bash
# Deploy snapshot to repository
./mvnw clean deploy -Drevision=1.0-SNAPSHOT
```

### Release Builds

```bash
# Release version
./mvnw clean deploy -Drevision=1.0.0

# Tag release
git tag v1.0.0
git push origin v1.0.0
```

## Performance Considerations

### Build Performance

```bash
# Parallel builds
./mvnw -T 4 clean compile

# Skip non-essential plugins during development
./mvnw compile -Dspotless.skip -Dsortpom.skip

# Use offline mode (when dependencies are cached)
./mvnw -o compile
```

### Test Performance

```bash
# Run tests in parallel
./mvnw test -Dparallel=classes -DthreadCount=4

# Reuse TestContainers
export TESTCONTAINERS_REUSE_ENABLE=true
./mvnw test -Dtest="*IT"
```

## Next Steps

- **[Architecture](architecture.md)**: Understand the codebase structure
- **[Testing](testing.md)**: Learn about the testing strategy
- **[Contributing](contributing.md)**: Guidelines for contributing code