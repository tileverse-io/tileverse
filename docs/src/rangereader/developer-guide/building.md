# Build System

This guide covers how to build, test, and install the library from source.

## Development Environment

*   **JDK**: Java 17 or later.
*   **Build Tool**: Maven 3.9+.
*   **Docker**: Required for running integration tests (Testcontainers).

## Common Tasks

We provide a `Makefile` to simplify common development commands.

### Building

```bash
# Clean compile
make compile

# Build JARs (skipping tests)
make package
```

### Testing

```bash
# Run unit tests
make test-unit

# Run integration tests (requires Docker)
make test-it
```

### Code Quality

We enforce strict formatting and style guidelines.

```bash
# Check format
make lint

# Fix format issues automatically
make format
```

## Maven Workflow

If you prefer direct Maven commands:

```bash
# Install to local repo
./mvnw clean install -DskipTests

# Run S3 integration tests only
./mvnw verify -pl :tileverse-rangereader-s3
```

## Dependency Management

The project structure separates the core logic from heavy cloud SDKs.



*   **`bom/`**: Defines the Bill of Materials for version alignment.
*   **`dependencies/`**: Centralizes third-party versions (AWS, Azure, etc.).

When adding a new dependency, add it to the `dependencies` module POM first, then reference it in the specific module without a version.
