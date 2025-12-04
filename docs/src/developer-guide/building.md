# Building

Instructions for building the Tileverse monorepo from source.

## Prerequisites

*   **Java 21+**: For development (the runtime requirement is Java 17+).
*   **Maven 3.9+**: Build tool and dependency management.
*   **Git**: Version control.
*   **Docker**: Required for running integration tests (Testcontainers).

## Quick Start

### Using the Makefile (Recommended)

The project includes a `Makefile` to simplify common tasks:

```bash
# Build everything (compile + test)
make

# Clean build artifacts
make clean

# Compile only
make compile

# Run all tests
make test

# Format code (Spotless + SortPOM)
make format
```

### Using Maven Directly

```bash
# Build and package
./mvnw clean package

# Skip tests for faster build
./mvnw clean package -DskipTests
```

## Build Targets

### Module Specific Builds

You can build specific modules to save time:

```bash
# Build Range Reader only
./mvnw clean package -pl tileverse-rangereader

# Build PMTiles only
./mvnw clean package -pl tileverse-pmtiles
```

## Code Quality

We enforce strict code quality standards using:

*   **Spotless**: For Java code formatting (Palantir style).
*   **SortPOM**: For consistent `pom.xml` ordering.
*   **License Maven Plugin**: To ensure file headers are present.

```bash
# Check code quality
make lint

# Apply fixes automatically
make format
```
