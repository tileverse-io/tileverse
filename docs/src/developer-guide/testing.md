# Testing

This guide covers the testing strategy for the Tileverse project.

## Overview

We use a tiered testing approach:

1.  **Unit Tests**: Fast, in-memory tests using JUnit 5 and Mockito.
2.  **Integration Tests**: Docker-based tests using [Testcontainers](https://testcontainers.com/) to verify interactions with real services (S3, Azure, GCS, HTTP).
3.  **Benchmarks**: JMH microbenchmarks for critical paths.

## Running Tests

### Unit Tests

```bash
make test-unit
# OR
./mvnw test
```

### Integration Tests

Requires Docker to be running.

```bash
make test-it
# OR
./mvnw verify
```

### Specific Modules

```bash
# Run tests for PMTiles only
./mvnw test -pl tileverse-pmtiles

# Run S3 integration tests
./mvnw verify -pl tileverse-rangereader/s3
```

## Test Infrastructure

### Testcontainers

We use specific images to emulate cloud environments:

*   **AWS S3**: `localstack/localstack`
*   **Azure Blob**: `mcr.microsoft.com/azure-storage/azurite`
*   **GCS**: `fsouza/fake-gcs-server`
*   **HTTP**: `nginx:alpine`

### Performance Benchmarks

Benchmarks are located in the `tileverse-rangereader/benchmarks` module.

```bash
# Build benchmarks
make build-benchmarks

# Run
make benchmarks
```
