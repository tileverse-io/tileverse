# Build and test

## Prerequisites

- Java 17 or later, to build and to run. CI also builds on Java 21 and 25.
- Maven 3.9 or later. The repository includes the Maven wrapper, `./mvnw`.
- Docker, for the integration tests.

## Build

The `Makefile` wraps the common tasks:

```bash
make            # install the modules into the local Maven repository, then run the tests
make compile    # compile only
make test       # run the tests
make format     # format the Java sources and sort the POMs
make lint       # check the formatting and the license headers, as CI does
make clean      # remove the build output
```

With Maven directly:

```bash
./mvnw clean package                              # build and test
./mvnw clean package -DskipTests                  # build without the tests
./mvnw clean package -pl :tileverse-pmtiles -am   # one module and the modules it needs
```

Select modules by artifact id (`-pl :artifactId`), and add `-am` to also build the modules they depend on.

Each Maven build formats the Java sources with Spotless, in the Palantir style, and sorts the POMs with SortPOM. `-Dqa` switches both to check mode, as in CI. `-Dcoverage` adds JaCoCo and the aggregate coverage report.

Spotless also adds the license header from `license_header.txt` to new Java files, and `-Dqa` fails on a file without it. Spotless skips `package-info.java` files: copy the header into them by hand.

## Test

Unit tests (`*Test.java`) use JUnit 5 and Mockito. Integration tests (`*IT.java`) run against real services in Docker containers, through [Testcontainers](https://testcontainers.com/).

```bash
./mvnw test                                   # unit tests, or make test-unit
./mvnw verify                                 # unit and integration tests, or make test-it
./mvnw verify -pl :tileverse-storage-s3 -am   # the tests of one module
```

The integration tests use these images:

| Backend | Images |
| :--- | :--- |
| S3 | `localstack/localstack`, and the S3-compatible servers `dxflrs/garage` and `andrewgaul/s3proxy` |
| Azure Blob | `mcr.microsoft.com/azure-storage/azurite` |
| GCS | `fsouza/fake-gcs-server` |
| HTTP | `httpd:alpine` |
