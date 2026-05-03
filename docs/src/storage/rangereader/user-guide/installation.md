# Installation

This guide explains how to add the `tileverse-storage` library to your Java project. The library exposes both the broader [`Storage`](../../index.md) API and the [`RangeReader`](../index.md) byte-range API in the same artifacts.

## Maven Installation

### Using the BOM (Recommended)

The project provides a Bill of Materials (BOM) to manage dependency versions:

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>io.tileverse</groupId>
            <artifactId>tileverse-bom</artifactId>
            <version>2.0.0</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>

<dependencies>
    <!-- Now you can omit versions - they're managed by the BOM -->
    <dependency>
        <groupId>io.tileverse.storage</groupId>
        <artifactId>tileverse-storage-core</artifactId>
    </dependency>
    <!-- Add cloud provider modules as needed -->
    <dependency>
        <groupId>io.tileverse.storage</groupId>
        <artifactId>tileverse-storage-s3</artifactId>
    </dependency>
</dependencies>
```

### All Modules (Simple Approach)

Include all functionality with a single dependency:

```xml
<dependency>
    <groupId>io.tileverse.storage</groupId>
    <artifactId>tileverse-storage-all</artifactId>
    <version>2.0.0</version>
</dependency>
```

!!! success "No More Netty Conflicts"
    A major benefit of this library is that the `s3` and `azure` modules can be used together without causing `netty` dependency conflicts.

    Historically, using the AWS and Azure Java SDKs in the same project was challenging because they relied on incompatible versions of Netty. This library solves that problem by using alternative HTTP clients (Apache HttpClient for S3, `java.net.HttpClient` for Azure), removing Netty entirely. You can now build multi-cloud applications without complex dependency management.

### Individual Modules (Without BOM)

If you prefer not to use the BOM, specify versions explicitly:

#### Core Module (Required)

```xml
<dependency>
    <groupId>io.tileverse.storage</groupId>
    <artifactId>tileverse-storage-core</artifactId>
    <version>2.0.0</version>
</dependency>
```

#### Cloud Provider Modules

=== "Amazon S3"

    ```xml
    <dependency>
        <groupId>io.tileverse.storage</groupId>
        <artifactId>tileverse-storage-s3</artifactId>
        <version>2.0.0</version>
    </dependency>
    ```

=== "Azure Blob Storage"

    ```xml
    <dependency>
        <groupId>io.tileverse.storage</groupId>
        <artifactId>tileverse-storage-azure</artifactId>
        <version>2.0.0</version>
    </dependency>
    ```

=== "Google Cloud Storage"

    ```xml
    <dependency>
        <groupId>io.tileverse.storage</groupId>
        <artifactId>tileverse-storage-gcs</artifactId>
        <version>2.0.0</version>
    </dependency>
    ```

## Gradle Installation

### Using the BOM (Recommended)

```gradle
dependencyManagement {
    imports {
        mavenBom 'io.tileverse:tileverse-bom:2.0.0'
    }
}

dependencies {
    // Versions managed by the BOM
    implementation 'io.tileverse.storage:tileverse-storage-core'
    implementation 'io.tileverse.storage:tileverse-storage-s3'
}
```

### All Modules

```gradle
implementation 'io.tileverse.storage:tileverse-storage-all:2.0.0'
```

### Individual Modules

```gradle
// Core module (required)
implementation 'io.tileverse.storage:tileverse-storage-core:2.0.0'

// Cloud provider modules (optional)
implementation 'io.tileverse.storage:tileverse-storage-s3:2.0.0'
implementation 'io.tileverse.storage:tileverse-storage-azure:2.0.0'
implementation 'io.tileverse.storage:tileverse-storage-gcs:2.0.0'
```

## Verify Installation

Create a simple test to verify the installation:

```java
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

public class InstallationTest {
    public static void main(String[] args) throws Exception {
        // Create a temporary test file
        Path testFile = Files.createTempFile("test", ".bin");
        Files.write(testFile, "Hello, World!".getBytes());

        // Test the library: open a Storage rooted at the file (FileStorageProvider re-roots
        // at the parent directory transparently) and read the leaf URI.
        try (Storage storage = StorageFactory.open(testFile.toUri());
                RangeReader reader = storage.openRangeReader(testFile.toUri())) {

            ByteBuffer data = reader.readRange(0, 5);
            data.flip();
            String result = new String(data.array(), 0, data.remaining());
            System.out.println("Read: " + result); // Should print "Hello"

            System.out.println("Installation successful!");
        }

        // Clean up
        Files.deleteIfExists(testFile);
    }
}
```

## Migration Guide

### From Other Range Reading Libraries

Common migration patterns:

- **Map offset/length operations** to `readRange()` calls
- **Replace custom caching** with built-in decorators  
- **Adopt builder patterns** for configuration instead of constructors

## Next Steps

- **[Quick Start](quick-start.md)**: Basic usage examples
- **[Configuration](configuration.md)**: Performance optimization
- **[Authentication](authentication.md)**: Cloud provider setup
