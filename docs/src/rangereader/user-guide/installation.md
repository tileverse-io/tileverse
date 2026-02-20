# Installation

This guide explains how to add the Tileverse Range Reader library to your Java project.

## Maven Installation

### Using the BOM (Recommended)

The project provides a Bill of Materials (BOM) to manage dependency versions:

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>io.tileverse</groupId>
            <artifactId>tileverse-bom</artifactId>
            <version>1.1.0</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>

<dependencies>
    <!-- Now you can omit versions - they're managed by the BOM -->
    <dependency>
        <groupId>io.tileverse.rangereader</groupId>
        <artifactId>tileverse-rangereader-core</artifactId>
    </dependency>
    <!-- Add cloud provider modules as needed -->
    <dependency>
        <groupId>io.tileverse.rangereader</groupId>
        <artifactId>tileverse-rangereader-s3</artifactId>
    </dependency>
</dependencies>
```

### All Modules (Simple Approach)

Include all functionality with a single dependency:

```xml
<dependency>
    <groupId>io.tileverse.rangereader</groupId>
    <artifactId>tileverse-rangereader-all</artifactId>
    <version>1.1.0</version>
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
    <groupId>io.tileverse.rangereader</groupId>
    <artifactId>tileverse-rangereader-core</artifactId>
    <version>1.1.0</version>
</dependency>
```

#### Cloud Provider Modules

=== "Amazon S3"

    ```xml
    <dependency>
        <groupId>io.tileverse.rangereader</groupId>
        <artifactId>tileverse-rangereader-s3</artifactId>
        <version>1.1.0</version>
    </dependency>
    ```

=== "Azure Blob Storage"

    ```xml
    <dependency>
        <groupId>io.tileverse.rangereader</groupId>
        <artifactId>tileverse-rangereader-azure</artifactId>
        <version>1.1.0</version>
    </dependency>
    ```

=== "Google Cloud Storage"

    ```xml
    <dependency>
        <groupId>io.tileverse.rangereader</groupId>
        <artifactId>tileverse-rangereader-gcs</artifactId>
        <version>1.1.0</version>
    </dependency>
    ```

## Gradle Installation

### Using the BOM (Recommended)

```gradle
dependencyManagement {
    imports {
        mavenBom 'io.tileverse:tileverse-bom:1.1.0'
    }
}

dependencies {
    // Versions managed by the BOM
    implementation 'io.tileverse.rangereader:tileverse-rangereader-core'
    implementation 'io.tileverse.rangereader:tileverse-rangereader-s3'
}
```

### All Modules

```gradle
implementation 'io.tileverse.rangereader:tileverse-rangereader-all:1.1.0'
```

### Individual Modules

```gradle
// Core module (required)
implementation 'io.tileverse.rangereader:tileverse-rangereader-core:1.1.0'

// Cloud provider modules (optional)
implementation 'io.tileverse.rangereader:tileverse-rangereader-s3:1.1.0'
implementation 'io.tileverse.rangereader:tileverse-rangereader-azure:1.1.0'
implementation 'io.tileverse.rangereader:tileverse-rangereader-gcs:1.1.0'
```

## Verify Installation

Create a simple test to verify the installation:

```java
import io.tileverse.rangereader.FileRangeReader;
import java.nio.file.Path;
import java.nio.file.Files;

public class InstallationTest {
    public static void main(String[] args) throws Exception {
        // Create a temporary test file
        Path testFile = Files.createTempFile("test", ".bin");
        Files.write(testFile, "Hello, World!".getBytes());
        
        // Test the library
        try (var reader = FileRangeReader.builder()
                .path(testFile)
                .build()) {
            
            var data = reader.readRange(0, 5);
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
