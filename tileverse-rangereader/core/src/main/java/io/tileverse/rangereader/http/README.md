# HTTP Range Reader with Authentication

This package provides authentication support for the `HttpRangeReader` class, enabling secure access to remote resources via HTTP(S).

## Available Authentication Methods

The `HttpRangeReader` supports several authentication methods:

1. **Basic Authentication** - Username/password authentication using the HTTP Basic Auth scheme
2. **Bearer Token Authentication** - Token-based authentication using the Bearer scheme
3. **API Key Authentication** - Authentication using a custom header with an API key
4. **Custom Header Authentication** - Authentication using arbitrary custom headers
5. **Digest Authentication** - HTTP Digest Authentication (MD5, SHA-256, SHA-512)

## Usage Examples

### Basic Authentication

```java
import io.tileverse.rangereader.HttpRangeReader;
import io.tileverse.rangereader.http.BasicAuthentication;

// Create a basic authentication object
BasicAuthentication auth = new BasicAuthentication("username", "password");

// Create an HTTP range reader with authentication
URI uri = URI.create("https://example.com/secure/data.bin");
HttpRangeReader reader = new HttpRangeReader(uri, auth);

// Use the reader to access secured content
ByteBuffer data = reader.readRange(0, 1024);
```

### Bearer Token Authentication

```java
import io.tileverse.rangereader.HttpRangeReader;
import io.tileverse.rangereader.http.BearerTokenAuthentication;

// Create a bearer token authentication object
BearerTokenAuthentication auth = new BearerTokenAuthentication("your-token-here");

// Create an HTTP range reader with authentication
URI uri = URI.create("https://example.com/secure/data.bin");
HttpRangeReader reader = new HttpRangeReader(uri, auth);
```

### API Key Authentication

```java
import io.tileverse.rangereader.HttpRangeReader;
import io.tileverse.rangereader.http.ApiKeyAuthentication;

// Create an API key authentication object (header name and value)
ApiKeyAuthentication auth = new ApiKeyAuthentication("X-API-Key", "your-api-key");

// Create an HTTP range reader with authentication
URI uri = URI.create("https://example.com/secure/data.bin");
HttpRangeReader reader = new HttpRangeReader(uri, auth);
```

### Custom Header Authentication

```java
import io.tileverse.rangereader.HttpRangeReader;
import io.tileverse.rangereader.http.CustomHeaderAuthentication;
import java.util.Map;
import java.util.HashMap;

// Create a map of custom headers
Map<String, String> headers = new HashMap<>();
headers.put("X-Custom-Auth", "auth-value");
headers.put("X-Tenant-ID", "tenant-123");

// Create a custom header authentication object
CustomHeaderAuthentication auth = new CustomHeaderAuthentication(headers);

// Create an HTTP range reader with authentication
URI uri = URI.create("https://example.com/secure/data.bin");
HttpRangeReader reader = new HttpRangeReader(uri, auth);
```

## Using the RangeReaderBuilder

For a more fluent API, you can use the `RangeReaderBuilder`:

```java
import io.tileverse.rangereader.RangeReader;
import io.tileverse.rangereader.RangeReaderBuilder;

// Create a range reader with basic authentication
RangeReader reader = RangeReaderBuilder.create()
    .http(URI.create("https://example.com/secure/data.bin"))
    .withBasicAuth("username", "password")
    .build();

// Create a range reader with bearer token
RangeReader tokenReader = RangeReaderBuilder.create()
    .http(URI.create("https://example.com/secure/data.bin"))
    .withBearerToken("your-token")
    .build();

// Create a range reader with API key
RangeReader apiKeyReader = RangeReaderBuilder.create()
    .http(URI.create("https://example.com/secure/data.bin"))
    .withApiKey("X-API-Key", "your-api-key")
    .build();
```

## Security Considerations

- The `HttpRangeReader` supports SSL/TLS for secure connections
- By default, it accepts all SSL certificates, which is useful for development but not recommended for production
- For production use, configure proper certificate validation:
  ```java
  HttpRangeReader reader = new HttpRangeReader(uri, false, auth); // false = don't trust all certificates
  ```
- Authentication credentials are held in memory and are sent with each request
- Use secure connections (HTTPS) when transmitting credentials