# Authentication

Configure secure authentication for cloud storage providers and HTTP sources.

## HTTP Authentication

The library supports multiple HTTP authentication methods through the `HttpRangeReader`.

### Basic Authentication

Username and password authentication:

```java
var reader = HttpRangeReader.builder()
    .uri(URI.create("https://secure.example.com/data.bin"))
    .withBasicAuth("username", "password")
    .build();
```

### Bearer Token Authentication

JWT or other bearer tokens:

```java
var reader = HttpRangeReader.builder()
    .uri(URI.create("https://api.example.com/data.bin"))
    .withBearerToken("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...")
    .build();
```

### API Key Authentication

Custom API key headers:

```java
var reader = HttpRangeReader.builder()
    .uri(URI.create("https://api.example.com/data.bin"))
    .withApiKey("X-API-Key", "your-api-key-here")
    .build();
```

### Custom Header Authentication

Multiple custom headers for complex authentication:

```java
var reader = HttpRangeReader.builder()
    .uri(URI.create("https://api.example.com/data.bin"))
    .withCustomHeaders(Map.of(
        "X-Auth-Token", "token-value",
        "X-Client-ID", "client-123",
        "X-Signature", "hmac-signature"
    ))
    .build();
```

### Digest Authentication

HTTP Digest authentication with multiple algorithms:

```java
// MD5 digest (default)
var reader = HttpRangeReader.builder()
    .uri(URI.create("https://secure.example.com/data.bin"))
    .withDigestAuth("username", "password")
    .build();

// SHA-256 digest
var reader = HttpRangeReader.builder()
    .uri(URI.create("https://secure.example.com/data.bin"))
    .withDigestAuth("username", "password", "SHA-256")
    .build();
```

## Amazon S3 Authentication

S3 authentication uses the AWS SDK credential chain.

### Default Credential Chain

The library automatically discovers credentials in this order:

1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. Java system properties (`aws.accessKeyId`, `aws.secretAccessKey`)
3. Web Identity Token from AWS STS
4. Shared credential files (`~/.aws/credentials`)
5. Amazon ECS container credentials
6. Amazon EC2 instance profile credentials

```java
// Uses default credential chain
var reader = S3RangeReader.builder()
    .uri(URI.create("s3://my-bucket/data.bin"))
    .region(Region.US_WEST_2)
    .build();
```

### Environment Variables

```bash
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export AWS_DEFAULT_REGION=us-west-2
```

### AWS Credentials File

Create `~/.aws/credentials`:

```ini
[default]
aws_access_key_id = AKIAIOSFODNN7EXAMPLE
aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

[production]
aws_access_key_id = AKIAI44QH8DHBEXAMPLE
aws_secret_access_key = je7MtGbClwBF/2Zp9Utk/h3yCo8nvbEXAMPLEKEY
```

Create `~/.aws/config`:

```ini
[default]
region = us-west-2

[profile production]
region = us-east-1
```

### Programmatic Credentials

```java
// Static credentials (not recommended for production)
var credentialsProvider = StaticCredentialsProvider.create(
    AwsBasicCredentials.create("access-key", "secret-key"));

var reader = S3RangeReader.builder()
    .uri(URI.create("s3://bucket/key"))
    .credentialsProvider(credentialsProvider)
    .region(Region.US_WEST_2)
    .build();
```

### Profile-Based Credentials

```java
// Use specific profile
var credentialsProvider = ProfileCredentialsProvider.create("production");

var reader = S3RangeReader.builder()
    .uri(URI.create("s3://bucket/key"))
    .credentialsProvider(credentialsProvider)
    .region(Region.US_EAST_1)
    .build();
```

### IAM Roles (Recommended)

For applications running on AWS:

```java
// Automatically uses EC2 instance profile or ECS task role
var reader = S3RangeReader.builder()
    .uri(URI.create("s3://bucket/key"))
    .region(Region.US_WEST_2)
    .build();
```

### STS Assume Role

```java
var stsClient = StsClient.builder()
    .region(Region.US_WEST_2)
    .build();

var credentialsProvider = StsAssumeRoleCredentialsProvider.builder()
    .stsClient(stsClient)
    .refreshRequest(AssumeRoleRequest.builder()
        .roleArn("arn:aws:iam::123456789012:role/S3AccessRole")
        .roleSessionName("rangereader-session")
        .build())
    .build();

var reader = S3RangeReader.builder()
    .uri(URI.create("s3://bucket/key"))
    .credentialsProvider(credentialsProvider)
    .region(Region.US_WEST_2)
    .build();
```

## Azure Blob Storage Authentication

Azure supports multiple authentication methods.

### Connection String (Simplest)

```java
var connectionString = "DefaultEndpointsProtocol=https;" +
    "AccountName=mystorageaccount;" +
    "AccountKey=StorageAccountKeyEndingIn==;" +
    "EndpointSuffix=core.windows.net";

var reader = AzureBlobRangeReader.builder()
    .uri(URI.create("https://mystorageaccount.blob.core.windows.net/container/blob"))
    .connectionString(connectionString)
    .build();
```

### Account Key

```java
var credential = new StorageSharedKeyCredential("accountname", "accountkey");

var reader = AzureBlobRangeReader.builder()
    .uri(URI.create("https://accountname.blob.core.windows.net/container/blob"))
    .credential(credential)
    .build();
```

### SAS Token (Recommended)

```java
var sasToken = "sv=2020-08-04&ss=b&srt=sco&sp=r&se=2023-12-31T23:59:59Z&st=2023-01-01T00:00:00Z&spr=https&sig=signature";

var reader = AzureBlobRangeReader.builder()
    .uri(URI.create("https://account.blob.core.windows.net/container/blob"))
    .sasToken(sasToken)
    .build();
```

### Azure AD Authentication

```java
// Default Azure credential chain
var credential = new DefaultAzureCredentialBuilder().build();

var reader = AzureBlobRangeReader.builder()
    .uri(URI.create("https://account.blob.core.windows.net/container/blob"))
    .credential(credential)
    .build();
```

### Service Principal

```java
var credential = new ClientSecretCredentialBuilder()
    .clientId("client-id")
    .clientSecret("client-secret")
    .tenantId("tenant-id")
    .build();

var reader = AzureBlobRangeReader.builder()
    .uri(URI.create("https://account.blob.core.windows.net/container/blob"))
    .credential(credential)
    .build();
```

### Managed Identity

For applications running on Azure:

```java
var credential = new ManagedIdentityCredentialBuilder()
    .clientId("user-assigned-identity-client-id")  // Optional for user-assigned
    .build();

var reader = AzureBlobRangeReader.builder()
    .uri(URI.create("https://account.blob.core.windows.net/container/blob"))
    .credential(credential)
    .build();
```

## Google Cloud Storage Authentication

GCS uses service accounts and Application Default Credentials.

### Application Default Credentials (ADC)

Automatically discovers credentials in this order:

1. `GOOGLE_APPLICATION_CREDENTIALS` environment variable
2. gcloud user credentials
3. Google Cloud SDK default credentials
4. Google Compute Engine service account

```java
// Uses ADC
var reader = GoogleCloudStorageRangeReader.builder()
    .uri(URI.create("gs://my-bucket/object"))
    .build();
```

### Service Account Key File

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
```

```java
var reader = GoogleCloudStorageRangeReader.builder()
    .uri(URI.create("gs://bucket/object"))
    .build();
```

### Programmatic Service Account

```java
var credentials = ServiceAccountCredentials.fromStream(
    new FileInputStream("/path/to/service-account.json"));

var reader = GoogleCloudStorageRangeReader.builder()
    .uri(URI.create("gs://bucket/object"))
    .credentials(credentials)
    .build();
```

### Service Account from JSON

```java
var credentialsJson = """
{
  "type": "service_account",
  "project_id": "my-project",
  "private_key_id": "key-id",
  "private_key": "-----BEGIN PRIVATE KEY-----\\n...\\n-----END PRIVATE KEY-----\\n",
  "client_email": "service-account@my-project.iam.gserviceaccount.com",
  "client_id": "123456789",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token"
}
""";

var credentials = ServiceAccountCredentials.fromStream(
    new ByteArrayInputStream(credentialsJson.getBytes()));

var reader = GoogleCloudStorageRangeReader.builder()
    .uri(URI.create("gs://bucket/object"))
    .credentials(credentials)
    .build();
```

### Compute Engine Service Account

For applications running on Google Compute Engine:

```java
var credentials = ComputeEngineCredentials.create();

var reader = GoogleCloudStorageRangeReader.builder()
    .uri(URI.create("gs://bucket/object"))
    .credentials(credentials)
    .build();
```

## Security Best Practices

### Credential Storage

- **Never hardcode credentials** in source code
- **Use environment variables** for development
- **Use managed identities** in cloud environments
- **Rotate credentials regularly**
- **Use least privilege** access policies

### Network Security

```java
// Enable SSL/TLS verification
var reader = HttpRangeReader.builder()
    .uri(URI.create("https://secure.example.com/data.bin"))
    .verifySSL(true)  // Default is true
    .build();

// For development with self-signed certificates (not recommended for production)
var reader = HttpRangeReader.builder()
    .uri(URI.create("https://dev.example.com/data.bin"))
    .trustAllCertificates(true)  // Only for development
    .build();
```

### Access Control

#### S3 Bucket Policy Example

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::ACCOUNT:user/rangereader-user"
      },
      "Action": [
        "s3:GetObject",
        "s3:GetObjectRange"
      ],
      "Resource": "arn:aws:s3:::my-bucket/*"
    }
  ]
}
```

#### Azure Blob Storage Access Policy

```json
{
  "Version": "2020-08-04",
  "SignedPermissions": "r",
  "SignedResource": "b",
  "SignedExpiry": "2024-12-31T23:59:59Z"
}
```

## Troubleshooting Authentication

### Common Issues

**AWS S3 Access Denied**:
```
Exception: Access Denied (Service: S3, Status Code: 403)
```
- Check IAM permissions
- Verify bucket policy
- Ensure correct region

**Azure Blob Storage Authentication Failed**:
```
Exception: AuthenticationFailed (Status Code: 403)
```
- Verify connection string or SAS token
- Check token expiration
- Ensure correct storage account

**Google Cloud Storage Forbidden**:
```
Exception: 403 Forbidden
```
- Check service account permissions
- Verify `GOOGLE_APPLICATION_CREDENTIALS`
- Ensure correct project ID

### Debugging Authentication

```java
// Enable debug logging
System.setProperty("org.slf4j.simpleLogger.log.software.amazon.awssdk", "DEBUG");
System.setProperty("org.slf4j.simpleLogger.log.com.azure", "DEBUG");
System.setProperty("org.slf4j.simpleLogger.log.com.google.cloud", "DEBUG");

// Test authentication separately
try {
    var reader = S3RangeReader.builder()
        .uri(URI.create("s3://bucket/key"))
        .build();
    
    long size = reader.size();  // This will test authentication
    System.out.println("Authentication successful, file size: " + size);
} catch (Exception e) {
    System.err.println("Authentication failed: " + e.getMessage());
}
```

## Next Steps

- **[Troubleshooting](troubleshooting.md)**: Common authentication issues
- **[Configuration](configuration.md)**: Advanced configuration options
- **[Quick Start](quick-start.md)**: Usage examples with authentication