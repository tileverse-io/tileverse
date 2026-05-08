/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tileverse.storage;

import static org.assertj.core.api.Assertions.assertThat;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.google.auth.Credentials;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.aiven.testcontainers.fakegcsserver.FakeGcsServerContainer;
import io.tileverse.storage.azure.AzureBlobStorageProvider;
import io.tileverse.storage.gcs.GoogleCloudStorageProvider;
import io.tileverse.storage.http.HttpStorageProvider;
import io.tileverse.storage.it.TestUtil;
import io.tileverse.storage.s3.S3StorageProvider;
import io.tileverse.storage.spi.StorageProvider;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.azure.AzuriteContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

@Testcontainers(disabledWithoutDocker = true)
class StorageFactoryIT {

    private static final String BUCKET_NAME = "testbucket";
    private static final String FILE_NAME = "testfile.bin";
    private static final int FILE_SIZE = 1024 * 1024 + 1;
    private static final Credentials NO_CREDENTIALS = new Credentials() {
        @Override
        public String getAuthenticationType() {
            return "None";
        }

        @Override
        public Map<String, List<String>> getRequestMetadata(URI uri) {
            return Collections.emptyMap();
        }

        @Override
        public boolean hasRequestMetadata() {
            return false;
        }

        @Override
        public boolean hasRequestMetadataOnly() {
            return false;
        }

        @Override
        public void refresh() {
            // no-op for emulator access
        }
    };

    private static Path fileURI;

    static GenericContainer<?> httpd;

    @Container
    static FakeGcsServerContainer gcsEmulator = new FakeGcsServerContainer();

    @Container
    static MinIOContainer minio = new MinIOContainer("minio/minio:latest");

    @Container
    @SuppressWarnings("resource")
    static AzuriteContainer azurite = new AzuriteContainer("mcr.microsoft.com/azure-storage/azurite:3.35.0")
            .withCommand("azurite-blob --skipApiVersionCheck --loose --blobHost 0.0.0.0 --debug")
            .withExposedPorts(10000, 10001, 10002);

    @BeforeAll
    static void setupContainers(@TempDir Path tempDir) throws Exception {
        fileURI = tempDir.resolve(FILE_NAME);
        TestUtil.createMockTestFile(fileURI, FILE_SIZE);
        setupHttpd();
        setupMinIO();
        setupAzurite();
        setupGCS();
    }

    static void setupAzurite() {
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(azurite.getConnectionString())
                .buildClient();
        BlobContainerClient containerClient = blobServiceClient.createBlobContainer(BUCKET_NAME);
        containerClient.getBlobClient(FILE_NAME).uploadFromFile(fileURI.toString(), true);
    }

    @SuppressWarnings("resource")
    static void setupHttpd() {
        MountableFile hostPath = MountableFile.forHostPath(fileURI);
        String mountPath = "/usr/local/apache2/htdocs/" + FILE_NAME;
        httpd = new GenericContainer<>(DockerImageName.parse("httpd:alpine"))
                .withExposedPorts(80)
                .withCopyToContainer(hostPath, mountPath)
                .waitingFor(Wait.forHttp("/").forPort(80));
        httpd.start();
    }

    private static void setupMinIO() {
        AwsBasicCredentials minioCredentials = AwsBasicCredentials.create(minio.getUserName(), minio.getPassword());
        StaticCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(minioCredentials);
        S3Client minioClient = S3Client.builder()
                .endpointOverride(URI.create(minio.getS3URL()))
                // MinIO ignores region but the SDK requires one for signing.
                .region(Region.US_EAST_1)
                .credentialsProvider(credentialsProvider)
                // path-style addressing is required for S3-compatible servers.
                .forcePathStyle(true)
                .build();
        minioClient.createBucket(
                CreateBucketRequest.builder().bucket(BUCKET_NAME).build());
        minioClient.putObject(
                PutObjectRequest.builder().bucket(BUCKET_NAME).key(FILE_NAME).build(), RequestBody.fromFile(fileURI));
        minioClient.close();
    }

    static void setupGCS() throws Exception {
        String emulatorHost = gcsEmulator.getHost();
        Integer emulatorPort = gcsEmulator.getFirstMappedPort();
        String emulatorEndpoint = "http://" + emulatorHost + ":" + emulatorPort;
        Storage storage = StorageOptions.newBuilder()
                .setProjectId("test-project")
                .setHost(emulatorEndpoint)
                .setCredentials(NO_CREDENTIALS)
                .build()
                .getService();
        storage.create(BucketInfo.newBuilder(BUCKET_NAME).build());
        byte[] fileContent = Files.readAllBytes(fileURI);
        BlobInfo blobInfo =
                BlobInfo.newBuilder(BlobId.of(BUCKET_NAME, FILE_NAME)).build();
        storage.create(blobInfo, fileContent);
        storage.close();
    }

    @AfterAll
    static void cleanup() {
        if (httpd != null) {
            httpd.stop();
        }
    }

    @Test
    void testHTTP() throws IOException {
        String url = "http://" + httpd.getHost() + ":" + httpd.getFirstMappedPort() + "/" + FILE_NAME;
        testFindBestProvider(URI.create(url), HttpStorageProvider.class);

        StorageConfig config = new StorageConfig(url);
        RangeReader reader = testCreate(config);
        assertThat(reader.size()).hasValue(FILE_SIZE);
    }

    @Test
    @SuppressWarnings("java:S125") // The block comment below documents the GCS metadata response; it's not dead code.
    void testGCS() throws IOException {
        String emulatorHost = gcsEmulator.getHost();
        Integer emulatorPort = gcsEmulator.getFirstMappedPort();
        // e.g.: http://localhost:59542/storage/v1/b/testbucket/o/testfile.bin?alt=media"
        /*
         * The alt=media parameter is important for GCS API calls when you want to download the actual file content rather than metadata.
         * Otherwise we'll get metadata like
         * {
         * "kind": "storage#object",
         *   "name": "testfile.bin",
         *   "id": "testbucket/testfile.bin",
         *   "bucket": "testbucket",
         *   "size": "1048576",
         *   "contentType": "application/octet-stream",
         *   "crc32c": "l4iVSw==",
         *   "acl": [
         *     {
         *       "bucket": "testbucket",
         *       "entity": "projectOwner-test-project",
         *       "object": "testfile.bin",
         *       "projectTeam": {},
         *       "role": "OWNER"
         *     }
         *   ],
         *   "md5Hash": "Fq8tADubC1Fqxqio4nuD4w==",
         *   "etag": "\"Fq8tADubC1Fqxqio4nuD4w==\"",
         *   "timeCreated": "2025-09-07T01:41:48.638109Z",
         *   "updated": "2025-09-07T01:41:48.638112Z",
         *   "generation": "1757209308638113"
         * }
         */
        String gcsURL = "http://%s:%d/storage/v1/b/%s/o/%s?alt=media"
                .formatted(emulatorHost, emulatorPort, BUCKET_NAME, FILE_NAME);

        testFindBestProvider(URI.create(gcsURL), GoogleCloudStorageProvider.class);

        StorageConfig config = new StorageConfig(gcsURL);
        RangeReader reader = testCreate(config);
        assertThat(reader.size()).hasValue(FILE_SIZE);
    }

    @Test
    void testAzureBlobAzurite() throws IOException {
        final String wellKnownAccountName = "devstoreaccount1";
        Integer port = azurite.getMappedPort(10000);

        String azuriteURI =
                "http://localhost:%d/%s/%s/%s".formatted(port, wellKnownAccountName, BUCKET_NAME, FILE_NAME);

        // The negative "no credentials -> AuthorizationFailure 403" path that used to flow through
        // the standalone HttpClient is no longer reachable now that AzureBlobStorage routes via
        // BlobServiceClient (which requires a credential at build time). Anonymous public-blob
        // access through AzureBlobStorage is tracked separately. The positive read remains:
        final String wellKnownAccountKey =
                "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
        RangeReader reader = testAzureBlob(azuriteURI, wellKnownAccountKey);
        assertThat(reader.size()).hasValue(FILE_SIZE);
    }

    @Test
    void testS3MinIO() throws IOException {
        final URI minioURI = URI.create("%s/%s/%s".formatted(minio.getS3URL(), BUCKET_NAME, FILE_NAME));
        String accessKey = minio.getUserName();
        String secretKey = minio.getPassword();

        /*
         * Forcing a region for MinIO, or risk an error like the following in github actions (couldn't figure
         * out where it may be getting the region from in my local dev env):
         *
         * Failed to create S3 client: Unable to load region from any of the providers in the chain
         * software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain@62615be: [
         *   software.amazon.awssdk.regions.providers.SystemSettingsRegionProvider@23365142: Unable to load region from system settings.
         *    Region must be specified either via environment variable (AWS_REGION) or  system property (aws.region).,
         *   software.amazon.awssdk.regions.providers.AwsProfileRegionProvider@3f2ef402:
         *    No region provided in profile: default, software.amazon.awssdk.regions.providers.InstanceProfileRegionProvider@68229a6:
         *    Unable to retrieve region information from EC2 Metadata service. Please make sure the application is running on EC2.]         *
         */
        System.setProperty("aws.region", "us-east-1");
        try {
            RangeReader reader = testS3(minioURI, accessKey, secretKey);
            assertThat(reader.size()).hasValue(FILE_SIZE);
        } finally {
            System.clearProperty("aws.region");
        }
    }

    static RangeReader testAzureBlob(String url, String accountKey) throws IOException {
        testFindBestProvider(URI.create(url), AzureBlobStorageProvider.class);

        StorageConfig config = new StorageConfig(url);

        if (accountKey != null) {
            config.setParameter(AzureBlobStorageProvider.AZURE_ACCOUNT_KEY, accountKey);
        }

        return testCreate(config);
    }

    /**
     * Single-arg overload aimed at public-bucket URLs (Overture Maps and similar). Sets {@code S3_ANONYMOUS=true} so
     * the SDK does not consult the default credential chain, which would fail on a clean CI box.
     */
    static RangeReader testS3(String uri) throws IOException {
        return testS3(URI.create(uri), null, null, true);
    }

    static RangeReader testS3(final URI s3URI, String accessKey, String secretKey) throws IOException {
        return testS3(s3URI, accessKey, secretKey, false);
    }

    static RangeReader testS3(final URI s3URI, String accessKey, String secretKey, boolean anonymous)
            throws IOException {
        testFindBestProvider(s3URI, S3StorageProvider.class);
        StorageConfig config = new StorageConfig(s3URI);
        if (accessKey != null && secretKey != null) {
            // The negative "no credentials -> IOException" path is environment-sensitive
            // (the AWS DefaultCredentialsProvider may pick up ambient creds), so we only
            // assert the positive read here. Anonymous-vs-credentialed coverage lives in
            // the s3 module's TCK and the per-backend IT.
            config.setParameter(S3StorageProvider.S3_AWS_ACCESS_KEY_ID, accessKey);
            config.setParameter(S3StorageProvider.S3_AWS_SECRET_ACCESS_KEY, secretKey);
        } else if (anonymous) {
            config.setParameter(S3StorageProvider.S3_ANONYMOUS, true);
        }
        return testCreate(config);
    }

    static void testFindBestProvider(URI uri, Class<? extends StorageProvider> expected) {
        StorageConfig config = new StorageConfig(uri);
        testFindBestProvider(config, expected);
    }

    static void testFindBestProvider(StorageConfig config, Class<? extends StorageProvider> expected) {
        StorageProvider provider = StorageFactory.findProvider(config);
        assertThat(provider)
                .as("With only URI, findProvider() should dissambiguate to " + expected.getName())
                .isInstanceOf(expected);
    }

    /**
     * Open a single-file RangeReader for {@code config.baseUri()} via the two-resource pattern
     * ({@link StorageFactory#open Storage} + {@link Storage#openRangeReader(URI)}), verify a 100-byte read works, and
     * return a reader that closes both on {@link RangeReader#close()} for caller convenience.
     */
    static RangeReader testCreate(StorageConfig config) throws IOException {
        URI leaf = config.baseUri();
        URI parent = leaf.resolve(".");
        // Provider dispatch is driven by the leaf URL's shape (e.g. an S3 path-style URL like
        // http://host/bucket/key); the parent of that URL alone is ambiguous, so we pin the provider id derived from
        // the leaf into the parent-rooted config to keep the dispatcher honest.
        StorageProvider provider = StorageFactory.findProvider(config);
        java.util.Properties props = config.toProperties();
        props.setProperty(StorageConfig.URI_KEY, parent.toString());
        props.setProperty(StorageConfig.PROVIDER_ID_KEY, provider.getId());
        io.tileverse.storage.Storage storage = StorageFactory.open(StorageConfig.fromProperties(props));
        RangeReader reader;
        try {
            reader = storage.openRangeReader(leaf);
        } catch (RuntimeException e) {
            try {
                storage.close();
            } catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
        ByteBuffer range = reader.readRange(0, 100);
        assertThat(range.limit()).isEqualTo(100);
        return new TestOwningRangeReader(reader, storage);
    }

    /** Bundles the storage and the reader so the caller has a single closeable. */
    private static final class TestOwningRangeReader implements RangeReader {

        private final RangeReader delegate;
        private final io.tileverse.storage.Storage owner;

        TestOwningRangeReader(RangeReader delegate, io.tileverse.storage.Storage owner) {
            this.delegate = delegate;
            this.owner = owner;
        }

        @Override
        public int readRange(long offset, int length, ByteBuffer target) {
            return delegate.readRange(offset, length, target);
        }

        @Override
        public java.util.OptionalLong size() {
            return delegate.size();
        }

        @Override
        public String getSourceIdentifier() {
            return delegate.getSourceIdentifier();
        }

        @Override
        public void close() throws IOException {
            try {
                delegate.close();
            } finally {
                owner.close();
            }
        }
    }
}
