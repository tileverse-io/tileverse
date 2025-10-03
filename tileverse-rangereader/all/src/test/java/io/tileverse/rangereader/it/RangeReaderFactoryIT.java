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
package io.tileverse.rangereader.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.aiven.testcontainers.fakegcsserver.FakeGcsServerContainer;
import io.tileverse.rangereader.RangeReader;
import io.tileverse.rangereader.RangeReaderFactory;
import io.tileverse.rangereader.azure.AzureBlobRangeReader;
import io.tileverse.rangereader.azure.AzureBlobRangeReaderProvider;
import io.tileverse.rangereader.gcs.GoogleCloudStorageRangeReader;
import io.tileverse.rangereader.gcs.GoogleCloudStorageRangeReaderProvider;
import io.tileverse.rangereader.http.HttpRangeReader;
import io.tileverse.rangereader.http.HttpRangeReaderProvider;
import io.tileverse.rangereader.s3.S3RangeReader;
import io.tileverse.rangereader.s3.S3RangeReaderProvider;
import io.tileverse.rangereader.spi.RangeReaderConfig;
import io.tileverse.rangereader.spi.RangeReaderProvider;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.azure.AzuriteContainer;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.NginxContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

@Testcontainers(disabledWithoutDocker = true)
class RangeReaderFactoryIT {

    private static final String BUCKET_NAME = "testbucket";
    private static final String FILE_NAME = "testfile.bin";
    private static final int FILE_SIZE = 1024 * 1024 + 1;

    private static Path fileURI;

    static NginxContainer<?> nginx;

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
        // Create a test file
        fileURI = tempDir.resolve(FILE_NAME);
        TestUtil.createMockTestFile(fileURI, FILE_SIZE);

        setupNginx();
        setupMinIO();
        setupAzurite();
        setupGCS();
    }

    static void setupAzurite() throws IOException {
        // Initialize Blob Service client with client options that disable API version
        // validation
        // Use a lower API version that's compatible with Azurite
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(azurite.getConnectionString())
                .buildClient();

        // Create container
        BlobContainerClient containerClient = blobServiceClient.createBlobContainer(BUCKET_NAME);

        // Upload the test file
        containerClient.getBlobClient(FILE_NAME).uploadFromFile(fileURI.toString(), true);
    }

    @SuppressWarnings("resource")
    static void setupNginx() {
        MountableFile hostPath = MountableFile.forHostPath(fileURI);
        String mountPath = "/usr/share/nginx/html/" + FILE_NAME;
        nginx = new NginxContainer<>("nginx:latest").withCopyToContainer(hostPath, mountPath);
        nginx.start();
    }

    private static void setupMinIO() {
        // Create credentials provider
        AwsBasicCredentials minioCredentials = AwsBasicCredentials.create(minio.getUserName(), minio.getPassword());
        StaticCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(minioCredentials);

        // Initialize S3 client with explicit endpoint configuration for MinIO
        S3Client minioClient = S3Client.builder()
                .endpointOverride(URI.create(minio.getS3URL()))
                .region(Region.US_EAST_1) // MinIO doesn't care about region, but it's required by the SDK
                .credentialsProvider(credentialsProvider)
                .forcePathStyle(true) // Important for S3 compatibility with
                // MinIO
                .build();

        // Create a bucket
        minioClient.createBucket(
                CreateBucketRequest.builder().bucket(BUCKET_NAME).build());

        // Upload the test file
        minioClient.putObject(
                PutObjectRequest.builder().bucket(BUCKET_NAME).key(FILE_NAME).build(), RequestBody.fromFile(fileURI));
        minioClient.close();
    }

    static void setupGCS() throws Exception {
        // Configure the Storage client to use the emulator
        String emulatorHost = gcsEmulator.getHost();
        Integer emulatorPort = gcsEmulator.getFirstMappedPort();
        String emulatorEndpoint = "http://" + emulatorHost + ":" + emulatorPort;

        // Create Storage client
        Storage storage = StorageOptions.newBuilder()
                .setProjectId("test-project")
                .setHost(emulatorEndpoint)
                .build()
                .getService();

        // Create a bucket
        BucketInfo bucketInfo = BucketInfo.newBuilder(BUCKET_NAME).build();
        storage.create(bucketInfo);

        // Upload the test file
        byte[] fileContent = Files.readAllBytes(fileURI);
        BlobId blobId = BlobId.of(BUCKET_NAME, FILE_NAME);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        storage.create(blobInfo, fileContent);
        storage.close();
    }

    @AfterAll
    static void cleanup() {
        if (nginx != null) {
            nginx.stop();
        }
    }

    @Test
    void testHTTP() throws IOException {
        String url = "http://" + nginx.getHost() + ":" + nginx.getFirstMappedPort() + "/" + FILE_NAME;
        testFindBestProvider(URI.create(url), HttpRangeReaderProvider.class);

        RangeReaderConfig config = new RangeReaderConfig().uri(URI.create(url));
        RangeReader reader = testCreate(config, HttpRangeReader.class);
        assertThat(reader.size()).hasValue(FILE_SIZE);
    }

    @Test
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

        testFindBestProvider(URI.create(gcsURL), GoogleCloudStorageRangeReaderProvider.class);

        RangeReaderConfig config = new RangeReaderConfig().uri(gcsURL);
        RangeReader reader = testCreate(config, GoogleCloudStorageRangeReader.class);
        assertThat(reader.size()).hasValue(FILE_SIZE);
    }

    @Test
    void testAzureBlobAzurite() throws IOException {
        final String wellKnownAccountName = "devstoreaccount1";
        Integer port = azurite.getMappedPort(10000);

        String azuriteURI =
                "http://localhost:%d/%s/%s/%s".formatted(port, wellKnownAccountName, BUCKET_NAME, FILE_NAME);

        IOException ioe = assertThrows(IOException.class, () -> testAzureBlob(azuriteURI, null));
        assertThat(ioe).hasMessageContainingAll("AuthorizationFailure", "403", azuriteURI);

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
        testFindBestProvider(URI.create(url), AzureBlobRangeReaderProvider.class);

        RangeReaderConfig config = new RangeReaderConfig().uri(URI.create(url));

        if (accountKey != null) {
            config.setParameter(AzureBlobRangeReaderProvider.ACCOUNT_KEY, accountKey);
        }

        return testCreate(config, AzureBlobRangeReader.class);
    }

    static RangeReader testS3(String uri) throws IOException {
        return testS3(URI.create(uri), null, null);
    }

    static RangeReader testS3(final URI s3URI, String accessKey, String secretKey) throws IOException {

        testFindBestProvider(s3URI, S3RangeReaderProvider.class);

        RangeReaderConfig config = new RangeReaderConfig().uri(s3URI);

        if (accessKey != null && secretKey != null) {
            IOException ioe = assertThrows(IOException.class, () -> RangeReaderFactory.create(s3URI));
            assertThat(ioe.getMessage())
                    .containsAnyOf(
                            "Forbidden",
                            "Failed to access S3 object",
                            "Unable to load credentials from any of the providers");
            config.setParameter(S3RangeReaderProvider.AWS_ACCESS_KEY_ID, accessKey);
            config.setParameter(S3RangeReaderProvider.AWS_SECRET_ACCESS_KEY, secretKey);
        }
        return testCreate(config, S3RangeReader.class);
    }

    static void testFindBestProvider(URI uri, Class<? extends RangeReaderProvider> expected) {
        RangeReaderConfig config = new RangeReaderConfig().uri(uri);
        testFindBestProvider(config, expected);
    }

    static void testFindBestProvider(RangeReaderConfig config, Class<? extends RangeReaderProvider> expected) {
        RangeReaderProvider provider = RangeReaderFactory.findBestProvider(config);
        assertThat(provider)
                .as("With only URI, findBestProvider() should dissambiguate to " + expected.getName())
                .isInstanceOf(expected);
    }

    static RangeReader testCreate(RangeReaderConfig config, Class<? extends RangeReader> expected) throws IOException {
        RangeReader reader = RangeReaderFactory.create(config);
        assertThat(reader).isInstanceOf(expected);
        ByteBuffer range = reader.readRange(0, 100);
        assertThat(range.limit()).isEqualTo(100);
        return reader;
    }
}
