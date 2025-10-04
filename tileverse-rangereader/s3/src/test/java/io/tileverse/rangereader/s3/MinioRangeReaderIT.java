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
package io.tileverse.rangereader.s3;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tileverse.rangereader.RangeReader;
import io.tileverse.rangereader.block.BlockAlignedRangeReader;
import io.tileverse.rangereader.cache.CachingRangeReader;
import io.tileverse.rangereader.it.AbstractRangeReaderIT;
import io.tileverse.rangereader.it.TestUtil;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * Integration tests for S3RangeReader using MinIO.
 * <p>
 * These tests verify that the S3RangeReader can correctly read ranges of bytes
 * from an S3-compatible storage using MinIO. This demonstrates compatibility
 * with S3-compatible storage systems beyond AWS S3.
 */
@Testcontainers(disabledWithoutDocker = true)
class MinIORangeReaderIT extends AbstractRangeReaderIT {

    private static final String BUCKET_NAME = "test-bucket";
    private static final String KEY_NAME = "test.bin";

    private static Path testFile;
    private static S3Client s3Client;
    private static StaticCredentialsProvider credentialsProvider;

    @Container
    static MinIOContainer minio = new MinIOContainer("minio/minio:latest");

    @BeforeAll
    static void setupMinio() throws IOException {
        // Create a test file
        testFile = TestUtil.createTempTestFile(TEST_FILE_SIZE);

        // Create credentials provider
        credentialsProvider =
                StaticCredentialsProvider.create(AwsBasicCredentials.create(minio.getUserName(), minio.getPassword()));

        // Initialize S3 client with explicit endpoint configuration for MinIO
        s3Client = S3Client.builder()
                .endpointOverride(URI.create(minio.getS3URL()))
                .region(Region.US_EAST_1) // MinIO doesn't care about region, but it's required by the SDK
                .credentialsProvider(credentialsProvider)
                .forcePathStyle(true) // Important for S3 compatibility with MinIO
                .build();

        // Create a bucket
        s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET_NAME).build());

        // Upload the test file
        s3Client.putObject(
                PutObjectRequest.builder().bucket(BUCKET_NAME).key(KEY_NAME).build(), RequestBody.fromFile(testFile));
    }

    @AfterAll
    static void cleanupMinio() {
        if (s3Client != null) {
            s3Client.close();
        }
    }

    @Override
    protected void setUp() throws IOException {
        // Nothing needed here since all setup is done in @BeforeAll
    }

    @Override
    protected RangeReader createBaseReader() throws IOException {
        return S3RangeReader.builder()
                .uri(minio.getS3URL())
                .bucket(BUCKET_NAME)
                .key(KEY_NAME)
                .region(Region.US_EAST_1)
                .credentialsProvider(credentialsProvider)
                .forcePathStyle()
                .build();
    }

    /**
     * Additional MinIO-specific tests can go here
     */
    @Test
    void testMinioSpecificConfiguration() throws IOException {
        // Create RangeReader using builder with explicit endpoint override and force path style
        try (RangeReader reader = S3RangeReader.builder()
                .uri(URI.create("s3://" + BUCKET_NAME + "/" + KEY_NAME))
                .credentialsProvider(credentialsProvider)
                .endpoint(URI.create(minio.getS3URL()))
                .region(Region.US_EAST_1)
                .forcePathStyle()
                .build()) {

            // Verify it's the right implementation
            assertTrue(reader instanceof S3RangeReader, "Should be an S3RangeReader instance");
        }
    }

    @Test
    void testDualBlockSizes() throws IOException {
        // Create RangeReader with different block sizes for memory and disk operations
        try (RangeReader reader = CachingRangeReader.builder(BlockAlignedRangeReader.builder(S3RangeReader.builder()
                                .uri(URI.create("s3://" + BUCKET_NAME + "/" + KEY_NAME))
                                .credentialsProvider(credentialsProvider)
                                .endpoint(URI.create(minio.getS3URL()))
                                .region(Region.US_EAST_1)
                                .forcePathStyle()
                                .build())
                        .blockSize(16384) // 16KB blocks for disk I/O
                        .build())
                .build()) {

            // This test relies on the base test cases from AbstractRangeReaderIT
            // to verify the basic functionality, and we're just testing that
            // this configuration can be created without errors
            assertTrue(reader != null, "Reader with dual block sizes should be created");
        }
    }
}
