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
import io.tileverse.rangereader.it.AbstractRangeReaderIT;
import io.tileverse.rangereader.it.TestUtil;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * Integration tests for S3RangeReader using LocalStack.
 * <p>
 * These tests verify that the S3RangeReader can correctly read ranges of bytes
 * from an S3 bucket using the AWS S3 API against a LocalStack container.
 */
@Testcontainers(disabledWithoutDocker = true)
class S3RangeReaderLocalStackIT extends AbstractRangeReaderIT {

    private static final String BUCKET_NAME = "test-bucket";
    private static final String KEY_NAME = "test.bin";

    private static Path testFile;
    private static S3Client s3Client;
    private static StaticCredentialsProvider credentialsProvider;

    @Container
    @SuppressWarnings("resource")
    static LocalStackContainer localstack = new LocalStackContainer(
                    DockerImageName.parse("localstack/localstack:3.2.0"))
            .withServices(LocalStackContainer.Service.S3);

    @BeforeAll
    static void setupS3() throws IOException {
        // Create a test file
        testFile = TestUtil.createTempTestFile(TEST_FILE_SIZE);

        // Initialize S3 client with explicit endpoint configuration for LocalStack
        credentialsProvider = StaticCredentialsProvider.create(
                AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey()));

        s3Client = S3Client.builder()
                .endpointOverride(localstack.getEndpoint())
                .region(Region.of(localstack.getRegion()))
                .credentialsProvider(credentialsProvider)
                .forcePathStyle(true) // Important for S3 compatibility with LocalStack
                .build();

        // Create a bucket
        s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET_NAME).build());

        // Upload the test file
        s3Client.putObject(
                PutObjectRequest.builder().bucket(BUCKET_NAME).key(KEY_NAME).build(), RequestBody.fromFile(testFile));
    }

    @AfterAll
    static void cleanupS3() {
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
        S3Client client = S3Client.builder()
                .endpointOverride(localstack.getEndpoint())
                .region(Region.of(localstack.getRegion()))
                .credentialsProvider(credentialsProvider)
                .forcePathStyle(true)
                .build();

        return S3RangeReader.builder()
                .s3Client(client)
                .bucket(BUCKET_NAME)
                .key(KEY_NAME)
                .build();
    }

    /**
     * Additional S3-specific tests can go here
     */
    @Test
    void testS3SpecificBuilder() throws IOException {
        // Create S3 URI
        URI s3Uri = URI.create("s3://" + BUCKET_NAME + "/" + KEY_NAME);

        // Create RangeReader using builder with explicit endpoint override and force path style
        try (RangeReader reader = S3RangeReader.builder()
                .uri(s3Uri)
                .credentialsProvider(credentialsProvider)
                .region(Region.of(localstack.getRegion()))
                .endpoint(localstack.getEndpoint())
                .forcePathStyle()
                .build()) {

            // Verify it's the right implementation
            assertTrue(reader instanceof S3RangeReader, "Should be an S3RangeReader instance");
        }
    }
}
