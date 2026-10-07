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
package io.tileverse.storage.s3;

import static org.assertj.core.api.Assertions.assertThat;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.RangeReaderTestSupport;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageEntry;
import io.tileverse.storage.WriteOptions;
import io.tileverse.storage.it.AbstractRangeReaderIT;
import io.tileverse.storage.it.GarageContainer;
import io.tileverse.storage.it.TestUtil;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * Integration tests for S3RangeReader using Garage.
 *
 * <p>These tests verify that the S3RangeReader can correctly read ranges of bytes from an S3-compatible storage using
 * Garage. This demonstrates compatibility with S3-compatible storage systems beyond AWS S3.
 */
@Testcontainers(disabledWithoutDocker = true)
class GarageRangeReaderIT extends AbstractRangeReaderIT {

    private static final String BUCKET_NAME = "test-bucket";
    private static final String KEY_NAME = "test.bin";

    private static Path testFile;
    private static S3Client s3Client;
    private static S3AsyncClient asyncClient;
    private static StaticCredentialsProvider credentialsProvider;

    @Container
    static GarageContainer garage = new GarageContainer();

    @BeforeAll
    static void setupGarage() throws IOException {
        testFile = TestUtil.createTempTestFile(TEST_FILE_SIZE);

        credentialsProvider = StaticCredentialsProvider.create(
                AwsBasicCredentials.create(garage.getAccessKeyId(), garage.getSecretAccessKey()));

        s3Client = S3Client.builder()
                .endpointOverride(URI.create(garage.getS3URL()))
                .region(Region.of(GarageContainer.REGION))
                .credentialsProvider(credentialsProvider)
                .forcePathStyle(true)
                // Garage rejects the SDK's default signed aws-chunked upload with a trailing checksum.
                .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                .build();

        s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET_NAME).build());
        PutObjectRequest putObjectRequest =
                PutObjectRequest.builder().bucket(BUCKET_NAME).key(KEY_NAME).build();
        RequestBody body = RequestBody.fromFile(testFile);
        s3Client.putObject(putObjectRequest, body);

        asyncClient = S3AsyncClient.builder()
                .endpointOverride(URI.create(garage.getS3URL()))
                .region(Region.of(GarageContainer.REGION))
                .credentialsProvider(credentialsProvider)
                .forcePathStyle(true)
                .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED)
                .build();
    }

    @AfterAll
    static void cleanupGarage() {
        if (s3Client != null) {
            s3Client.close();
        }
        if (asyncClient != null) {
            asyncClient.close();
        }
    }

    @Override
    protected RangeReader createBaseReader() throws IOException {
        URI bucketUri = URI.create("s3://" + BUCKET_NAME + "/");
        Storage storage = S3StorageProvider.open(bucketUri, asyncClient);
        try {
            return RangeReaderTestSupport.bundle(storage.openRangeReader(KEY_NAME), storage);
        } catch (RuntimeException e) {
            try {
                storage.close();
            } catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    @Test
    void aFileAboveTheMultipartThresholdUploadsThroughTheCallersClient(@TempDir Path dir) throws IOException {
        // one byte above the multipart threshold: without multipart, the caller's client sends one PutObject
        int size = 8 * 1024 * 1024 + 1;
        Path source = TestUtil.createMockTestFile(dir.resolve("upload.bin"), size);
        int tailOffset = size - 1024;
        byte[] written = Files.readAllBytes(source);
        byte[] writtenTail = Arrays.copyOfRange(written, tailOffset, size);
        String key = "uploaded.bin";
        URI bucketUri = URI.create("s3://" + BUCKET_NAME + "/");
        try (Storage storage = S3StorageProvider.open(bucketUri, asyncClient)) {
            storage.put(key, source, WriteOptions.defaults());
            try {
                assertThat(storage.stat(key)).map(StorageEntry.File::size).hasValue((long) size);
                assertThat(readRange(storage, key, tailOffset, size - tailOffset))
                        .isEqualTo(writtenTail);
            } finally {
                storage.delete(key);
            }
        }
    }

    private static byte[] readRange(Storage storage, String key, long offset, int length) throws IOException {
        try (RangeReader reader = storage.openRangeReader(key)) {
            ByteBuffer range = reader.readRange(offset, length).flip();
            byte[] bytes = new byte[range.remaining()];
            range.get(bytes);
            return bytes;
        }
    }
}
