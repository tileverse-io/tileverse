/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 */
package io.tileverse.geotools.parquet;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URL;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
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

@Testcontainers(disabledWithoutDocker = true)
class GeoParquetFileDataStoreFactoryMinIOIT {

    private static final String BUCKET_NAME = "geoparquet-it";

    @Container
    static MinIOContainer minio = new MinIOContainer("minio/minio:latest");

    private static URL minioFixtureUrl;

    @BeforeAll
    static void setupMinio(@TempDir Path tempDir) throws Exception {
        Path parquetFixture =
                GeoParquetDatastoreCloudITSupport.extractFixture(GeoParquetFileDataStoreFactoryMinIOIT.class, tempDir);
        uploadFixture(parquetFixture);
        minioFixtureUrl = new URL(
                "%s/%s/%s".formatted(minio.getS3URL(), BUCKET_NAME, GeoParquetDatastoreCloudITSupport.OBJECT_NAME));
    }

    @Test
    void createDataStore_requiresS3ConnectionParametersForMinioHttpUrl() {
        assertThatThrownBy(
                        () -> GeoParquetDatastoreCloudITSupport.FACTORY.createDataStore(Map.of("url", minioFixtureUrl)))
                .isInstanceOf(io.tileverse.storage.AccessDeniedException.class);
    }

    @Test
    void createDataStore_readsParquetThroughParameterizedS3RangeReader() throws Exception {
        Map<String, Object> params = Map.of(
                "url",
                minioFixtureUrl,
                "storage.provider",
                "s3",
                "storage.s3.force-path-style",
                true,
                "storage.s3.region",
                "us-east-1",
                "storage.s3.aws-access-key-id",
                minio.getUserName(),
                "storage.s3.aws-secret-access-key",
                minio.getPassword());

        GeoParquetDatastoreCloudITSupport.assertReadsSampleGeoParquet(params);
    }

    private static void uploadFixture(Path parquetFixture) {
        AwsBasicCredentials credentials = AwsBasicCredentials.create(minio.getUserName(), minio.getPassword());
        try (S3Client client = S3Client.builder()
                .endpointOverride(java.net.URI.create(minio.getS3URL()))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                .forcePathStyle(true)
                .build()) {
            client.createBucket(
                    CreateBucketRequest.builder().bucket(BUCKET_NAME).build());
            client.putObject(
                    PutObjectRequest.builder()
                            .bucket(BUCKET_NAME)
                            .key(GeoParquetDatastoreCloudITSupport.OBJECT_NAME)
                            .build(),
                    RequestBody.fromFile(parquetFixture));
        }
    }
}
