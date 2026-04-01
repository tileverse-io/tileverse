/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 */
package io.tileverse.geotools.parquet;

import com.google.auth.Credentials;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.aiven.testcontainers.fakegcsserver.FakeGcsServerContainer;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class GeoParquetFileDataStoreFactoryGcsIT {

    private static final String BUCKET_NAME = "geoparquet-it";
    private static final String PROJECT_ID = "test-project";
    private static final Credentials NO_CREDENTIALS = new Credentials() {
        @Override
        public String getAuthenticationType() {
            return "None";
        }

        @Override
        public Map<String, List<String>> getRequestMetadata(java.net.URI uri) {
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

    @Container
    static FakeGcsServerContainer gcsEmulator = new FakeGcsServerContainer();

    private static URL gcsFixtureUrl;

    @BeforeAll
    static void setupGcs(@TempDir Path tempDir) throws Exception {
        Path parquetFixture =
                GeoParquetDatastoreCloudITSupport.extractFixture(GeoParquetFileDataStoreFactoryGcsIT.class, tempDir);
        uploadFixture(parquetFixture);
        String emulatorUrl = "http://%s:%d/storage/v1/b/%s/o/%s?alt=media"
                .formatted(
                        gcsEmulator.getHost(),
                        gcsEmulator.getFirstMappedPort(),
                        BUCKET_NAME,
                        GeoParquetDatastoreCloudITSupport.OBJECT_NAME);
        gcsFixtureUrl = new URL(emulatorUrl);
    }

    @Test
    void createDataStore_readsParquetThroughParameterizedGcsRangeReader() throws Exception {
        Map<String, Object> params = Map.of(
                "url",
                gcsFixtureUrl,
                "io.tileverse.rangereader.provider",
                "gcs",
                "io.tileverse.rangereader.gcs.project-id",
                PROJECT_ID,
                "io.tileverse.rangereader.gcs.default-credentials-chain",
                false);

        GeoParquetDatastoreCloudITSupport.assertReadsSampleGeoParquet(params);
    }

    private static void uploadFixture(Path parquetFixture) throws IOException {
        String emulatorEndpoint = "http://" + gcsEmulator.getHost() + ":" + gcsEmulator.getFirstMappedPort();
        Storage storage = StorageOptions.newBuilder()
                .setProjectId(PROJECT_ID)
                .setHost(emulatorEndpoint)
                .setCredentials(NO_CREDENTIALS)
                .build()
                .getService();
        try {
            storage.create(BucketInfo.newBuilder(BUCKET_NAME).build());
            storage.create(
                    BlobInfo.newBuilder(BlobId.of(BUCKET_NAME, GeoParquetDatastoreCloudITSupport.OBJECT_NAME))
                            .build(),
                    java.nio.file.Files.readAllBytes(parquetFixture));
        } finally {
            try {
                storage.close();
            } catch (Exception e) {
                throw new IOException("Failed to close GCS emulator client", e);
            }
        }
    }
}
