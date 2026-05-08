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
package io.tileverse.pmtiles;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import io.tileverse.jackson.databind.pmtiles.v3.PMTilesMetadata;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;
import io.tileverse.storage.azure.AzureBlobStorageProvider;
import io.tileverse.storage.cache.CachingRangeReader;
import io.tileverse.storage.s3.S3StorageProvider;
import io.tileverse.tiling.pyramid.TileIndex;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Integration tests for cloud storage access with PMTilesReader using RangeReaderBuilder.
 *
 * <p>These tests demonstrate accessing PMTiles files from different cloud storage providers using the
 * RangeReaderBuilder and PMTilesReader.
 *
 * <p>NOTE: These tests require actual cloud storage access, so they are marked with the "integration" tag and are
 * skipped by default. To run them, you need to: 1. Have the necessary cloud storage credentials configured 2. Have test
 * PMTiles files uploaded to the appropriate locations 3. Configure the test properties via environment variables 4. Run
 * Maven with the integration profile: mvn test -Pintegration
 */
@Slf4j
@Tag("integration")
class CloudStorageIntegrationTest {

    // Test configuration from environment variables
    private static String s3Bucket;
    private static String s3Key;
    private static String s3Region;

    private static String azureConnectionString;
    private static String azureContainer;
    private static String azureBlob;

    private static String httpUrl;

    @BeforeAll
    static void setup() {
        // S3 configuration
        s3Bucket = System.getenv("TEST_S3_BUCKET");
        s3Key = System.getenv("TEST_S3_KEY");
        s3Region = System.getenv("TEST_S3_REGION");

        // Azure configuration
        azureConnectionString = System.getenv("TEST_AZURE_CONNECTION_STRING");
        azureContainer = System.getenv("TEST_AZURE_CONTAINER");
        azureBlob = System.getenv("TEST_AZURE_BLOB");

        // HTTP configuration
        httpUrl = System.getenv("TEST_HTTP_URL");
    }

    /**
     * Test reading a PMTiles file from S3 using the RangeReaderBuilder.
     *
     * <p>This test demonstrates: - Creating an S3 RangeReader with the builder pattern - Configuring region and
     * credentials - Adding performance optimizations (caching and block alignment) - Reading the PMTiles header and a
     * tile
     */
    @Test
    void testReadPMTilesFromS3() throws IOException {
        assumeTrue(
                s3Bucket != null && s3Key != null && s3Region != null,
                "S3 test configuration not found in environment variables");

        URI bucketUri = URI.create("s3://" + s3Bucket + "/");

        S3Client s3Client = S3Client.builder()
                .region(Region.of(s3Region))
                .credentialsProvider(DefaultCredentialsProvider.builder().build())
                .build();

        try (S3Client closeable = s3Client;
                Storage storage = S3StorageProvider.open(bucketUri, s3Client);
                RangeReader baseReader = storage.openRangeReader(s3Key);
                RangeReader rangeReader =
                        CachingRangeReader.builder(baseReader).blockSize(16384).build()) {

            PMTilesReader pmTilesReader = new PMTilesReader(rangeReader);
            // Verify we can read the header
            PMTilesHeader header = pmTilesReader.getHeader();
            assertNotNull(header);
            log.debug(
                    "PMTiles header: version={}, minZoom={}, maxZoom={}",
                    header.version(),
                    header.minZoom(),
                    header.maxZoom());

            // Try to read a tile
            int z = header.minZoom();
            Optional<ByteBuffer> tileData = pmTilesReader.getTile(TileIndex.zxy(z, 0, 0));
            assertTrue(tileData.isPresent(), "Should be able to read a tile at minimum zoom level");
            log.debug(
                    "Successfully read tile at z={} with size {} bytes",
                    z,
                    tileData.get().remaining());
        }
    }

    /**
     * Test reading a PMTiles file from Azure Blob Storage using the RangeReaderBuilder.
     *
     * <p>This test demonstrates: - Creating an Azure Blob RangeReader with the builder pattern - Configuring connection
     * string and container/blob path - Adding performance optimizations (caching and block alignment) - Reading the
     * PMTiles header and metadata
     */
    @Test
    void testReadPMTilesFromAzure() throws IOException {
        assumeTrue(
                azureConnectionString != null && azureContainer != null && azureBlob != null,
                "Azure test configuration not found in environment variables");

        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(azureConnectionString)
                .buildClient();

        // Container endpoint URI is required by AzureBlobStorageProvider.open; derive it from the service client.
        URI containerUri = URI.create(
                blobServiceClient.getBlobContainerClient(azureContainer).getBlobContainerUrl() + "/");

        try (Storage storage = AzureBlobStorageProvider.open(containerUri, blobServiceClient);
                RangeReader baseReader = storage.openRangeReader(azureBlob);
                RangeReader reader =
                        CachingRangeReader.builder(baseReader).blockSize(32768).build()) {

            PMTilesReader pmTilesReader = new PMTilesReader(reader);
            // Verify we can read the header
            PMTilesHeader header = pmTilesReader.getHeader();
            assertNotNull(header);
            log.debug(
                    "PMTiles header: version={}, minZoom={}, maxZoom={}",
                    header.version(),
                    header.minZoom(),
                    header.maxZoom());

            // Try to read the metadata
            PMTilesMetadata metadata = pmTilesReader.getMetadata();
            assertNotNull(metadata);
        }
    }

    /**
     * Test reading a PMTiles file from an HTTP URL using the RangeReaderBuilder.
     *
     * <p>This test demonstrates: - Creating an HTTP RangeReader with the builder pattern - Configuring to trust all
     * certificates (for self-signed certs) - Adding caching for performance - Reading the PMTiles header and a specific
     * tile
     */
    @Test
    void testReadPMTilesFromHttp() throws IOException {
        assumeTrue(httpUrl != null, "HTTP test configuration not found in environment variables");

        URI httpUri = URI.create(httpUrl);

        Properties props = new Properties();
        props.setProperty("storage.http.trust-all-certificates", "true");

        String full = httpUri.toString();
        URI parent = URI.create(full.substring(0, full.lastIndexOf('/') + 1));
        try (io.tileverse.storage.Storage storage = StorageFactory.open(parent, props);
                RangeReader baseReader = storage.openRangeReader(httpUri);
                RangeReader rangeReader = CachingRangeReader.builder(baseReader).build()) {

            PMTilesReader pmTilesReader = new PMTilesReader(rangeReader);
            // Verify we can read the header
            PMTilesHeader header = pmTilesReader.getHeader();
            assertNotNull(header);
            log.debug(
                    "PMTiles header: version={}, minZoom={}, maxZoom={}",
                    header.version(),
                    header.minZoom(),
                    header.maxZoom());

            // Try to read a tile from a specific zoom level
            int z = header.minZoom() + 1;
            if (z <= header.maxZoom()) {
                Optional<ByteBuffer> tileData = pmTilesReader.getTile(TileIndex.zxy(z, 0, 0));
                assertTrue(tileData.isPresent(), "Should be able to read a tile at zoom level " + z);
                log.debug(
                        "Successfully read tile at z={} with size {} bytes",
                        z,
                        tileData.get().remaining());
            }
        }
    }
}
