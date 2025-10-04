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

import io.tileverse.rangereader.RangeReader;
import io.tileverse.rangereader.azure.AzureBlobRangeReader;
import io.tileverse.rangereader.cache.CachingRangeReader;
import io.tileverse.rangereader.http.HttpRangeReader;
import io.tileverse.rangereader.s3.S3RangeReader;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Optional;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;

/**
 * Integration tests for cloud storage access with PMTilesReader using RangeReaderBuilder.
 * <p>
 * These tests demonstrate accessing PMTiles files from different cloud storage providers
 * using the RangeReaderBuilder and PMTilesReader.
 * <p>
 * NOTE: These tests require actual cloud storage access, so they are marked with the
 * "integration" tag and are skipped by default. To run them, you need to:
 * 1. Have the necessary cloud storage credentials configured
 * 2. Have test PMTiles files uploaded to the appropriate locations
 * 3. Configure the test properties via environment variables
 * 4. Run Maven with the integration profile: mvn test -Pintegration
 */
@Tag("integration")
public class CloudStorageIntegrationTest {

    // Test configuration from environment variables
    private static String s3Bucket;
    private static String s3Key;
    private static String s3Region;

    private static String azureConnectionString;
    private static String azureContainer;
    private static String azureBlob;

    private static String httpUrl;

    @BeforeAll
    public static void setup() {
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
     * <p>
     * This test demonstrates:
     * - Creating an S3 RangeReader with the builder pattern
     * - Configuring region and credentials
     * - Adding performance optimizations (caching and block alignment)
     * - Reading the PMTiles header and a tile
     */
    @Test
    void testReadPMTilesFromS3() throws IOException, InvalidHeaderException, UnsupportedCompressionException {
        assumeTrue(
                s3Bucket != null && s3Key != null && s3Region != null,
                "S3 test configuration not found in environment variables");

        URI s3Uri = URI.create("s3://" + s3Bucket + "/" + s3Key);

        try (S3RangeReader s3Reader = S3RangeReader.builder()
                        .uri(s3Uri)
                        .region(Region.of(s3Region))
                        .credentialsProvider(
                                DefaultCredentialsProvider.builder().build())
                        .build();
                RangeReader rangeReader =
                        CachingRangeReader.builder(s3Reader).blockSize(16384).build()) {

            PMTilesReader pmTilesReader = new PMTilesReader(rangeReader::asByteChannel);
            // Verify we can read the header
            PMTilesHeader header = pmTilesReader.getHeader();
            assertNotNull(header);
            System.out.println("PMTiles header: version=" + header.version() + ", minZoom=" + header.minZoom()
                    + ", maxZoom=" + header.maxZoom());

            // Try to read a tile
            int z = header.minZoom();
            Optional<ByteBuffer> tileData = pmTilesReader.getTile(z, 0, 0);
            assertTrue(tileData.isPresent(), "Should be able to read a tile at minimum zoom level");
            System.out.println("Successfully read tile at z=" + z + " with size "
                    + tileData.get().remaining() + " bytes");
        }
    }

    /**
     * Test reading a PMTiles file from Azure Blob Storage using the RangeReaderBuilder.
     * <p>
     * This test demonstrates:
     * - Creating an Azure Blob RangeReader with the builder pattern
     * - Configuring connection string and container/blob path
     * - Adding performance optimizations (caching and block alignment)
     * - Reading the PMTiles header and metadata
     */
    @Test
    void testReadPMTilesFromAzure() throws IOException, InvalidHeaderException, UnsupportedCompressionException {
        assumeTrue(
                azureConnectionString != null && azureContainer != null && azureBlob != null,
                "Azure test configuration not found in environment variables");

        try (AzureBlobRangeReader azureRangeReader = AzureBlobRangeReader.builder()
                        .connectionString(azureConnectionString)
                        .containerName(azureContainer)
                        .blobName(azureBlob)
                        .build();
                RangeReader reader = CachingRangeReader.builder(azureRangeReader)
                        .blockSize(32768)
                        .build()) {

            PMTilesReader pmTilesReader = new PMTilesReader(reader::asByteChannel);
            // Verify we can read the header
            PMTilesHeader header = pmTilesReader.getHeader();
            assertNotNull(header);
            System.out.println("PMTiles header: version=" + header.version() + ", minZoom=" + header.minZoom()
                    + ", maxZoom=" + header.maxZoom());

            // Try to read the metadata
            ByteBuffer metadata = pmTilesReader.getRawMetadata();
            assertNotNull(metadata);
            assertTrue(metadata.remaining() > 0, "Metadata should not be empty");
            System.out.println("Successfully read metadata with size " + metadata.remaining() + " bytes");
            System.out.println("Metadata preview: " + pmTilesReader.getMetadataAsString() + "...");
        }
    }

    /**
     * Test reading a PMTiles file from an HTTP URL using the RangeReaderBuilder.
     * <p>
     * This test demonstrates:
     * - Creating an HTTP RangeReader with the builder pattern
     * - Configuring to trust all certificates (for self-signed certs)
     * - Adding caching for performance
     * - Reading the PMTiles header and a specific tile
     */
    @Test
    void testReadPMTilesFromHttp() throws IOException, InvalidHeaderException, UnsupportedCompressionException {
        assumeTrue(httpUrl != null, "HTTP test configuration not found in environment variables");

        URI httpUri = URI.create(httpUrl);

        try (HttpRangeReader httpRangeReader =
                        HttpRangeReader.builder(httpUri).trustAllCertificates().build();
                RangeReader rangeReader =
                        CachingRangeReader.builder(httpRangeReader).build()) {

            PMTilesReader pmTilesReader = new PMTilesReader(rangeReader::asByteChannel);
            // Verify we can read the header
            PMTilesHeader header = pmTilesReader.getHeader();
            assertNotNull(header);
            System.out.println("PMTiles header: version=" + header.version() + ", minZoom=" + header.minZoom()
                    + ", maxZoom=" + header.maxZoom());

            // Try to read a tile from a specific zoom level
            int z = header.minZoom() + 1;
            if (z <= header.maxZoom()) {
                Optional<ByteBuffer> tileData = pmTilesReader.getTile(z, 0, 0);
                assertTrue(tileData.isPresent(), "Should be able to read a tile at zoom level " + z);
                System.out.println("Successfully read tile at z=" + z + " with size "
                        + tileData.get().remaining() + " bytes");
            }
        }
    }
}
