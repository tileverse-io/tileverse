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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for the PMTilesWriter implementation.
 */
public class PMTilesWriterTest {

    @TempDir
    Path tempDir;

    private Path outputPath;

    @BeforeEach
    void setup() {
        outputPath = tempDir.resolve("test.pmtiles");
    }

    @AfterEach
    void cleanup() throws IOException {
        Files.deleteIfExists(outputPath);
    }

    @Test
    void testWriteEmptyFile() throws IOException {
        PMTilesWriter writer = PMTilesWriter.builder().outputPath(outputPath).build();

        assertThrows(IllegalStateException.class, writer::complete);
    }

    @Test
    void testWriteAndReadSingleTile() throws IOException, UnsupportedCompressionException, InvalidHeaderException {
        // Create sample tile data
        byte[] tileData = "Sample tile data".getBytes(StandardCharsets.UTF_8);

        // Create writer and add a tile
        try (PMTilesWriter writer = PMTilesWriter.builder()
                .outputPath(outputPath)
                .minZoom((byte) 0)
                .maxZoom((byte) 0)
                .tileCompression(PMTilesHeader.COMPRESSION_NONE) // No compression for easy testing
                .build()) {

            writer.addTile((byte) 0, 0, 0, tileData);
            writer.complete();
        }

        // Verify the file exists and has content
        assertTrue(Files.exists(outputPath));
        assertTrue(Files.size(outputPath) > 0);

        // Read the file back and verify the content
        try (PMTilesReader reader = new PMTilesReader(outputPath)) {
            PMTilesHeader header = reader.getHeader();

            // Check header values
            assertEquals(0, header.minZoom());
            assertEquals(0, header.maxZoom());
            assertEquals(PMTilesHeader.COMPRESSION_NONE, header.tileCompression());
            assertEquals(PMTilesHeader.TILETYPE_MVT, header.tileType());
            assertEquals(1, header.addressedTilesCount());

            // Read the tile
            Optional<ByteBuffer> readTileData = reader.getTile(0, 0, 0);
            assertTrue(readTileData.isPresent());
            assertArrayEquals(tileData, readTileData.get());
        }
    }

    @Test
    void testWriteAndReadMultipleTiles() throws IOException, UnsupportedCompressionException, InvalidHeaderException {
        // Create sample tile data
        byte[] tileData1 = "Tile data 1".getBytes(StandardCharsets.UTF_8);
        byte[] tileData2 = "Tile data 2".getBytes(StandardCharsets.UTF_8);
        byte[] tileData3 = tileData1; // Same as tile 1 to test deduplication

        // Create writer and add tiles
        try (PMTilesWriter writer = PMTilesWriter.builder()
                .outputPath(outputPath)
                .minZoom((byte) 0)
                .maxZoom((byte) 1)
                .tileCompression(PMTilesHeader.COMPRESSION_NONE) // No compression for easy testing
                .build()) {

            writer.addTile((byte) 0, 0, 0, tileData1);
            writer.addTile((byte) 1, 0, 0, tileData2);
            writer.addTile((byte) 1, 0, 1, tileData3); // Same data as tile 1
            writer.complete();
        }

        // Read the file back and verify
        try (PMTilesReader reader = new PMTilesReader(outputPath)) {
            PMTilesHeader header = reader.getHeader();

            // Check header values
            assertEquals(0, header.minZoom());
            assertEquals(1, header.maxZoom());
            assertEquals(3, header.addressedTilesCount());
            // Should only have 2 unique tile contents due to deduplication
            assertEquals(2, header.tileContentsCount());

            // Read the tiles
            Optional<ByteBuffer> readTileData1 = reader.getTile(0, 0, 0);
            Optional<ByteBuffer> readTileData2 = reader.getTile(1, 0, 0);
            Optional<ByteBuffer> readTileData3 = reader.getTile(1, 0, 1);

            assertTrue(readTileData1.isPresent());
            assertTrue(readTileData2.isPresent());
            assertTrue(readTileData3.isPresent());

            assertArrayEquals(tileData1, readTileData1.get());
            assertArrayEquals(tileData2, readTileData2.get());
            assertArrayEquals(tileData3, readTileData3.get());

            // Verify tile 1 and 3 have the same content in the file
            assertArrayEquals(readTileData1.get(), readTileData3.get());
        }
    }

    @Test
    void testSetMetadata() throws IOException, UnsupportedCompressionException, InvalidHeaderException {
        // Create sample tile data and metadata
        byte[] tileData = "Sample tile data".getBytes(StandardCharsets.UTF_8);
        String metadata = "{\"name\":\"Test Tileset\",\"version\":\"1.0\"}";

        // Create writer and add a tile with metadata
        try (PMTilesWriter writer = PMTilesWriter.builder()
                .outputPath(outputPath)
                .minZoom((byte) 0)
                .maxZoom((byte) 0)
                .tileCompression(PMTilesHeader.COMPRESSION_NONE)
                .internalCompression(PMTilesHeader.COMPRESSION_NONE) // No compression for easy testing
                .build()) {

            writer.addTile((byte) 0, 0, 0, tileData);
            writer.setMetadata(metadata);
            writer.complete();
        }

        // Read the file back and verify the metadata
        try (PMTilesReader reader = new PMTilesReader(outputPath)) {
            String metadataString = reader.getMetadataAsString();

            // Verify metadata content
            assertEquals(metadata, metadataString);
        }
    }

    @Test
    void testProgressTracking() throws IOException {
        // Create sample tile data
        byte[] tileData = "Sample tile data".getBytes(StandardCharsets.UTF_8);

        // Progress tracking
        AtomicBoolean progressReported = new AtomicBoolean(false);
        AtomicReference<Double> lastProgress = new AtomicReference<>(0.0);

        // Create writer with progress listener
        try (PMTilesWriter writer =
                PMTilesWriter.builder().outputPath(outputPath).build()) {

            writer.setProgressListener(new PMTilesWriter.ProgressListener() {
                @Override
                public void onProgress(double progress) {
                    progressReported.set(true);
                    lastProgress.set(progress);
                }

                @Override
                public boolean isCancelled() {
                    return false;
                }
            });

            writer.addTile((byte) 0, 0, 0, tileData);
            writer.complete();
        }

        // Verify progress reporting
        assertTrue(progressReported.get());
        assertEquals(1.0, lastProgress.get(), 0.001);
    }

    private void assertArrayEquals(ByteBuffer expected, ByteBuffer actual) {
        assertArrayEquals(bytes(expected), actual);
    }

    private void assertArrayEquals(byte[] expected, ByteBuffer actual) {
        byte[] byteBuffer = bytes(actual);
        Assertions.assertArrayEquals(expected, byteBuffer);
    }

    private byte[] bytes(ByteBuffer buffer) {
        byte[] byteBuffer = new byte[buffer.remaining()];
        buffer.get(byteBuffer);
        return byteBuffer;
    }
}
