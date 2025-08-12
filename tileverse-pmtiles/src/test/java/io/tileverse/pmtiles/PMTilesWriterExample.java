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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Example usage of the PMTilesWriter to create a simple PMTiles file.
 */
public class PMTilesWriterExample {

    /**
     * Example method that demonstrates how to create a PMTiles file.
     *
     * @param outputPath the path to write the PMTiles file to
     * @throws IOException if an I/O error occurs
     */
    public static void createExamplePMTiles(Path outputPath) throws IOException {
        // Create some example tile data
        byte[] emptyTile = createSampleTileData("Empty tile");
        byte[] landTile = createSampleTileData("Land tile with features");
        byte[] oceanTile = createSampleTileData("Ocean tile");

        // Create a PMTilesWriter
        try (PMTilesWriter writer = PMTilesWriter.builder()
                .outputPath(outputPath)
                .minZoom((byte) 0)
                .maxZoom((byte) 2)
                .tileCompression(PMTilesHeader.COMPRESSION_GZIP)
                .tileType(PMTilesHeader.TILETYPE_MVT)
                .bounds(-180, -85.05113, 180, 85.05113)
                .center(0, 0, (byte) 0)
                .build()) {

            // Add some tiles
            // Root tile
            writer.addTile((byte) 0, 0, 0, emptyTile);

            // Zoom level 1
            writer.addTile((byte) 1, 0, 0, oceanTile);
            writer.addTile((byte) 1, 0, 1, oceanTile); // Duplicate content for RLE
            writer.addTile((byte) 1, 1, 0, landTile);
            writer.addTile((byte) 1, 1, 1, landTile); // Duplicate content for RLE

            // Zoom level 2 (just a few sample tiles)
            writer.addTile((byte) 2, 0, 0, oceanTile);
            writer.addTile((byte) 2, 0, 1, oceanTile);
            writer.addTile((byte) 2, 1, 0, landTile);
            writer.addTile((byte) 2, 1, 1, landTile);
            writer.addTile((byte) 2, 2, 2, landTile);
            writer.addTile((byte) 2, 3, 3, emptyTile);

            // Set metadata
            writer.setMetadata(
                    """
                {
                  "name": "Example Tileset",
                  "format": "pbf",
                  "description": "A simple example tileset",
                  "version": "1.0.0",
                  "attribution": "© Example Contributors",
                  "vector_layers": [
                    {
                      "id": "land",
                      "description": "Land features",
                      "minzoom": 0,
                      "maxzoom": 2,
                      "fields": {
                        "type": "string",
                        "area": "number"
                      }
                    }
                  ]
                }
                """);

            // Set a progress listener
            writer.setProgressListener(new PMTilesWriter.ProgressListener() {
                @Override
                public void onProgress(double progress) {
                    System.out.printf("Progress: %.1f%%\n", progress * 100);
                }

                @Override
                public boolean isCancelled() {
                    return false;
                }
            });

            // Complete the file
            writer.complete();
        }

        System.out.println("PMTiles file created successfully at: " + outputPath);

        // Read back and show some information
        try (PMTilesReader reader = new PMTilesReader(outputPath)) {
            PMTilesHeader header = reader.getHeader();
            System.out.println("\nPMTiles file information:");
            System.out.println("Tile count: " + header.addressedTilesCount());
            System.out.println("Unique tile contents: " + header.tileContentsCount());
            System.out.println("Zoom levels: " + header.minZoom() + " to " + header.maxZoom());
            System.out.println("Bounds: " + header.minLonE7() / 10000000.0
                    + "," + header.minLatE7() / 10000000.0
                    + "," + header.maxLonE7() / 10000000.0
                    + "," + header.maxLatE7() / 10000000.0);

            // Read metadata
            String metadata = reader.getMetadataAsString();
            System.out.println("\nMetadata: " + metadata);

            // Read a specific tile
            Optional<ByteBuffer> tileData = reader.getTile(0, 0, 0);
            if (tileData.isPresent()) {
                String stringData = reader.toString(tileData.get());
                System.out.println("\nRoot tile data: " + stringData);
            }
        } catch (InvalidHeaderException | UnsupportedCompressionException e) {
            e.printStackTrace();
        }
    }

    /**
     * Creates a sample tile with the given content.
     * In a real application, this would create actual MVT data.
     *
     * @param content the content to include in the tile
     * @return the tile data as a byte array
     */
    private static byte[] createSampleTileData(String content) {
        return content.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Example main method to demonstrate usage.
     *
     * @param args command line arguments
     */
    public static void main(String[] args) {
        try {
            Path outputPath = Path.of("example.pmtiles");
            createExamplePMTiles(outputPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
