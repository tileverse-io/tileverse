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

import io.tileverse.rangereader.RangeReader;
import io.tileverse.rangereader.RangeReaderFactory;
import io.tileverse.rangereader.file.FileRangeReader;
import io.tileverse.rangereader.http.HttpRangeReader;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Optional;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;

/**
 * Example usage of PMTilesReader with various data sources, including cloud storage.
 * <p>
 * This class demonstrates how to read PMTiles files from different sources:
 * - Local file system
 * - HTTP server
 * - AWS S3
 * - Azure Blob Storage
 * <p>
 * The examples show proper separation of concerns:
 * 1. Create the appropriate RangeReader for your data source
 * 2. Configure the RangeReader with authentication, caching, etc. if needed
 * 3. Pass the RangeReader to the PMTilesReader
 */
public class PMTilesReaderExample {

    /**
     * Example of reading from a local file.
     */
    public static void readFromLocalFile(Path path)
            throws IOException, InvalidHeaderException, UnsupportedCompressionException {
        // Create a FileRangeReader directly
        try (RangeReader reader = FileRangeReader.of(path);
                PMTilesReader pmtiles = new PMTilesReader(reader)) {

            // Get the header
            PMTilesHeader header = pmtiles.getHeader();
            System.out.println("Tile type: " + getTileTypeName(header.tileType()));
            System.out.println("Zoom levels: " + header.minZoom() + " to " + header.maxZoom());

            // Read a specific tile
            int z = 0;
            int x = 0;
            int y = 0;
            Optional<ByteBuffer> tileData = pmtiles.getTile(z, x, y);
            if (tileData.isPresent()) {
                System.out.println("Found tile at z/x/y: " + z + "/" + x + "/" + y);
                System.out.println("Tile size: " + tileData.get().remaining() + " bytes");
            } else {
                System.out.println("Tile not found at z/x/y: " + z + "/" + x + "/" + y);
            }
        }
    }

    /**
     * Example of reading from a HTTP source.
     */
    public static void readFromHttp(String url)
            throws IOException, InvalidHeaderException, UnsupportedCompressionException {
        // Create an HttpRangeReader
        URI uri = URI.create(url);
        try (RangeReader reader = HttpRangeReader.builder(uri).build();
                PMTilesReader pmtiles = new PMTilesReader(reader)) {

            // Get the header
            PMTilesHeader header = pmtiles.getHeader();
            System.out.println("Tile type: " + getTileTypeName(header.tileType()));
            System.out.println("Zoom levels: " + header.minZoom() + " to " + header.maxZoom());

            // Read a specific tile
            int z = 0;
            int x = 0;
            int y = 0;
            Optional<ByteBuffer> tileData = pmtiles.getTile(z, x, y);
            if (tileData.isPresent()) {
                System.out.println("Found tile at z/x/y: " + z + "/" + x + "/" + y);
                System.out.println("Tile size: " + tileData.get().remaining() + " bytes");
            } else {
                System.out.println("Tile not found at z/x/y: " + z + "/" + x + "/" + y);
            }
        }
    }

    /**
     * Example of reading from AWS S3 with caching enabled.
     */
    public static void readFromS3(String bucket, String key)
            throws IOException, InvalidHeaderException, UnsupportedCompressionException {
        // Create a S3 URI
        URI uri = URI.create("s3://" + bucket + "/" + key);

        // Create a basic S3 reader with default credentials
        RangeReader basicReader = RangeReaderFactory.create(uri);

        // Wrap with block-aligned caching for better performance with cloud storage
        RangeReader cachedReader = RangeReaderFactory.createBlockAlignedCaching(basicReader);

        // Use the enhanced reader with PMTilesReader
        try (PMTilesReader pmtiles = new PMTilesReader(cachedReader)) {
            // Get the header
            PMTilesHeader header = pmtiles.getHeader();
            System.out.println("Tile type: " + getTileTypeName(header.tileType()));
            System.out.println("Zoom levels: " + header.minZoom() + " to " + header.maxZoom());

            // Read a specific tile
            int z = 0;
            int x = 0;
            int y = 0;
            Optional<ByteBuffer> tileData = pmtiles.getTile(z, x, y);
            if (tileData.isPresent()) {
                System.out.println("Found tile at z/x/y: " + z + "/" + x + "/" + y);
                System.out.println("Tile size: " + tileData.get().remaining() + " bytes");
            } else {
                System.out.println("Tile not found at z/x/y: " + z + "/" + x + "/" + y);
            }
        }
    }

    /**
     * Example of reading from AWS S3 with custom credentials and region.
     */
    public static void readFromS3WithCustomAuth(String bucket, String key)
            throws IOException, InvalidHeaderException, UnsupportedCompressionException {
        // Create a custom S3 reader with specific region and credentials
        RangeReader reader = RangeReaderFactory.createS3RangeReader(
                URI.create("s3://" + bucket + "/" + key), DefaultCredentialsProvider.create(), Region.US_WEST_2);

        // Add block alignment and caching for better performance
        RangeReader optimizedReader = RangeReaderFactory.createBlockAlignedCaching(reader);

        try (PMTilesReader pmtiles = new PMTilesReader(optimizedReader)) {
            // Use the reader...
            System.out.println("Zoom levels: " + pmtiles.getHeader().minZoom() + " to "
                    + pmtiles.getHeader().maxZoom());
        }
    }

    /**
     * Example of reading from Azure Blob Storage with caching and custom block size.
     */
    public static void readFromAzure(String account, String container, String blob)
            throws IOException, InvalidHeaderException, UnsupportedCompressionException {
        // Create an Azure blob URI
        URI uri = URI.create("azure://" + account + ".blob.core.windows.net/" + container + "/" + blob);

        // Create a basic Azure reader
        RangeReader basicReader = RangeReaderFactory.create(uri);

        // Wrap with block-aligned caching with custom block size (16KB)
        int blockSize = 16384; // 16KB blocks
        RangeReader optimizedReader = RangeReaderFactory.createBlockAlignedCaching(basicReader, blockSize);

        // Use the enhanced reader with PMTilesReader
        try (PMTilesReader pmtiles = new PMTilesReader(optimizedReader)) {
            // Get the header
            PMTilesHeader header = pmtiles.getHeader();
            System.out.println("Tile type: " + getTileTypeName(header.tileType()));
            System.out.println("Zoom levels: " + header.minZoom() + " to " + header.maxZoom());

            // Read a specific tile
            int z = 0;
            int x = 0;
            int y = 0;
            Optional<ByteBuffer> tileData = pmtiles.getTile(z, x, y);
            if (tileData.isPresent()) {
                System.out.println("Found tile at z/x/y: " + z + "/" + x + "/" + y);
                System.out.println("Tile size: " + tileData.get().remaining() + " bytes");
            } else {
                System.out.println("Tile not found at z/x/y: " + z + "/" + x + "/" + y);
            }
        }
    }

    /**
     * Example of reading from Azure Blob Storage with connection string.
     */
    public static void readFromAzureWithConnectionString(String connectionString, String container, String blob)
            throws IOException, InvalidHeaderException, UnsupportedCompressionException {
        // Create an Azure reader with connection string
        RangeReader reader = RangeReaderFactory.createAzureBlobRangeReader(connectionString, container, blob);

        // Add caching for better performance
        RangeReader cachedReader = RangeReaderFactory.createBlockAlignedCaching(reader);

        try (PMTilesReader pmtiles = new PMTilesReader(cachedReader)) {
            // Use the reader...
            System.out.println("Zoom levels: " + pmtiles.getHeader().minZoom() + " to "
                    + pmtiles.getHeader().maxZoom());
        }
    }

    /**
     * Convert tile type byte to a human-readable name.
     */
    private static String getTileTypeName(byte tileType) {
        return switch (tileType) {
            case PMTilesHeader.TILETYPE_MVT -> "MVT (Vector)";
            case PMTilesHeader.TILETYPE_PNG -> "PNG";
            case PMTilesHeader.TILETYPE_JPEG -> "JPEG";
            case PMTilesHeader.TILETYPE_WEBP -> "WebP";
            default -> "Unknown";
        };
    }

    /**
     * Example usage.
     */
    public static void main(String[] args) {
        try {
            // Example for local file (uncomment and provide an actual path)
            // readFromLocalFile(Path.of("/path/to/your/tiles.pmtiles"));

            // Example for HTTP (uncomment and provide an actual URL)
            // readFromHttp("https://example.com/tiles.pmtiles");

            // Example for AWS S3 (uncomment and provide an actual bucket and key)
            // readFromS3("example-bucket", "path/to/tiles.pmtiles");

            // Example for S3 with custom auth
            // readFromS3WithCustomAuth("example-bucket", "path/to/tiles.pmtiles");

            // Example for Azure Blob Storage (uncomment and provide actual values)
            // readFromAzure("accountname", "container", "tiles.pmtiles");

            // Example for Azure with connection string
            // readFromAzureWithConnectionString("DefaultEndpointsProtocol=https;AccountName=mystorageaccount;AccountKey=accountkey==",
            //                                  "container", "tiles.pmtiles");

            System.out.println(
                    "To use these examples, uncomment the appropriate method calls and provide valid paths/URLs.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
