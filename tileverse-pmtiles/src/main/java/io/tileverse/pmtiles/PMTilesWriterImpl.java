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
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Implementation of the PMTilesWriter interface.
 * <p>
 * This class is responsible for creating PMTiles files by collecting tiles,
 * optimizing storage with deduplication and run-length encoding, and
 * writing the file in the PMTiles format.
 */
class PMTilesWriterImpl implements PMTilesWriter {
    // Constants
    private static final int MAX_ROOT_DIR_SIZE = 16384; // 16KB

    // Configuration
    private final Path outputPath;
    private final byte minZoom;
    private final byte maxZoom;
    private final byte tileCompression;
    private final byte internalCompression;
    private final byte tileType;
    private final int minLonE7;
    private final int minLatE7;
    private final int maxLonE7;
    private final int maxLatE7;
    private final byte centerZoom;
    private final int centerLonE7;
    private final int centerLatE7;

    // State
    private final TileRegistry tileRegistry;
    private byte[] compressedMetadata;
    private ProgressListener progressListener;
    private boolean completed;
    private boolean closed;

    /**
     * Creates a new PMTilesWriterImpl with the specified configuration.
     *
     * @param outputPath the path where the PMTiles file will be written
     * @param minZoom the minimum zoom level
     * @param maxZoom the maximum zoom level
     * @param tileCompression the compression type for tile data
     * @param internalCompression the compression type for internal structures
     * @param tileType the tile type
     * @param minLonE7 the minimum longitude in E7 format
     * @param minLatE7 the minimum latitude in E7 format
     * @param maxLonE7 the maximum longitude in E7 format
     * @param maxLatE7 the maximum latitude in E7 format
     * @param centerZoom the center zoom level
     * @param centerLonE7 the center longitude in E7 format
     * @param centerLatE7 the center latitude in E7 format
     * @throws IOException if an I/O error occurs
     */
    private PMTilesWriterImpl(
            Path outputPath,
            byte minZoom,
            byte maxZoom,
            byte tileCompression,
            byte internalCompression,
            byte tileType,
            int minLonE7,
            int minLatE7,
            int maxLonE7,
            int maxLatE7,
            byte centerZoom,
            int centerLonE7,
            int centerLatE7)
            throws IOException {

        this.outputPath = Objects.requireNonNull(outputPath, "Output path cannot be null");
        this.minZoom = minZoom;
        this.maxZoom = maxZoom;
        this.tileCompression = tileCompression;
        this.internalCompression = internalCompression;
        this.tileType = tileType;
        this.minLonE7 = minLonE7;
        this.minLatE7 = minLatE7;
        this.maxLonE7 = maxLonE7;
        this.maxLatE7 = maxLatE7;
        this.centerZoom = centerZoom;
        this.centerLonE7 = centerLonE7;
        this.centerLatE7 = centerLatE7;

        this.tileRegistry = new TileRegistry();

        // Create default metadata
        setMetadata("{}");

        // Create parent directories if they don't exist
        Files.createDirectories(outputPath.getParent());
    }

    @Override
    public void addTile(byte z, int x, int y, byte[] data) throws IOException {
        addTile(new ZXY(z, x, y), data);
    }

    @Override
    public void addTile(ZXY zxy, byte[] data) throws IOException {
        checkNotCompletedOrClosed();
        Objects.requireNonNull(zxy, "Tile coordinates cannot be null");
        Objects.requireNonNull(data, "Tile data cannot be null");

        // Compress the tile data if needed
        byte[] processedData = data;
        if (tileCompression != PMTilesHeader.COMPRESSION_NONE) {
            try {
                processedData = CompressionUtil.compress(data, tileCompression);
            } catch (UnsupportedCompressionException e) {
                throw new IOException("Failed to compress tile data", e);
            }
        }

        // Add to registry
        tileRegistry.addTile(zxy, processedData);
    }

    @Override
    public void setMetadata(String metadata) throws IOException {
        checkNotCompletedOrClosed();
        Objects.requireNonNull(metadata, "Metadata cannot be null");

        // Convert to bytes and compress
        byte[] metadataBytes = metadata.getBytes(StandardCharsets.UTF_8);
        try {
            this.compressedMetadata = CompressionUtil.compress(metadataBytes, internalCompression);
        } catch (UnsupportedCompressionException e) {
            throw new IOException("Failed to compress metadata", e);
        }
    }

    @Override
    public void complete() throws IOException {
        checkNotCompletedOrClosed();

        if (tileRegistry.isEmpty()) {
            throw new IllegalStateException("No tiles added to the PMTiles file");
        }

        // Process tiles and write file
        try {
            writePMTilesFile();
            completed = true;
        } catch (UnsupportedCompressionException e) {
            throw new IOException("Failed to compress data", e);
        }
    }

    @Override
    public void setProgressListener(ProgressListener listener) {
        this.progressListener = listener;
    }

    @Override
    public void close() throws IOException {
        closed = true;
    }

    /**
     * Writes the PMTiles file to disk.
     *
     * @throws IOException if an I/O error occurs
     * @throws UnsupportedCompressionException if the compression type is not supported
     */
    private void writePMTilesFile() throws IOException, UnsupportedCompressionException {
        // Report progress
        reportProgress(0.0);

        // Get optimized entries
        List<PMTilesEntry> entries = tileRegistry.getOptimizedEntries();
        reportProgress(0.1);

        // Build the directory structure
        DirectoryUtil.DirectoryResult directoryResult =
                DirectoryUtil.buildRootLeaves(entries, internalCompression, MAX_ROOT_DIR_SIZE);
        reportProgress(0.2);

        // Get the unique tile data
        List<TileContent> tileContents = tileRegistry.getUniqueContents();
        reportProgress(0.3);

        // Calculate the file layout
        FileLayout layout = calculateLayout(directoryResult, compressedMetadata, tileContents);
        reportProgress(0.4);

        // Create the header
        PMTilesHeader header = createHeader(layout, directoryResult, tileContents.size(), entries.size());
        reportProgress(0.5);

        // Write file
        try (FileChannel channel = FileChannel.open(
                outputPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

            // Write header
            byte[] headerBytes = header.serialize();
            ByteBuffer headerBuffer = ByteBuffer.wrap(headerBytes);
            channel.write(headerBuffer);
            reportProgress(0.6);

            // Write root directory
            ByteBuffer rootDirBuffer = ByteBuffer.wrap(directoryResult.rootDirectory());
            channel.write(rootDirBuffer);
            reportProgress(0.65);

            // Write metadata
            ByteBuffer metadataBuffer = ByteBuffer.wrap(compressedMetadata);
            channel.write(metadataBuffer);
            reportProgress(0.7);

            // Write leaf directories
            ByteBuffer leafDirsBuffer = ByteBuffer.wrap(directoryResult.leafDirectories());
            channel.write(leafDirsBuffer);
            reportProgress(0.75);

            // Write tile data
            writeTileData(channel, tileContents);
            reportProgress(1.0);
        }
    }

    /**
     * Calculates the layout of the PMTiles file.
     *
     * @param directoryResult the result of building the directory structure
     * @param metadata the compressed metadata
     * @param tileContents the unique tile contents
     * @return the file layout
     */
    private FileLayout calculateLayout(
            DirectoryUtil.DirectoryResult directoryResult, byte[] metadata, List<TileContent> tileContents) {

        long rootDirOffset = 127; // Start after header
        long rootDirBytes = directoryResult.rootDirectory().length;

        long metadataOffset = rootDirOffset + rootDirBytes;
        long metadataBytes = metadata.length;

        long leafDirsOffset = metadataOffset + metadataBytes;
        long leafDirsBytes = directoryResult.leafDirectories().length;

        long tileDataOffset = leafDirsOffset + leafDirsBytes;
        long tileDataBytes = calculateTotalTileSize(tileContents);

        return new FileLayout(
                rootDirOffset, rootDirBytes,
                metadataOffset, metadataBytes,
                leafDirsOffset, leafDirsBytes,
                tileDataOffset, tileDataBytes);
    }

    /**
     * Calculates the total size of all tile data.
     *
     * @param tileContents the unique tile contents
     * @return the total size in bytes
     */
    private long calculateTotalTileSize(List<TileContent> tileContents) {
        return tileContents.stream().mapToLong(content -> content.data.length).sum();
    }

    /**
     * Creates the PMTiles header based on the file layout and configuration.
     *
     * @param layout the file layout
     * @param directoryResult the result of building the directory structure
     * @param uniqueTileCount the number of unique tile contents
     * @param entryCount the number of directory entries
     * @return the PMTiles header
     */
    private PMTilesHeader createHeader(
            FileLayout layout, DirectoryUtil.DirectoryResult directoryResult, int uniqueTileCount, int entryCount) {

        return PMTilesHeader.builder()
                .rootDirOffset(layout.rootDirOffset)
                .rootDirBytes(layout.rootDirBytes)
                .jsonMetadataOffset(layout.metadataOffset)
                .jsonMetadataBytes(layout.metadataBytes)
                .leafDirsOffset(layout.leafDirsOffset)
                .leafDirsBytes(layout.leafDirsBytes)
                .tileDataOffset(layout.tileDataOffset)
                .tileDataBytes(layout.tileDataBytes)
                .addressedTilesCount(tileRegistry.getTileCount())
                .tileEntriesCount(entryCount)
                .tileContentsCount(uniqueTileCount)
                .clustered(true)
                .internalCompression(internalCompression)
                .tileCompression(tileCompression)
                .tileType(tileType)
                .minZoom(minZoom)
                .maxZoom(maxZoom)
                .minLonE7(minLonE7)
                .minLatE7(minLatE7)
                .maxLonE7(maxLonE7)
                .maxLatE7(maxLatE7)
                .centerZoom(centerZoom)
                .centerLonE7(centerLonE7)
                .centerLatE7(centerLatE7)
                .build();
    }

    /**
     * Writes tile data to the file.
     *
     * @param channel the file channel to write to
     * @param tileContents the tile contents to write
     * @throws IOException if an I/O error occurs
     */
    private void writeTileData(FileChannel channel, List<TileContent> tileContents) throws IOException {
        long totalBytes = calculateTotalTileSize(tileContents);
        long writtenBytes = 0;
        double baseProgress = 0.75;
        double progressWeight = 0.25;

        // Sort tile contents by offset to ensure correct order
        Collections.sort(tileContents, Comparator.comparingLong(content -> content.offset));

        // Write each tile content
        for (TileContent content : tileContents) {
            ByteBuffer buffer = ByteBuffer.wrap(content.data);
            channel.write(buffer);

            writtenBytes += content.data.length;
            double progress = baseProgress + (writtenBytes / (double) totalBytes) * progressWeight;
            reportProgress(progress);

            // Check for cancellation
            if (isCancelled()) {
                throw new IOException("Operation cancelled by user");
            }
        }
    }

    /**
     * Reports progress to the listener, if set.
     *
     * @param progress the progress as a value between 0.0 and 1.0
     */
    private void reportProgress(double progress) {
        if (progressListener != null) {
            progressListener.onProgress(progress);
        }
    }

    /**
     * Checks if the operation has been cancelled by the progress listener.
     *
     * @return true if cancelled, false otherwise
     */
    private boolean isCancelled() {
        return progressListener != null && progressListener.isCancelled();
    }

    /**
     * Checks that the writer has not been completed or closed.
     *
     * @throws IllegalStateException if the writer has been completed or closed
     */
    private void checkNotCompletedOrClosed() {
        if (completed) {
            throw new IllegalStateException("PMTilesWriter has already been completed");
        }
        if (closed) {
            throw new IllegalStateException("PMTilesWriter has been closed");
        }
    }

    /**
     * Builder implementation for PMTilesWriter.
     */
    static class BuilderImpl implements Builder {
        private Path outputPath;
        private byte minZoom = 0;
        private byte maxZoom = 0;
        private byte tileCompression = PMTilesHeader.COMPRESSION_GZIP;
        private byte internalCompression = PMTilesHeader.COMPRESSION_GZIP;
        private byte tileType = PMTilesHeader.TILETYPE_MVT;
        private int minLonE7 = -1800000000;
        private int minLatE7 = -850000000;
        private int maxLonE7 = 1800000000;
        private int maxLatE7 = 850000000;
        private byte centerZoom = 0;
        private int centerLonE7 = 0;
        private int centerLatE7 = 0;

        @Override
        public Builder outputPath(Path path) {
            this.outputPath = path;
            return this;
        }

        @Override
        public Builder minZoom(byte minZoom) {
            this.minZoom = minZoom;
            return this;
        }

        @Override
        public Builder maxZoom(byte maxZoom) {
            this.maxZoom = maxZoom;
            return this;
        }

        @Override
        public Builder tileCompression(byte compressionType) {
            this.tileCompression = compressionType;
            return this;
        }

        @Override
        public Builder internalCompression(byte compressionType) {
            this.internalCompression = compressionType;
            return this;
        }

        @Override
        public Builder tileType(byte tileType) {
            this.tileType = tileType;
            return this;
        }

        @Override
        public Builder bounds(double minLon, double minLat, double maxLon, double maxLat) {
            this.minLonE7 = (int) (minLon * 10000000);
            this.minLatE7 = (int) (minLat * 10000000);
            this.maxLonE7 = (int) (maxLon * 10000000);
            this.maxLatE7 = (int) (maxLat * 10000000);
            return this;
        }

        @Override
        public Builder center(double lon, double lat, byte zoom) {
            this.centerLonE7 = (int) (lon * 10000000);
            this.centerLatE7 = (int) (lat * 10000000);
            this.centerZoom = zoom;
            return this;
        }

        @Override
        public PMTilesWriter build() throws IOException {
            if (outputPath == null) {
                throw new IllegalArgumentException("Output path must be specified");
            }

            return new PMTilesWriterImpl(
                    outputPath,
                    minZoom,
                    maxZoom,
                    tileCompression,
                    internalCompression,
                    tileType,
                    minLonE7,
                    minLatE7,
                    maxLonE7,
                    maxLatE7,
                    centerZoom,
                    centerLonE7,
                    centerLatE7);
        }
    }

    /**
     * Registry for tracking tiles and optimizing storage.
     */
    private static class TileRegistry {
        // Map of tile ID to content hash
        private final Map<Long, String> tileIdToHash = new TreeMap<>();

        // Map of content hash to content data and offset
        private final Map<String, TileContent> contentMap = new HashMap<>();

        /**
         * Adds a tile to the registry.
         *
         * @param zxy the tile coordinates
         * @param data the tile data
         */
        public void addTile(ZXY zxy, byte[] data) {
            long tileId = zxy.toTileId();
            String hash = computeHash(data);

            tileIdToHash.put(tileId, hash);

            // Only store the data if we haven't seen this content before
            if (!contentMap.containsKey(hash)) {
                contentMap.put(hash, new TileContent(hash, data, 0));
            }
        }

        /**
         * Gets the total number of tiles in the registry.
         *
         * @return the number of tiles
         */
        public long getTileCount() {
            return tileIdToHash.size();
        }

        /**
         * Checks if the registry is empty.
         *
         * @return true if empty, false otherwise
         */
        public boolean isEmpty() {
            return tileIdToHash.isEmpty();
        }

        /**
         * Gets the optimized directory entries with run-length encoding.
         *
         * @return the list of optimized entries
         */
        public List<PMTilesEntry> getOptimizedEntries() {
            List<PMTilesEntry> entries = new ArrayList<>();

            // Assign offsets to unique contents
            long offset = 0;
            for (TileContent content : getUniqueContents()) {
                content.offset = offset;
                offset += content.data.length;
            }

            if (tileIdToHash.isEmpty()) {
                return entries;
            }

            // Create entries with run-length encoding
            long runStart = -1;
            String runHash = null;
            long runLength = 0;

            for (Map.Entry<Long, String> entry : tileIdToHash.entrySet()) {
                long tileId = entry.getKey();
                String hash = entry.getValue();
                TileContent content = contentMap.get(hash);

                if (runStart == -1) {
                    // Start a new run
                    runStart = tileId;
                    runHash = hash;
                    runLength = 1;
                } else if (tileId == runStart + runLength && hash.equals(runHash)) {
                    // Extend the current run
                    runLength++;
                } else {
                    // End the current run and start a new one
                    TileContent runContent = contentMap.get(runHash);
                    entries.add(new PMTilesEntry(runStart, runContent.offset, runContent.data.length, (int) runLength));

                    runStart = tileId;
                    runHash = hash;
                    runLength = 1;
                }
            }

            // Add the last run
            if (runStart != -1) {
                TileContent runContent = contentMap.get(runHash);
                entries.add(new PMTilesEntry(runStart, runContent.offset, runContent.data.length, (int) runLength));
            }

            return entries;
        }

        /**
         * Gets all unique tile contents, sorted by offset.
         *
         * @return the list of unique tile contents
         */
        public List<TileContent> getUniqueContents() {
            List<TileContent> result = new ArrayList<>(contentMap.values());
            Collections.sort(result, Comparator.comparingLong(content -> content.offset));
            return result;
        }

        /**
         * Computes a hash of the tile data for deduplication.
         *
         * @param data the tile data
         * @return a hash of the data
         */
        private String computeHash(byte[] data) {
            try {
                MessageDigest digest = MessageDigest.getInstance("SHA-256");
                byte[] hash = digest.digest(data);
                return bytesToHex(hash);
            } catch (NoSuchAlgorithmException e) {
                // Fallback to a simpler hashing algorithm
                return String.valueOf(data.length) + "_" + Arrays.hashCode(data);
            }
        }

        /**
         * Converts a byte array to a hexadecimal string.
         *
         * @param bytes the byte array
         * @return the hexadecimal string
         */
        private String bytesToHex(byte[] bytes) {
            StringBuilder sb = new StringBuilder();
            for (byte b : bytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        }
    }

    /**
     * Represents tile content with its hash, data, and offset.
     */
    private static class TileContent {
        private final String hash;
        private final byte[] data;
        private long offset;

        /**
         * Creates a new TileContent.
         *
         * @param hash the content hash
         * @param data the tile data
         * @param offset the offset in the file
         */
        public TileContent(String hash, byte[] data, long offset) {
            this.hash = hash;
            this.data = data;
            this.offset = offset;
        }
    }

    /**
     * Represents the layout of the PMTiles file.
     */
    private record FileLayout(
            long rootDirOffset,
            long rootDirBytes,
            long metadataOffset,
            long metadataBytes,
            long leafDirsOffset,
            long leafDirsBytes,
            long tileDataOffset,
            long tileDataBytes) {}
}
