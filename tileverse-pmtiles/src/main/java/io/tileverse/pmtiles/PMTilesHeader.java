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

import io.tileverse.io.ByteBufferPool;
import io.tileverse.io.ByteRange;
import io.tileverse.tiling.common.BoundingBox2D;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Represents the header of a PMTiles file, based on version 3 of the PMTiles specification.
 * <p>
 * The header is a fixed-length structure of 127 bytes containing metadata and offsets.
 * It provides the layout of the file, including the location of the root directory,
 * metadata, leaf directories, and tile data. It also includes global metadata such as
 * the coordinate bounds, zoom levels, and compression types used.
 * </p>
 *
 * @param rootDirOffset the byte offset from the start of the archive to the first byte of the root directory
 * @param rootDirBytes the number of bytes in the root directory
 * @param jsonMetadataOffset the byte offset from the start of the archive to the first byte of the JSON metadata
 * @param jsonMetadataBytes the number of bytes of JSON metadata
 * @param leafDirsOffset the byte offset from the start of the archive to the first byte of leaf directories
 * @param leafDirsBytes the total number of bytes of leaf directories
 * @param tileDataOffset the byte offset from the start of the archive to the first byte of tile data
 * @param tileDataBytes the total number of bytes of tile data
 * @param addressedTilesCount the total number of tiles before Run Length Encoding, or 0 if unknown
 * @param tileEntriesCount the total number of tile entries where RunLength > 0, or 0 if unknown
 * @param tileContentsCount the total number of blobs in the tile data section, or 0 if unknown
 * @param clustered whether the tiles in the tile data section are ordered by TileID
 * @param internalCompression the compression type used for directories and metadata (see COMPRESSION_* constants)
 * @param tileCompression the compression type used for individual tiles (see COMPRESSION_* constants)
 * @param tileType the type of tiles stored in this archive (see TILETYPE_* constants)
 * @param minZoom the minimum zoom level of tiles in this archive (0-30)
 * @param maxZoom the maximum zoom level of tiles in this archive (0-30, must be >= minZoom)
 * @param minLonE7 the minimum longitude of the bounding box in E7 format (longitude * 10,000,000)
 * @param minLatE7 the minimum latitude of the bounding box in E7 format (latitude * 10,000,000)
 * @param maxLonE7 the maximum longitude of the bounding box in E7 format (longitude * 10,000,000)
 * @param maxLatE7 the maximum latitude of the bounding box in E7 format (latitude * 10,000,000)
 * @param centerZoom the initial recommended zoom level for displaying the tileset
 * @param centerLonE7 the center longitude for displaying the tileset in E7 format (longitude * 10,000,000)
 * @param centerLatE7 the center latitude for displaying the tileset in E7 format (latitude * 10,000,000)
 */
public record PMTilesHeader(
        long rootDirOffset,
        int rootDirBytes,
        long jsonMetadataOffset,
        long jsonMetadataBytes,
        long leafDirsOffset,
        long leafDirsBytes,
        long tileDataOffset,
        long tileDataBytes,
        long addressedTilesCount,
        long tileEntriesCount,
        long tileContentsCount,
        boolean clustered,
        byte internalCompression,
        byte tileCompression,
        byte tileType,
        byte minZoom,
        byte maxZoom,
        int minLonE7,
        int minLatE7,
        int maxLonE7,
        int maxLatE7,
        byte centerZoom,
        int centerLonE7,
        int centerLatE7) {
    // Constants for compression types
    public static final byte COMPRESSION_UNKNOWN = 0x0;
    public static final byte COMPRESSION_NONE = 0x1;
    public static final byte COMPRESSION_GZIP = 0x2;
    public static final byte COMPRESSION_BROTLI = 0x3;
    public static final byte COMPRESSION_ZSTD = 0x4;

    // Constants for tile types
    public static final byte TILETYPE_UNKNOWN = 0x0;
    public static final byte TILETYPE_MVT = 0x1;
    public static final byte TILETYPE_PNG = 0x2;
    public static final byte TILETYPE_JPEG = 0x3;
    public static final byte TILETYPE_WEBP = 0x4;

    // Header magic and version
    private static final byte[] MAGIC = "PMTiles".getBytes(StandardCharsets.UTF_8);
    public static final byte VERSION_3 = 3;
    private static final byte VERSION = VERSION_3;
    private static final int HEADER_SIZE = 127;

    /**
     * Returns the PMTiles format version.
     * @return The version number (always 3 for this implementation)
     */
    public byte version() {
        return VERSION;
    }

    public ByteRange rootDirectory() {
        return ByteRange.of(rootDirOffset(), rootDirBytes());
    }

    public ByteRange jsonMetadata() {
        final long offset = jsonMetadataOffset();
        final int length = (int) jsonMetadataBytes();
        return ByteRange.of(offset, length);
    }

    /**
     * Returns the minimum longitude as a double value.
     * @return The minimum longitude in decimal degrees
     */
    public double minLon() {
        return minLonE7 / 10_000_000.0;
    }

    /**
     * Returns the minimum latitude as a double value.
     * @return The minimum latitude in decimal degrees
     */
    public double minLat() {
        return minLatE7 / 10_000_000.0;
    }

    /**
     * Returns the maximum longitude as a double value.
     * @return The maximum longitude in decimal degrees
     */
    public double maxLon() {
        return maxLonE7 / 10_000_000.0;
    }

    /**
     * Returns the maximum latitude as a double value.
     * @return The maximum latitude in decimal degrees
     */
    public double maxLat() {
        return maxLatE7 / 10_000_000.0;
    }

    /**
     * Returns the center longitude as a double value.
     * @return The center longitude in decimal degrees
     */
    public double centerLon() {
        return centerLonE7 / 10_000_000.0;
    }

    /**
     * Returns the center latitude as a double value.
     * @return The center latitude in decimal degrees
     */
    public double centerLat() {
        return centerLatE7 / 10_000_000.0;
    }

    public BoundingBox2D geographicBoundingBox() {
        return BoundingBox2D.extent(
                minLon(), minLat(),
                maxLon(), maxLat());
    }

    /**
     * Serializes the header to a byte array.
     *
     * @return A byte array containing the serialized header.
     * @throws IOException If an I/O error occurs.
     */
    public byte[] serialize() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream(HEADER_SIZE);

        // Write magic and version
        out.write(MAGIC);
        out.write(VERSION);

        // Use ByteBuffer for writing numeric values in little-endian order
        ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);

        // Write offset and size fields
        writeInt64(out, buffer, rootDirOffset);
        writeInt64(out, buffer, rootDirBytes);
        writeInt64(out, buffer, jsonMetadataOffset);
        writeInt64(out, buffer, jsonMetadataBytes);
        writeInt64(out, buffer, leafDirsOffset);
        writeInt64(out, buffer, leafDirsBytes);
        writeInt64(out, buffer, tileDataOffset);
        writeInt64(out, buffer, tileDataBytes);
        writeInt64(out, buffer, addressedTilesCount);
        writeInt64(out, buffer, tileEntriesCount);
        writeInt64(out, buffer, tileContentsCount);

        // Write boolean and byte fields
        out.write(clustered ? 0x1 : 0x0);
        out.write(internalCompression);
        out.write(tileCompression);
        out.write(tileType);
        out.write(minZoom);
        out.write(maxZoom);

        // Write coordinate bounds and center
        writeInt32(out, buffer, minLonE7);
        writeInt32(out, buffer, minLatE7);
        writeInt32(out, buffer, maxLonE7);
        writeInt32(out, buffer, maxLatE7);
        out.write(centerZoom);
        writeInt32(out, buffer, centerLonE7);
        writeInt32(out, buffer, centerLatE7);

        return out.toByteArray();
    }

    private void writeInt64(ByteArrayOutputStream out, ByteBuffer buffer, long value) {
        buffer.clear();
        buffer.putLong(value);
        out.write(buffer.array(), 0, 8);
    }

    private void writeInt32(ByteArrayOutputStream out, ByteBuffer buffer, int value) {
        buffer.clear();
        buffer.putInt(value);
        out.write(buffer.array(), 0, 4);
    }

    public static PMTilesHeader deserialize(ReadableByteChannel channel) throws IOException {
        // Read the header
        try (var pooledBuffer = ByteBufferPool.heapBuffer(HEADER_SIZE)) {
            ByteBuffer buffer = pooledBuffer.buffer();
            int bytesRead = channel.read(buffer);
            if (bytesRead != 127) {
                throw new InvalidHeaderException("Failed to read complete header. Read " + bytesRead + " bytes");
            }
            buffer.flip();
            // Deserialize the header directly from the ByteBuffer
            return deserialize(buffer);
        }
    }

    /**
     * Deserializes a PMTiles header from a ByteBuffer.
     *
     * @param buffer The ByteBuffer containing the header data.
     * @return A new PMTilesHeader instance.
     * @throws InvalidHeaderException If the header is invalid.
     */
    public static PMTilesHeader deserialize(ByteBuffer buffer) throws InvalidHeaderException {
        if (buffer.remaining() != HEADER_SIZE) {
            throw new InvalidHeaderException("Header must be exactly " + HEADER_SIZE + " bytes");
        }

        // Save the original position
        int originalPosition = buffer.position();

        // Check magic
        byte[] magic = new byte[7];
        buffer.get(magic);
        if (!Arrays.equals(magic, MAGIC)) {
            throw new InvalidHeaderException("Invalid magic number");
        }

        // Check version
        byte version = buffer.get();
        if (version != VERSION) {
            throw new InvalidHeaderException("Unsupported version: " + version);
        }

        // Create a duplicate so we don't modify the original buffer
        ByteBuffer dup = buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        // Reset position to after magic and version
        dup.position(originalPosition + 8); // Skip magic and version

        return new PMTilesHeader(
                dup.getLong(), // rootDirOffset
                (int) dup.getLong(), // rootDirBytes
                dup.getLong(), // jsonMetadataOffset
                dup.getLong(), // jsonMetadataBytes
                dup.getLong(), // leafDirsOffset
                dup.getLong(), // leafDirsBytes
                dup.getLong(), // tileDataOffset
                dup.getLong(), // tileDataBytes
                dup.getLong(), // addressedTilesCount
                dup.getLong(), // tileEntriesCount
                dup.getLong(), // tileContentsCount
                dup.get() == 0x1, // clustered
                dup.get(), // internalCompression
                dup.get(), // tileCompression
                dup.get(), // tileType
                dup.get(), // minZoom
                dup.get(), // maxZoom
                dup.getInt(), // minLonE7
                dup.getInt(), // minLatE7
                dup.getInt(), // maxLonE7
                dup.getInt(), // maxLatE7
                dup.get(), // centerZoom
                dup.getInt(), // centerLonE7
                dup.getInt() // centerLatE7
                );
    }

    /**
     * Deserializes a PMTiles header from a byte array.
     * This is a convenience method that wraps the byte array in a ByteBuffer.
     *
     * @param bytes The byte array containing the header data.
     * @return A new PMTilesHeader instance.
     * @throws InvalidHeaderException If the header is invalid.
     */
    public static PMTilesHeader deserialize(byte[] bytes) throws InvalidHeaderException {
        if (bytes.length != HEADER_SIZE) {
            throw new InvalidHeaderException("Header must be exactly " + HEADER_SIZE + " bytes");
        }
        return deserialize(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN));
    }

    /**
     * Creates a builder for constructing a PMTilesHeader with default values.
     *
     * @return A new builder instance.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for PMTilesHeader instances.
     * <p>
     * Provides a fluent API for constructing PMTilesHeader objects.
     * Default values are set for typical use cases (MVT tiles, gzip compression, full world bounds).
     */
    public static class Builder {
        private long rootDirOffset = 127; // Default starts after header
        private int rootDirBytes = 0;
        private long jsonMetadataOffset = 0;
        private long jsonMetadataBytes = 0;
        private long leafDirsOffset = 0;
        private long leafDirsBytes = 0;
        private long tileDataOffset = 0;
        private long tileDataBytes = 0;
        private long addressedTilesCount = 0;
        private long tileEntriesCount = 0;
        private long tileContentsCount = 0;
        private boolean clustered = false;
        private byte internalCompression = COMPRESSION_GZIP;
        private byte tileCompression = COMPRESSION_GZIP;
        private byte tileType = TILETYPE_MVT;
        private byte minZoom = 0;
        private byte maxZoom = 0;
        private int minLonE7 = -1800000000;
        private int minLatE7 = -850000000;
        private int maxLonE7 = 1800000000;
        private int maxLatE7 = 850000000;
        private byte centerZoom = 0;
        private int centerLonE7 = 0;
        private int centerLatE7 = 0;

        /**
         * Sets the offset to the root directory.
         * @param rootDirOffset the offset in bytes
         * @return this builder
         */
        public Builder rootDirOffset(long rootDirOffset) {
            this.rootDirOffset = rootDirOffset;
            return this;
        }

        /**
         * Sets the size of the root directory.
         * @param rootDirBytes the size in bytes
         * @return this builder
         */
        public Builder rootDirBytes(long rootDirBytes) {
            this.rootDirBytes = (int) rootDirBytes;
            return this;
        }

        /**
         * Sets the offset to the JSON metadata.
         * @param jsonMetadataOffset the offset in bytes
         * @return this builder
         */
        public Builder jsonMetadataOffset(long jsonMetadataOffset) {
            this.jsonMetadataOffset = jsonMetadataOffset;
            return this;
        }

        /**
         * Sets the size of the JSON metadata.
         * @param jsonMetadataBytes the size in bytes
         * @return this builder
         */
        public Builder jsonMetadataBytes(long jsonMetadataBytes) {
            this.jsonMetadataBytes = jsonMetadataBytes;
            return this;
        }

        /**
         * Sets the offset to the leaf directories section.
         * @param leafDirsOffset the offset in bytes
         * @return this builder
         */
        public Builder leafDirsOffset(long leafDirsOffset) {
            this.leafDirsOffset = leafDirsOffset;
            return this;
        }

        /**
         * Sets the size of the leaf directories section.
         * @param leafDirsBytes the size in bytes
         * @return this builder
         */
        public Builder leafDirsBytes(long leafDirsBytes) {
            this.leafDirsBytes = leafDirsBytes;
            return this;
        }

        /**
         * Sets the offset to the tile data section.
         * @param tileDataOffset the offset in bytes
         * @return this builder
         */
        public Builder tileDataOffset(long tileDataOffset) {
            this.tileDataOffset = tileDataOffset;
            return this;
        }

        /**
         * Sets the size of the tile data section.
         * @param tileDataBytes the size in bytes
         * @return this builder
         */
        public Builder tileDataBytes(long tileDataBytes) {
            this.tileDataBytes = tileDataBytes;
            return this;
        }

        /**
         * Sets the total count of addressed tiles (before RLE).
         * @param addressedTilesCount the count
         * @return this builder
         */
        public Builder addressedTilesCount(long addressedTilesCount) {
            this.addressedTilesCount = addressedTilesCount;
            return this;
        }

        /**
         * Sets the total count of tile entries (where RunLength > 0).
         * @param tileEntriesCount the count
         * @return this builder
         */
        public Builder tileEntriesCount(long tileEntriesCount) {
            this.tileEntriesCount = tileEntriesCount;
            return this;
        }

        /**
         * Sets the total count of distinct tile contents.
         * @param tileContentsCount the count
         * @return this builder
         */
        public Builder tileContentsCount(long tileContentsCount) {
            this.tileContentsCount = tileContentsCount;
            return this;
        }

        /**
         * Sets whether the tiles are clustered by TileID.
         * @param clustered true if clustered
         * @return this builder
         */
        public Builder clustered(boolean clustered) {
            this.clustered = clustered;
            return this;
        }

        /**
         * Sets the compression type for internal structures.
         * @param internalCompression the compression type
         * @return this builder
         */
        public Builder internalCompression(byte internalCompression) {
            this.internalCompression = internalCompression;
            return this;
        }

        /**
         * Sets the compression type for tiles.
         * @param tileCompression the compression type
         * @return this builder
         */
        public Builder tileCompression(byte tileCompression) {
            this.tileCompression = tileCompression;
            return this;
        }

        /**
         * Sets the tile type.
         * @param tileType the tile type
         * @return this builder
         */
        public Builder tileType(byte tileType) {
            this.tileType = tileType;
            return this;
        }

        /**
         * Sets the minimum zoom level.
         * @param minZoom the zoom level
         * @return this builder
         */
        public Builder minZoom(byte minZoom) {
            this.minZoom = minZoom;
            return this;
        }

        /**
         * Sets the maximum zoom level.
         * @param maxZoom the zoom level
         * @return this builder
         */
        public Builder maxZoom(byte maxZoom) {
            this.maxZoom = maxZoom;
            return this;
        }

        /**
         * Sets the minimum longitude in E7 format.
         * @param minLonE7 the longitude
         * @return this builder
         */
        public Builder minLonE7(int minLonE7) {
            this.minLonE7 = minLonE7;
            return this;
        }

        /**
         * Sets the minimum latitude in E7 format.
         * @param minLatE7 the latitude
         * @return this builder
         */
        public Builder minLatE7(int minLatE7) {
            this.minLatE7 = minLatE7;
            return this;
        }

        /**
         * Sets the maximum longitude in E7 format.
         * @param maxLonE7 the longitude
         * @return this builder
         */
        public Builder maxLonE7(int maxLonE7) {
            this.maxLonE7 = maxLonE7;
            return this;
        }

        /**
         * Sets the maximum latitude in E7 format.
         * @param maxLatE7 the latitude
         * @return this builder
         */
        public Builder maxLatE7(int maxLatE7) {
            this.maxLatE7 = maxLatE7;
            return this;
        }

        /**
         * Sets the center zoom level.
         * @param centerZoom the zoom level
         * @return this builder
         */
        public Builder centerZoom(byte centerZoom) {
            this.centerZoom = centerZoom;
            return this;
        }

        /**
         * Sets the center longitude in E7 format.
         * @param centerLonE7 the longitude
         * @return this builder
         */
        public Builder centerLonE7(int centerLonE7) {
            this.centerLonE7 = centerLonE7;
            return this;
        }

        /**
         * Sets the center latitude in E7 format.
         * @param centerLatE7 the latitude
         * @return this builder
         */
        public Builder centerLatE7(int centerLatE7) {
            this.centerLatE7 = centerLatE7;
            return this;
        }

        /**
         * Convenience method for setting the minimum longitude.
         * @param minLon the longitude in decimal degrees
         * @return this builder
         */
        public Builder minLon(double minLon) {
            this.minLonE7 = (int) (minLon * 10000000);
            return this;
        }

        /**
         * Convenience method for setting the minimum latitude.
         * @param minLat the latitude in decimal degrees
         * @return this builder
         */
        public Builder minLat(double minLat) {
            this.minLatE7 = (int) (minLat * 10000000);
            return this;
        }

        /**
         * Convenience method for setting the maximum longitude.
         * @param maxLon the longitude in decimal degrees
         * @return this builder
         */
        public Builder maxLon(double maxLon) {
            this.maxLonE7 = (int) (maxLon * 10000000);
            return this;
        }

        /**
         * Convenience method for setting the maximum latitude.
         * @param maxLat the latitude in decimal degrees
         * @return this builder
         */
        public Builder maxLat(double maxLat) {
            this.maxLatE7 = (int) (maxLat * 10000000);
            return this;
        }

        /**
         * Convenience method for setting the center longitude.
         * @param centerLon the longitude in decimal degrees
         * @return this builder
         */
        public Builder centerLon(double centerLon) {
            this.centerLonE7 = (int) (centerLon * 10000000);
            return this;
        }

        /**
         * Convenience method for setting the center latitude.
         * @param centerLat the latitude in decimal degrees
         * @return this builder
         */
        public Builder centerLat(double centerLat) {
            this.centerLatE7 = (int) (centerLat * 10000000);
            return this;
        }

        /**
         * Builds the final header.
         * @return the constructed PMTilesHeader
         */
        public PMTilesHeader build() {
            return new PMTilesHeader(
                    rootDirOffset,
                    rootDirBytes,
                    jsonMetadataOffset,
                    jsonMetadataBytes,
                    leafDirsOffset,
                    leafDirsBytes,
                    tileDataOffset,
                    tileDataBytes,
                    addressedTilesCount,
                    tileEntriesCount,
                    tileContentsCount,
                    clustered,
                    internalCompression,
                    tileCompression,
                    tileType,
                    minZoom,
                    maxZoom,
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
     * @return The absolute position and length for a leaf directory entry
     */
    ByteRange leafDirDataRange(PMTilesEntry dirEntry) {
        long offset = leafDirsOffset() + dirEntry.offset();
        int length = dirEntry.length();
        return ByteRange.of(offset, length);
    }

    /**
     * @return The absolute position and length for a tile data
     */
    ByteRange tileDataRange(PMTilesEntry tileEntry) {
        final long offset = tileDataOffset() + tileEntry.offset();
        final int length = tileEntry.length();
        return ByteRange.of(offset, length);
    }
}
