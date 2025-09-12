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
 * The header is a fixed-length structure of 127 bytes containing metadata and offsets.
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
        long rootDirBytes,
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

    private void writeInt64(ByteArrayOutputStream out, ByteBuffer buffer, long value) throws IOException {
        buffer.clear();
        buffer.putLong(value);
        out.write(buffer.array(), 0, 8);
    }

    private void writeInt32(ByteArrayOutputStream out, ByteBuffer buffer, int value) throws IOException {
        buffer.clear();
        buffer.putInt(value);
        out.write(buffer.array(), 0, 4);
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
                dup.getLong(), // rootDirBytes
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
     */
    public static class Builder {
        private long rootDirOffset = 127; // Default starts after header
        private long rootDirBytes = 0;
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

        // Builder methods for all fields
        public Builder rootDirOffset(long rootDirOffset) {
            this.rootDirOffset = rootDirOffset;
            return this;
        }

        public Builder rootDirBytes(long rootDirBytes) {
            this.rootDirBytes = rootDirBytes;
            return this;
        }

        public Builder jsonMetadataOffset(long jsonMetadataOffset) {
            this.jsonMetadataOffset = jsonMetadataOffset;
            return this;
        }

        public Builder jsonMetadataBytes(long jsonMetadataBytes) {
            this.jsonMetadataBytes = jsonMetadataBytes;
            return this;
        }

        public Builder leafDirsOffset(long leafDirsOffset) {
            this.leafDirsOffset = leafDirsOffset;
            return this;
        }

        public Builder leafDirsBytes(long leafDirsBytes) {
            this.leafDirsBytes = leafDirsBytes;
            return this;
        }

        public Builder tileDataOffset(long tileDataOffset) {
            this.tileDataOffset = tileDataOffset;
            return this;
        }

        public Builder tileDataBytes(long tileDataBytes) {
            this.tileDataBytes = tileDataBytes;
            return this;
        }

        public Builder addressedTilesCount(long addressedTilesCount) {
            this.addressedTilesCount = addressedTilesCount;
            return this;
        }

        public Builder tileEntriesCount(long tileEntriesCount) {
            this.tileEntriesCount = tileEntriesCount;
            return this;
        }

        public Builder tileContentsCount(long tileContentsCount) {
            this.tileContentsCount = tileContentsCount;
            return this;
        }

        public Builder clustered(boolean clustered) {
            this.clustered = clustered;
            return this;
        }

        public Builder internalCompression(byte internalCompression) {
            this.internalCompression = internalCompression;
            return this;
        }

        public Builder tileCompression(byte tileCompression) {
            this.tileCompression = tileCompression;
            return this;
        }

        public Builder tileType(byte tileType) {
            this.tileType = tileType;
            return this;
        }

        public Builder minZoom(byte minZoom) {
            this.minZoom = minZoom;
            return this;
        }

        public Builder maxZoom(byte maxZoom) {
            this.maxZoom = maxZoom;
            return this;
        }

        public Builder minLonE7(int minLonE7) {
            this.minLonE7 = minLonE7;
            return this;
        }

        public Builder minLatE7(int minLatE7) {
            this.minLatE7 = minLatE7;
            return this;
        }

        public Builder maxLonE7(int maxLonE7) {
            this.maxLonE7 = maxLonE7;
            return this;
        }

        public Builder maxLatE7(int maxLatE7) {
            this.maxLatE7 = maxLatE7;
            return this;
        }

        public Builder centerZoom(byte centerZoom) {
            this.centerZoom = centerZoom;
            return this;
        }

        public Builder centerLonE7(int centerLonE7) {
            this.centerLonE7 = centerLonE7;
            return this;
        }

        public Builder centerLatE7(int centerLatE7) {
            this.centerLatE7 = centerLatE7;
            return this;
        }

        // Convenience methods for setting floating-point coordinates
        public Builder minLon(double minLon) {
            this.minLonE7 = (int) (minLon * 10000000);
            return this;
        }

        public Builder minLat(double minLat) {
            this.minLatE7 = (int) (minLat * 10000000);
            return this;
        }

        public Builder maxLon(double maxLon) {
            this.maxLonE7 = (int) (maxLon * 10000000);
            return this;
        }

        public Builder maxLat(double maxLat) {
            this.maxLatE7 = (int) (maxLat * 10000000);
            return this;
        }

        public Builder centerLon(double centerLon) {
            this.centerLonE7 = (int) (centerLon * 10000000);
            return this;
        }

        public Builder centerLat(double centerLat) {
            this.centerLatE7 = (int) (centerLat * 10000000);
            return this;
        }

        // Build the final header
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

    static PMTilesHeader readHeader(ReadableByteChannel channel) throws IOException, InvalidHeaderException {
        // Read the header
        ByteBuffer headerBuffer = ByteBufferPool.getDefault().borrowHeap(127);
        try {
            int bytesRead = channel.read(headerBuffer);
            if (bytesRead != 127) {
                throw new InvalidHeaderException("Failed to read complete header. Read " + bytesRead + " bytes");
            }
            headerBuffer.flip();
            // Deserialize the header directly from the ByteBuffer
            return deserialize(headerBuffer);
        } finally {
            ByteBufferPool.getDefault().returnBuffer(headerBuffer);
        }
    }
}
