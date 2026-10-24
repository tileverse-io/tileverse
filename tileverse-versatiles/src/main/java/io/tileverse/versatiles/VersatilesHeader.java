/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.versatiles;

import static java.util.Objects.requireNonNull;

import io.tileverse.geom.BoundingBox2D;
import io.tileverse.io.ByteRange;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;

/**
 * The header of a Versatiles 2.0 container: the first 66 bytes of the file.
 *
 * <p>Wire layout, with multi-byte fields in big-endian order:
 *
 * <pre>
 *  0  +14   ascii   magic ("versatiles_v02")
 * 14  +1    u8      tile_format
 * 15  +1    u8      precompression
 * 16  +1    u8      min_zoom
 * 17  +1    u8      max_zoom
 * 18  +4    i32     bbox_min_x  (lon * 1e7)
 * 22  +4    i32     bbox_min_y  (lat * 1e7)
 * 26  +4    i32     bbox_max_x  (lon * 1e7)
 * 30  +4    i32     bbox_max_y  (lat * 1e7)
 * 34  +8    u64     metadata_offset
 * 42  +8    u64     metadata_length
 * 50  +8    u64     block_index_offset
 * 58  +8    u64     block_index_length
 * </pre>
 *
 * @param tileFormat the format of the tile bodies
 * @param compression the compression of the metadata and of the tile bodies
 * @param minZoom the lowest zoom level of the archive
 * @param maxZoom the highest zoom level of the archive
 * @param minLon the western edge of the bounding box, in WGS84 degrees
 * @param minLat the southern edge of the bounding box, in WGS84 degrees
 * @param maxLon the eastern edge of the bounding box, in WGS84 degrees
 * @param maxLat the northern edge of the bounding box, in WGS84 degrees
 * @param metadataOffset the position of the metadata in the file
 * @param metadataLength the length of the metadata; zero when the archive has no metadata
 * @param blockIndexOffset the position of the block index in the file
 * @param blockIndexLength the length of the block index
 */
public record VersatilesHeader(
        TileFormat tileFormat,
        Compression compression,
        int minZoom,
        int maxZoom,
        double minLon,
        double minLat,
        double maxLon,
        double maxLat,
        long metadataOffset,
        long metadataLength,
        long blockIndexOffset,
        long blockIndexLength) {

    /** Length of the header in bytes. */
    public static final int HEADER_BYTES = 66;

    /** ASCII magic at the start of a Versatiles 2 container. */
    public static final String MAGIC = "versatiles_v02";

    private static final double E7 = 1.0e7;

    private static final BoundingBox2D WORLD = BoundingBox2D.extent(-180, -90, 180, 90);

    /**
     * Validates the header fields. The reader loads the metadata into one buffer, and the block index into another. A
     * Java array limits a buffer to 2 GiB.
     *
     * @throws IllegalArgumentException if the zoom range is reversed, or if an offset or a length is out of range
     */
    public VersatilesHeader {
        requireNonNull(tileFormat, "tileFormat");
        requireNonNull(compression, "compression");
        if (minZoom < 0 || minZoom > maxZoom) {
            throw new IllegalArgumentException("Invalid zoom range %d to %d".formatted(minZoom, maxZoom));
        }
        requireReadableSection("metadata", metadataOffset, metadataLength);
        requireReadableSection("block index", blockIndexOffset, blockIndexLength);
    }

    private static void requireReadableSection(String name, long offset, long length) {
        if (offset < 0 || length < 0 || length > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Invalid %s section: offset %s, length %s"
                    .formatted(name, Long.toUnsignedString(offset), Long.toUnsignedString(length)));
        }
    }

    /**
     * Reads a header from the start of {@code channel}.
     *
     * @throws InvalidHeaderException if the channel ends before the header does, if the magic does not match, or if a
     *     field holds a value outside the specification
     */
    public static VersatilesHeader deserialize(ReadableByteChannel channel) throws IOException {
        ByteBuffer buffer = readHeaderBytes(channel);
        requireMagic(buffer);
        try {
            return parseFields(buffer);
        } catch (IllegalArgumentException e) {
            throw new InvalidHeaderException(e.getMessage(), e);
        }
    }

    /**
     * Returns the bounding box of the archive in WGS84 (EPSG:4326) longitude and latitude degrees.
     *
     * <p>The versatiles tool writes an inverted box, with a minimum above its maximum, when it converts with a bounding
     * box outside the data. Such a box gives no usable bounds, and this method returns the bounds of the world instead.
     */
    public BoundingBox2D geographicBoundingBox() {
        if (minLon > maxLon || minLat > maxLat) {
            return WORLD;
        }
        return BoundingBox2D.extent(minLon, minLat, maxLon, maxLat);
    }

    /** Returns the position of the metadata in the file. The range is empty when the archive has no metadata. */
    public ByteRange metadataRange() {
        return ByteRange.of(metadataOffset, (int) metadataLength);
    }

    /** Returns the position of the block index in the file. */
    public ByteRange blockIndexRange() {
        return ByteRange.of(blockIndexOffset, (int) blockIndexLength);
    }

    private static ByteBuffer readHeaderBytes(ReadableByteChannel channel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_BYTES).order(ByteOrder.BIG_ENDIAN);
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer);
            if (read < 0) {
                throw new InvalidHeaderException("Unexpected end of file while reading header (got %d / %d bytes)"
                        .formatted(buffer.position(), HEADER_BYTES));
            }
        }
        return buffer.flip();
    }

    private static void requireMagic(ByteBuffer buffer) throws InvalidHeaderException {
        byte[] magicBytes = new byte[MAGIC.length()];
        buffer.get(magicBytes);
        String magic = new String(magicBytes, StandardCharsets.US_ASCII);
        if (!MAGIC.equals(magic)) {
            throw new InvalidHeaderException("Invalid magic bytes: expected '%s', got '%s'".formatted(MAGIC, magic));
        }
    }

    private static VersatilesHeader parseFields(ByteBuffer buffer) {
        TileFormat tileFormat = TileFormat.fromCode(buffer.get());
        Compression compression = Compression.fromCode(buffer.get());
        int minZoom = Byte.toUnsignedInt(buffer.get());
        int maxZoom = Byte.toUnsignedInt(buffer.get());
        double minLon = buffer.getInt() / E7;
        double minLat = buffer.getInt() / E7;
        double maxLon = buffer.getInt() / E7;
        double maxLat = buffer.getInt() / E7;
        long metadataOffset = buffer.getLong();
        long metadataLength = buffer.getLong();
        long blockIndexOffset = buffer.getLong();
        long blockIndexLength = buffer.getLong();
        return new VersatilesHeader(
                tileFormat,
                compression,
                minZoom,
                maxZoom,
                minLon,
                minLat,
                maxLon,
                maxLat,
                metadataOffset,
                metadataLength,
                blockIndexOffset,
                blockIndexLength);
    }
}
