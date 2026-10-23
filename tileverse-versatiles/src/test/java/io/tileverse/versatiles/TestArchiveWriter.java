/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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
package io.tileverse.versatiles;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream;

/**
 * Writes small Versatiles containers for tests. The tiles and the metadata use the compression of the archive. The
 * block index and the tile indexes use stored Brotli meta-blocks: a Brotli decoder reads them like other Brotli
 * streams, and writing them needs no Brotli encoder.
 */
public final class TestArchiveWriter {

    private static final int MAX_STORED_BLOCK = 1 << 16;

    private TileFormat tileFormat = TileFormat.PBF;

    private Compression compression = Compression.NONE;

    private String metadata = "";

    private int[] boundsE7 = {-1_800_000_000, -850_000_000, 1_800_000_000, 850_000_000};

    private final Map<BlockKey, List<TestTile>> blocks = new LinkedHashMap<>();

    public TestArchiveWriter tileFormat(TileFormat format) {
        this.tileFormat = format;
        return this;
    }

    public TestArchiveWriter compression(Compression archiveCompression) {
        this.compression = archiveCompression;
        return this;
    }

    public TestArchiveWriter metadata(String json) {
        this.metadata = json;
        return this;
    }

    /** Sets the bounding box of the header, in degrees, without checking that the minimum is below the maximum. */
    public TestArchiveWriter bounds(double minLon, double minLat, double maxLon, double maxLat) {
        this.boundsE7 = new int[] {toE7(minLon), toE7(minLat), toE7(maxLon), toE7(maxLat)};
        return this;
    }

    private static int toE7(double degrees) {
        return (int) Math.round(degrees * 1e7);
    }

    public TestArchiveWriter tile(int z, long x, long y, String body) {
        BlockKey key = new BlockKey(z, x / 256, y / 256);
        TestTile tile = new TestTile(z, x, y, body.getBytes(StandardCharsets.UTF_8));
        blocks.computeIfAbsent(key, k -> new ArrayList<>()).add(tile);
        return this;
    }

    public Path write(Path file) throws IOException {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        byte[] encodedMetadata = encode(metadata.getBytes(StandardCharsets.UTF_8));
        body.write(encodedMetadata);
        ByteArrayOutputStream blockIndex = new ByteArrayOutputStream();
        for (List<TestTile> tiles : blocks.values()) {
            long blockOffset = VersatilesHeader.HEADER_BYTES + (long) body.size();
            blockIndex.write(writeBlock(tiles, blockOffset, body));
        }
        long blockIndexOffset = VersatilesHeader.HEADER_BYTES + (long) body.size();
        byte[] encodedBlockIndex = encodeBlockIndex(blockIndex.toByteArray());
        body.write(encodedBlockIndex);

        ByteBuffer header = header(encodedMetadata.length, blockIndexOffset, encodedBlockIndex.length);
        ByteArrayOutputStream archive = new ByteArrayOutputStream();
        archive.write(header.array());
        archive.write(body.toByteArray());
        return Files.write(file, archive.toByteArray());
    }

    /**
     * The versatiles tool writes no block index at all, not even an empty Brotli stream, for an archive without tiles.
     */
    private byte[] encodeBlockIndex(byte[] entries) {
        if (blocks.isEmpty()) {
            return new byte[0];
        }
        return storedBrotli(entries);
    }

    /** Appends the tile bodies and the tile index of a block to {@code out} and returns its block index entry. */
    private byte[] writeBlock(List<TestTile> tiles, long blockOffset, ByteArrayOutputStream out) throws IOException {
        TestTile first = tiles.get(0);
        int colMin = tiles.stream().mapToInt(TestTile::col).min().orElseThrow();
        int colMax = tiles.stream().mapToInt(TestTile::col).max().orElseThrow();
        int rowMin = tiles.stream().mapToInt(TestTile::row).min().orElseThrow();
        int rowMax = tiles.stream().mapToInt(TestTile::row).max().orElseThrow();
        int width = colMax - colMin + 1;
        int height = rowMax - rowMin + 1;

        ByteBuffer tileIndex = ByteBuffer.allocate(width * height * 12);
        long tileBlobsLength = 0;
        for (TestTile tile : tiles) {
            byte[] encoded = encode(tile.body());
            int slot = (tile.row() - rowMin) * width + (tile.col() - colMin);
            tileIndex.putLong(slot * 12, tileBlobsLength);
            tileIndex.putInt(slot * 12 + 8, encoded.length);
            out.write(encoded);
            tileBlobsLength += encoded.length;
        }
        byte[] encodedTileIndex = storedBrotli(tileIndex.array());
        out.write(encodedTileIndex);

        ByteBuffer entry = ByteBuffer.allocate(33);
        entry.put((byte) first.z());
        entry.putInt((int) (first.x() / 256));
        entry.putInt((int) (first.y() / 256));
        entry.put((byte) colMin).put((byte) rowMin).put((byte) colMax).put((byte) rowMax);
        entry.putLong(blockOffset);
        entry.putLong(tileBlobsLength);
        entry.putInt(encodedTileIndex.length);
        return entry.array();
    }

    private ByteBuffer header(int metadataLength, long blockIndexOffset, int blockIndexLength) {
        int minZoom = blocks.keySet().stream().mapToInt(BlockKey::level).min().orElse(0);
        int maxZoom = blocks.keySet().stream().mapToInt(BlockKey::level).max().orElse(0);
        ByteBuffer header = ByteBuffer.allocate(VersatilesHeader.HEADER_BYTES);
        header.put(VersatilesHeader.MAGIC.getBytes(StandardCharsets.US_ASCII));
        header.put(tileFormat.code());
        header.put(compression.code());
        header.put((byte) minZoom);
        header.put((byte) maxZoom);
        header.putInt(boundsE7[0]).putInt(boundsE7[1]).putInt(boundsE7[2]).putInt(boundsE7[3]);
        header.putLong(VersatilesHeader.HEADER_BYTES).putLong(metadataLength);
        header.putLong(blockIndexOffset).putLong(blockIndexLength);
        return header;
    }

    private byte[] encode(byte[] data) {
        return switch (compression) {
            case NONE -> data;
            case GZIP -> gzip(data);
            case BROTLI -> storedBrotli(data);
            case ZSTD -> zstd(data);
        };
    }

    private static byte[] zstd(byte[] data) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ZstdCompressorOutputStream zstd = new ZstdCompressorOutputStream(out)) {
            zstd.write(data);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return out.toByteArray();
    }

    private static byte[] gzip(byte[] data) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
            gzip.write(data);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return out.toByteArray();
    }

    /**
     * Encodes {@code data} as a Brotli stream of uncompressed meta-blocks (RFC 7932, section 9.2). The first meta-block
     * follows a window size bit; a meta-block header holds ISLAST=0, four length nibbles, MLEN-1 and ISUNCOMPRESSED=1,
     * and the raw bytes start at the next byte boundary. An empty last meta-block ends the stream.
     */
    static byte[] storedBrotli(byte[] data) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int offset = 0;
        boolean first = true;
        while (offset < data.length) {
            int length = Math.min(MAX_STORED_BLOCK, data.length - offset);
            writeStoredHeader(out, length, first);
            out.write(data, offset, length);
            offset += length;
            first = false;
        }
        int lastEmptyMetaBlock = first ? 0x06 : 0x03;
        out.write(lastEmptyMetaBlock);
        return out.toByteArray();
    }

    private static void writeStoredHeader(ByteArrayOutputStream out, int length, boolean first) {
        int windowBits = first ? 1 : 0;
        long bits = ((long) length - 1) << (windowBits + 3);
        bits |= 1L << (windowBits + 19);
        int byteCount = (windowBits + 20 + 7) / 8;
        for (int i = 0; i < byteCount; i++) {
            out.write((int) (bits >>> (8 * i)) & 0xFF);
        }
    }

    private record BlockKey(int level, long col, long row) {}

    private record TestTile(int z, long x, long y, byte[] body) {
        int col() {
            return (int) (x % 256);
        }

        int row() {
            return (int) (y % 256);
        }
    }
}
