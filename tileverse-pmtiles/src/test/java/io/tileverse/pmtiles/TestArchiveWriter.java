/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.pmtiles;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Writes PMTiles archives of one tile for tests: the tile of zoom level 0. The archives have no metadata, and nothing
 * in them is compressed. The specification requires at least one entry in a directory, and an archive without tiles
 * would be invalid.
 */
public final class TestArchiveWriter {

    private final byte[] rootTile;

    private byte tileType = PMTilesHeader.TILETYPE_PNG;

    private boolean inLeafDirectory;

    public TestArchiveWriter(byte[] rootTile) {
        this.rootTile = rootTile;
    }

    public TestArchiveWriter(String rootTile) {
        this(rootTile.getBytes(StandardCharsets.UTF_8));
    }

    public TestArchiveWriter tileType(byte type) {
        this.tileType = type;
        return this;
    }

    /** Lists the tile in a leaf directory instead of the root directory. The root directory points to the leaf. */
    public TestArchiveWriter inLeafDirectory() {
        this.inLeafDirectory = true;
        return this;
    }

    public Path write(Path file) throws IOException {
        byte[] tileDirectory = serialize(PMTilesEntry.of(0, 0, rootTile.length, 1));
        byte[] rootDirectory = tileDirectory;
        byte[] leafDirectories = new byte[0];
        if (inLeafDirectory) {
            // a run length of zero marks a leaf directory
            rootDirectory = serialize(PMTilesEntry.of(0, 0, tileDirectory.length, 0));
            leafDirectories = tileDirectory;
        }

        long leafDirectoriesOffset = PMTilesHeader.HEADER_SIZE + (long) rootDirectory.length;
        long tileDataOffset = leafDirectoriesOffset + leafDirectories.length;
        PMTilesHeader header = PMTilesHeader.builder()
                .rootDirOffset(PMTilesHeader.HEADER_SIZE)
                .rootDirBytes(rootDirectory.length)
                .jsonMetadataOffset(leafDirectoriesOffset)
                .jsonMetadataBytes(0)
                .leafDirsOffset(leafDirectoriesOffset)
                .leafDirsBytes(leafDirectories.length)
                .tileDataOffset(tileDataOffset)
                .tileDataBytes(rootTile.length)
                .addressedTilesCount(1)
                .tileEntriesCount(1)
                .tileContentsCount(1)
                .clustered(true)
                .internalCompression(PMTilesHeader.COMPRESSION_NONE)
                .tileCompression(PMTilesHeader.COMPRESSION_NONE)
                .tileType(tileType)
                .build();

        ByteArrayOutputStream archive = new ByteArrayOutputStream();
        archive.write(header.serialize());
        archive.write(rootDirectory);
        archive.write(leafDirectories);
        archive.write(rootTile);
        return Files.write(file, archive.toByteArray());
    }

    /** Encodes a directory of one entry as in the PMTiles specification. */
    private static byte[] serialize(PMTilesEntry entry) {
        List<Long> fields = List.of(
                1L, // number of entries
                entry.tileId(), // later entries would hold the difference with the previous tile id
                (long) entry.runLength(),
                (long) entry.length(),
                entry.offset() + 1); // zero would mean "right after the previous entry"
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        fields.forEach(field -> writeVarint(out, field));
        return out.toByteArray();
    }

    private static void writeVarint(ByteArrayOutputStream out, long value) {
        long remaining = value;
        while (remaining >= 0x80) {
            out.write((int) (remaining & 0x7F) | 0x80);
            remaining >>>= 7;
        }
        out.write((int) remaining);
    }
}
