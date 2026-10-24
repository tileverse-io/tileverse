/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.pmtiles;

import io.tileverse.pmtiles.PMTilesDirectoryImpl.Builder;
import java.io.IOException;
import java.io.InputStream;

/** Utility class for deserializing PMTiles directories. */
final class DirectoryUtil {

    private DirectoryUtil() {
        // Prevent instantiation
    }

    public static PMTilesDirectory deserializeDirectory(InputStream packedData) throws IOException {

        // Read number of entries
        final int numEntries = (int) readVarint(packedData);
        Builder builder = PMTilesDirectoryImpl.builder(numEntries);

        // Read tile IDs (delta encoded)
        long lastId = 0;
        for (int i = 0; i < numEntries; i++) {
            long tileId = lastId + readVarint(packedData);
            lastId = tileId;
            builder.tileId(i, tileId);
        }

        // Read run lengths
        for (int i = 0; i < numEntries; i++) {
            int runLength = (int) readVarint(packedData);
            builder.runLength(i, runLength);
        }

        // Read lengths
        for (int i = 0; i < numEntries; i++) {
            int length = (int) readVarint(packedData);
            builder.length(i, length);
        }

        // Read offsets (with optimization for consecutive entries)
        for (int i = 0; i < numEntries; i++) {
            long tmp = readVarint(packedData);
            long offset;

            if (i > 0 && tmp == 0) {
                // Consecutive optimization: offset = previous entry's offset + length
                long previousOffset = builder.getOffset(i - 1);
                int previousLength = builder.getLength(i - 1);
                offset = previousOffset + previousLength;
            } else {
                offset = tmp - 1;
            }
            builder.offset(i, offset);
        }

        return builder.build();
    }

    private static long readVarint(InputStream input) throws IOException {
        long value = 0;
        int shift = 0;
        byte b;

        do {
            if (shift >= 64) {
                throw new IOException("Varint too long");
            }

            b = (byte) input.read();
            value |= (long) (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);

        return value;
    }
}
