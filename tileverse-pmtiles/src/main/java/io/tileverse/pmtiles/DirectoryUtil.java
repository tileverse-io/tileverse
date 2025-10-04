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

import io.tileverse.pmtiles.PMTilesDirectory.Builder;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Utility class for serializing and deserializing PMTiles directories.
 */
final class DirectoryUtil {

    private DirectoryUtil() {
        // Prevent instantiation
    }

    /**
     * Serializes a list of PMTilesEntry objects into a byte array.
     * <p>
     * The directory format consists of:
     * <ul>
     *   <li>Number of entries (varint)</li>
     *   <li>Delta-encoded tile IDs (varints)</li>
     *   <li>Run lengths (varints)</li>
     *   <li>Lengths (varints)</li>
     *   <li>Offsets (varints, with delta encoding for consecutive entries)</li>
     * </ul>
     *
     * @param entries the list of entries to serialize
     * @return the serialized directory as a byte array
     * @throws IOException if an I/O error occurs
     */
    public static byte[] serializeDirectory(List<PMTilesEntry> entries) throws IOException {
        // Sort entries by tile ID if not already sorted
        List<PMTilesEntry> sortedEntries = new ArrayList<>(entries);
        Collections.sort(sortedEntries);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        // Write number of entries
        writeVarint(baos, entries.size());

        // Write tile IDs (delta encoded)
        long lastId = 0;
        for (PMTilesEntry entry : sortedEntries) {
            writeVarint(baos, entry.tileId() - lastId);
            lastId = entry.tileId();
        }

        // Write run lengths
        for (PMTilesEntry entry : sortedEntries) {
            writeVarint(baos, entry.runLength());
        }

        // Write lengths
        for (PMTilesEntry entry : sortedEntries) {
            writeVarint(baos, entry.length());
        }

        // Write offsets (with optimization for consecutive entries)
        for (int i = 0; i < sortedEntries.size(); i++) {
            if (i > 0
                    && sortedEntries.get(i).offset()
                            == sortedEntries.get(i - 1).offset()
                                    + sortedEntries.get(i - 1).length()) {
                writeVarint(baos, 0);
            } else {
                writeVarint(baos, sortedEntries.get(i).offset() + 1);
            }
        }

        return baos.toByteArray();
    }

    /**
     * Deserializes a byte array into a list of PMTilesEntry objects.
     *
     * @param buffer the serialized directory data
     * @return the list of directory entries
     * @throws IOException if an I/O error occurs or the data is malformed
     */
    public static PMTilesDirectory deserializeDirectory(ByteBuffer buffer) throws IOException {

        // Read number of entries
        final int numEntries = (int) readVarint(buffer);
        Builder builder = PMTilesDirectory.builder(numEntries);

        // Read tile IDs (delta encoded)
        long lastId = 0;
        for (int i = 0; i < numEntries; i++) {
            long tileId = lastId + readVarint(buffer);
            lastId = tileId;
            builder.tileId(i, tileId);
        }

        // Read run lengths
        for (int i = 0; i < numEntries; i++) {
            int runLength = (int) readVarint(buffer);
            builder.runLength(i, runLength);
        }

        // Read lengths
        for (int i = 0; i < numEntries; i++) {
            int length = (int) readVarint(buffer);
            builder.length(i, length);
        }

        // Read offsets (with optimization for consecutive entries)
        for (int i = 0; i < numEntries; i++) {
            long tmp = readVarint(buffer);
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
        // Ensure we've consumed the entire buffer
        if (buffer.hasRemaining()) {
            throw new IOException("Malformed PMTiles directory: additional data at end of buffer");
        }

        return builder.build();
    }

    /**
     * Builds root and leaf directories from a list of entries.
     * <p>
     * This splits a large directory into a root directory and multiple leaf directories
     * to keep the root directory size under the maximum size (typically 16KB).
     *
     * @param entries the entries to include in the directories
     * @param compressionType the compression type to use
     * @param maxRootDirSize the maximum size for the root directory
     * @return a triple containing the compressed root directory, the compressed leaf directories,
     *         and the number of leaf directories
     * @throws IOException if an I/O error occurs
     * @throws UnsupportedCompressionException if the compression type is not supported
     */
    public static DirectoryResult buildRootLeaves(List<PMTilesEntry> entries, byte compressionType, int maxRootDirSize)
            throws IOException, UnsupportedCompressionException {

        // Try with just the root directory first
        byte[] uncompressedDir = serializeDirectory(entries);
        byte[] compressedDir = CompressionUtil.compress(uncompressedDir, compressionType);

        // If the root directory is small enough, we don't need leaf directories
        if (compressedDir.length <= maxRootDirSize) {
            return new DirectoryResult(compressedDir, new byte[0], 0);
        }

        // Otherwise, split into root and leaf directories
        int leafSize = 4096;
        while (true) {
            DirectoryResult result = buildRootLeavesWithSize(entries, compressionType, leafSize);

            if (result.rootDirectory().length <= maxRootDirSize) {
                return result;
            }

            // Try again with a larger leaf size
            leafSize *= 2;
        }
    }

    /**
     * Helper method to build root and leaf directories with a specific leaf size.
     *
     * @param entries the entries to include in the directories
     * @param compressionType the compression type to use
     * @param leafSize the number of entries per leaf directory
     * @return a triple containing the compressed root directory, the compressed leaf directories,
     *         and the number of leaf directories
     * @throws IOException if an I/O error occurs
     * @throws UnsupportedCompressionException if the compression type is not supported
     */
    private static DirectoryResult buildRootLeavesWithSize(
            List<PMTilesEntry> entries, byte compressionType, int leafSize)
            throws IOException, UnsupportedCompressionException {

        List<PMTilesEntry> rootEntries = new ArrayList<>();
        ByteArrayOutputStream leafDirsStream = new ByteArrayOutputStream();
        int numLeaves = 0;

        for (int i = 0; i < entries.size(); i += leafSize) {
            numLeaves++;
            int end = Math.min(i + leafSize, entries.size());
            List<PMTilesEntry> subentries = entries.subList(i, end);

            byte[] uncompressedLeaf = serializeDirectory(subentries);
            byte[] compressedLeaf = CompressionUtil.compress(uncompressedLeaf, compressionType);

            rootEntries.add(
                    PMTilesEntry.leaf(subentries.get(0).tileId(), leafDirsStream.size(), compressedLeaf.length));

            leafDirsStream.write(compressedLeaf);
        }

        byte[] uncompressedRoot = serializeDirectory(rootEntries);
        byte[] compressedRoot = CompressionUtil.compress(uncompressedRoot, compressionType);
        byte[] leafDirs = leafDirsStream.toByteArray();

        return new DirectoryResult(compressedRoot, leafDirs, numLeaves);
    }

    /**
     * Writes a variable-length integer (varint) to an output stream.
     *
     * @param out the output stream to write to
     * @param value the value to write
     * @throws IOException if an I/O error occurs
     */
    private static void writeVarint(ByteArrayOutputStream out, long value) throws IOException {
        while (value >= 0x80L) {
            out.write((int) ((value & 0x7FL) | 0x80L));
            value >>>= 7;
        }
        out.write((int) value);
    }

    /**
     * Reads a variable-length integer (varint) from a byte buffer.
     *
     * @param buffer the byte buffer to read from
     * @return the value read
     * @throws IOException if an I/O error occurs or the varint is malformed
     */
    private static long readVarint(ByteBuffer buffer) throws IOException {
        long value = 0;
        int shift = 0;
        byte b;

        do {
            if (!buffer.hasRemaining()) {
                throw new IOException("Unexpected end of buffer while reading varint");
            }
            if (shift >= 64) {
                throw new IOException("Varint too long");
            }

            b = buffer.get();
            value |= (long) (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);

        return value;
    }

    /**
     * Result of building root and leaf directories.
     *
     * @param rootDirectory the compressed root directory
     * @param leafDirectories the compressed leaf directories
     * @param numLeaves the number of leaf directories
     */
    public record DirectoryResult(byte[] rootDirectory, byte[] leafDirectories, int numLeaves) {}
}
