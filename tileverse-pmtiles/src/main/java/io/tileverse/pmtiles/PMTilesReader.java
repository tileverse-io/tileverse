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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.tileverse.jackson.databind.pmtiles.v3.PMTilesMetadata;
import io.tileverse.rangereader.ByteRange;
import io.tileverse.rangereader.RangeReader;
import io.tileverse.rangereader.file.FileRangeReader;
import io.tileverse.rangereader.nio.ByteBufferPool;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.io.IOUtils;

/**
 * Reader for PMTiles files that provides access to tiles and metadata.
 * <p>
 * This class implements the PMTiles format specification, providing a clean API
 * for accessing tile data, metadata, and directory structures within a PMTiles file.
 * <p>
 * It relies on the {@link RangeReader} interface for random access to the underlying
 * data source, allowing for efficient reading from local files, HTTP servers, or cloud storage.
 * <p>
 * Example usage:
 * <pre>{@code
 * // Create a RangeReader for the desired source
 * RangeReader reader = new FileRangeReader(Path.of("/path/to/tiles.pmtiles"));
 *
 * // For cloud storage or HTTP, create the appropriate RangeReader
 * // RangeReader reader = RangeReaderFactory.create(URI.create("s3://bucket/tiles.pmtiles"));
 *
 * // For performance optimization with cloud storage, use block alignment and caching
 * // RangeReader reader = RangeReaderFactory.createBlockAlignedCaching(
 * //     RangeReaderFactory.create(URI.create("s3://bucket/tiles.pmtiles")));
 *
 * // Create the PMTilesReader with the RangeReader
 * try (PMTilesReader pmtiles = new PMTilesReader(reader)) {
 *     // Access tiles, metadata, etc.
 *     Optional<byte[]> tile = pmtiles.getTile(10, 885, 412);
 * }
 * }</pre>
 */
public class PMTilesReader implements Closeable {

    private final RangeReader rangeReader;
    private final PMTilesHeader header;
    private final ObjectMapper objectMapper;

    /**
     * Creates a new PMTilesReader for the specified file.
     *
     * @param path the path to the PMTiles file
     * @throws IOException if an I/O error occurs
     * @throws InvalidHeaderException if the file has an invalid header
     */
    public PMTilesReader(Path path) throws IOException, InvalidHeaderException {
        this(FileRangeReader.of(path));
    }

    @Override
    public void close() throws IOException {
        rangeReader.close();
    }

    /**
     * Creates a new PMTilesReader using the specified RangeReader.
     * <p>
     * This constructor allows reading PMTiles from any source that implements the
     * RangeReader interface, such as local files, HTTP servers, or cloud storage.
     *
     * @param rangeReader the range reader to use
     * @throws IOException if an I/O error occurs
     * @throws InvalidHeaderException if the file has an invalid header
     */
    public PMTilesReader(RangeReader rangeReader) throws IOException, InvalidHeaderException {
        this.rangeReader = Objects.requireNonNull(rangeReader, "RangeReader cannot be null");
        this.objectMapper = new ObjectMapper();

        // Read the header
        ByteBuffer headerBuffer = ByteBufferPool.getDefault().borrowHeap(127);
        try {
            if (rangeReader.readRange(0, 127, headerBuffer) != 127) {
                throw new InvalidHeaderException("Failed to read complete header");
            }

            // Deserialize the header directly from the ByteBuffer
            this.header = PMTilesHeader.deserialize(headerBuffer);
        } finally {
            ByteBufferPool.getDefault().returnBuffer(headerBuffer);
        }
    }

    /**
     * Gets the header of the PMTiles file.
     *
     * @return the PMTiles header
     */
    public PMTilesHeader getHeader() {
        return header;
    }

    /**
     * Gets a tile by its ZXY coordinates.
     *
     * @param z the zoom level
     * @param x the X coordinate
     * @param y the Y coordinate
     * @return the tile data, or empty if the tile doesn't exist
     * @throws IOException if an I/O error occurs
     * @throws UnsupportedCompressionException if the compression type is not supported
     */
    public Optional<ByteBuffer> getTile(int z, int x, int y) throws IOException {

        ZXY zxy = ZXY.of(z, x, y);
        long tileId = zxy.toTileId();

        // Find the tile in the directory structure
        return findTileLocation(tileId).map(this::getTile);
    }

    /**
     * Reads and returns the decompressed tile data
     * @param absolutePosition tile location (offset/length)
     * @throws UncheckedIOException
     * @return the tile contents
     */
    private ByteBuffer getTile(ByteRange absolutePosition) {
        return readData(absolutePosition, header.tileCompression());
    }
    /**
     * Gets the raw, uncompressed, metadata JSON from the PMTiles file.
     *
     * @return the metadata as a byte array
     * @throws IOException if an I/O error occurs
     * @throws UnsupportedCompressionException if the compression type is not supported
     */
    public ByteBuffer getRawMetadata() throws IOException, UnsupportedCompressionException {

        final long offset = header.jsonMetadataOffset();
        final int length = (int) header.jsonMetadataBytes();
        return readData(ByteRange.of(offset, length), header.internalCompression());
    }

    /**
     * Gets the metadata as a parsed JSON string.
     *
     * @return the metadata as a JSON string
     * @throws IOException if an I/O error occurs
     * @throws UnsupportedCompressionException if the compression type is not supported
     */
    public String getMetadataAsString() throws IOException, UnsupportedCompressionException {
        return toString(getRawMetadata());
    }

    /**
     * Gets the metadata as a parsed {@link PMTilesMetadata} object.
     * <p>
     * This provides structured access to the metadata fields with proper type conversion.
     *
     * @return the metadata as a PMTilesMetadata object
     * @throws IOException if an I/O error occurs or JSON parsing fails
     * @throws UnsupportedCompressionException if the compression type is not supported
     */
    public PMTilesMetadata getMetadata() throws IOException, UnsupportedCompressionException {
        ByteBuffer metadataBytes = getRawMetadata();
        if (metadataBytes.remaining() == 0) {
            // Return empty metadata if no metadata is present
            return PMTilesMetadata.of(null);
        }

        try (ByteBufferInputStream in = new ByteBufferInputStream(metadataBytes)) {
            return objectMapper.readValue(in, PMTilesMetadata.class);
        } catch (Exception e) {
            throw new IOException("Failed to parse PMTiles metadata JSON", e);
        }
    }

    /**
     * Finds the location of a tile in the PMTiles file using recursive directory traversal.
     *
     * @param tileId the ID of the tile to find
     * @return the location of the tile, or empty if the tile doesn't exist
     * @throws IOException if an I/O error occurs
     * @throws UnsupportedCompressionException if the compression type is not supported
     */
    private Optional<ByteRange> findTileLocation(long tileId) throws IOException, UnsupportedCompressionException {
        final long rootDirOffset = header.rootDirOffset();
        final int rootDirLength = (int) header.rootDirBytes();
        final boolean isLeafDir = false;
        return searchDirectory(ByteRange.of(rootDirOffset, rootDirLength), tileId, isLeafDir);
    }

    /**
     * Recursively searches directories for a tile entry.
     *
     * @param dirOffset the offset of the directory to search
     * @param dirLength the length of the directory data
     * @param tileId the tile ID to find
     * @param isLeafDir whether this is a leaf directory (determines offset calculation)
     * @return the absolute tile location if found, or empty if not found
     * @throws IOException if an I/O error occurs
     * @throws UnsupportedCompressionException if the compression type is not supported
     */
    private Optional<ByteRange> searchDirectory(ByteRange entryRange, final long tileId, final boolean isLeafDir)
            throws IOException, UnsupportedCompressionException {

        // Read and deserialize directory
        ByteBuffer dirBytes = readDirectoryBytes(entryRange);
        List<PMTilesEntry> entries = DirectoryUtil.deserializeDirectory(dirBytes);

        // Find entry that might contain our tileId
        Optional<PMTilesEntry> entry = findEntryForTileId(entries, tileId);

        if (entry.isEmpty()) {
            return Optional.empty();
        }

        PMTilesEntry found = entry.get();

        if (found.isLeaf()) {
            // Recursively search the leaf directory
            return searchDirectory(header.leafDirDataRange(found), tileId, true);
        } else {
            // This is a tile entry - filter out empty tiles
            return Optional.of(found).filter(e -> !e.isEmpty()).map(header::tileDataRange);
        }
    }

    /**
     * Searches for a directory entry that contains the specified tile ID using binary search.
     * This method handles both regular tile entries (with run lengths) and leaf directory entries.
     *
     * @param entries the list of directory entries to search (must be sorted by tileId)
     * @param tileId the tile ID to search for
     * @return the directory entry that contains the tile, or empty if no suitable entry found
     */
    private Optional<PMTilesEntry> findEntryForTileId(List<PMTilesEntry> entries, long tileId) {
        int low = 0;
        int high = entries.size() - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            PMTilesEntry entry = entries.get(mid);

            if (tileId < entry.tileId()) {
                high = mid - 1;
            } else if (entry.isLeaf() || entry.runLength() == 0) {
                // For leaf entries or entries with no run length, match exact tileId
                if (tileId == entry.tileId()) {
                    return Optional.of(entry);
                } else if (tileId > entry.tileId()) {
                    low = mid + 1;
                } else {
                    high = mid - 1;
                }
            } else {
                // For regular entries, check if tileId falls within the run range
                long entryEnd = entry.tileId() + entry.runLength() - 1;
                if (tileId <= entryEnd) {
                    return Optional.of(entry);
                } else {
                    low = mid + 1;
                }
            }
        }

        // No exact match found, check for containing entry at insertion point
        return findContainingEntry(entries, high, tileId);
    }

    /**
     * Attempts to find an entry that contains the target tileId at the binary search insertion point.
     *
     * <p>This method handles the PMTiles format's range-based entries where a single directory entry
     * can represent multiple consecutive tiles. After a binary search fails to find an exact match,
     * the entry just before the insertion point might still contain our target tile.
     *
     * <p>Two cases are handled:
     * <ul>
     * <li><b>Regular entries with run lengths</b>: An entry with tileId=100 and runLength=5 represents
     *     tiles 100-104. If searching for tileId=102, this entry should be returned even though
     *     102 != 100, because 102 falls within the range [100, 104].</li>
     * <li><b>Leaf directory entries</b>: These point to subdirectories that might contain the target
     *     tile. Since we don't know the exact bounds of leaf directories, we return the closest
     *     leaf entry as a heuristic - it might contain our target in its subdirectory.</li>
     * </ul>
     *
     * @param entries the list of directory entries that was searched
     * @param insertionPoint the index where the target would be inserted (from binary search)
     * @param tileId the tile ID we're searching for
     * @return the containing entry if found, or empty if no suitable entry exists
     */
    private Optional<PMTilesEntry> findContainingEntry(List<PMTilesEntry> entries, int insertionPoint, long tileId) {
        if (insertionPoint < 0) {
            return Optional.empty();
        }

        PMTilesEntry candidate = entries.get(insertionPoint);

        if (candidate.isLeaf()) {
            // Return leaf directory - it might contain our tile in its subdirectory
            return Optional.of(candidate);
        } else if (candidate.runLength() > 0) {
            // Check if tileId falls within the entry's run range [tileId, tileId + runLength - 1]
            long rangeEnd = candidate.tileId() + candidate.runLength() - 1;
            if (tileId <= rangeEnd) {
                return Optional.of(candidate);
            }
        }

        return Optional.empty();
    }

    /**
     * Reads directory bytes from the file.
     *
     * @param offset the offset to read from
     * @param length the number of bytes to read
     * @return the directory bytes, decompressed if necessary
     * @throws IOException if an I/O error occurs
     * @throws UnsupportedCompressionException if the compression type is not supported
     */
    private ByteBuffer readDirectoryBytes(ByteRange byteRange) throws IOException, UnsupportedCompressionException {
        return readData(byteRange, header.internalCompression());
    }

    private ByteBuffer readData(ByteRange range, byte compression) {
        ByteBuffer buffer = ByteBufferPool.getDefault().borrowHeap(range.length());
        try {
            rangeReader.readRange(range, buffer);
            return CompressionUtil.decompress(buffer, compression);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            ByteBufferPool.getDefault().returnBuffer(buffer);
        }
    }

    /**
     * Represents a tile with its coordinates and data.
     *
     * @param z the zoom level
     * @param x the X coordinate
     * @param y the Y coordinate
     * @param data the tile data
     */
    public record Tile(byte z, int x, int y, byte[] data) {}

    String toString(ByteBuffer byteBuffer) {
        try {
            return IOUtils.toString(new ByteBufferInputStream(byteBuffer), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
