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
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.tileverse.io.ByteBufferPool;
import io.tileverse.io.ByteRange;
import io.tileverse.jackson.databind.pmtiles.v3.PMTilesMetadata;
import io.tileverse.rangereader.RangeReader;
import io.tileverse.tiling.pyramid.TileIndex;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reader for PMTiles files that provides access to tiles and metadata.
 * <p>
 * This class implements the PMTiles format specification, providing a clean API
 * for accessing tile data, metadata, and directory structures within a PMTiles
 * file.
 * <p>
 * It relies on a {@code Supplier<SeekableByteChannel>}, from which it
 * will <strong>acquire and close</strong> a {@link SeekableByteChannel} upon
 * each I/O operation.
 * <p>
 * Therefore this reader does not own the underlying input source and won't close it
 * explicitly unless it provides a new instance on each supplied byte channel.
 * <p>
 * It is recommended to use the {@link RangeReader} library for random access to the
 * underlying data source, allowing for efficient reading from local files, HTTP
 * servers, or cloud storage; though it's not mandatory.
 * <p>
 * Example usage:
 *
 * <pre>{@code
 * // Create a RangeReader for the desired source
 * RangeReader reader = new FileRangeReader(Path.of("/path/to/tiles.pmtiles"));
 *
 * // For cloud storage or HTTP, create the appropriate RangeReader
 * // RangeReader reader = RangeReaderFactory.create(URI.create("s3://bucket/tiles.pmtiles"));
 *
 * // Create the PMTilesReader with the RangeReader
 * try (PMTilesReader pmtiles = new PMTilesReader(reader)) {
 * 	// Access tiles, metadata, etc.
 * 	Optional<byte[]> tile = pmtiles.getTile(10, 885, 412);
 * }
 * }</pre>
 */
public class PMTilesReader {

    private static final Logger log = LoggerFactory.getLogger(PMTilesReader.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final Supplier<SeekableByteChannel> channelSupplier;

    private static final Duration expireAfterAccess = Duration.ofSeconds(30);

    /**
     * Short-lived (expireAfterAccess) cache of directory entries to account for multiple/concurrent requests
     */
    private final LoadingCache<ByteRange, PMTilesDirectory> directoryCache;

    private final PMTilesHeader header;

    /**
     * @see #getMetadata()
     */
    private PMTilesMetadata parsedMetadata;

    /**
     * Creates a new PMTilesReader for the specified file.
     * <p>
     * This constructor creates a {@link SeekableByteChannelSupplier} that will open and close the file upon each I/O operation.
     *
     * @param path the path to the PMTiles file
     * @throws IOException if an I/O error occurs
     * @throws InvalidHeaderException if the file has an invalid header
     */
    public PMTilesReader(Path path) throws IOException, InvalidHeaderException {
        this(fileChannelSupplier(path));
    }

    private static Supplier<SeekableByteChannel> fileChannelSupplier(Path path) {
        return () -> {
            try {
                return FileChannel.open(path, StandardOpenOption.READ);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    /**
     * Creates a new PMTilesReader using the specified RangeReader.
     * <p>
     * This constructor allows reading PMTiles from any source that implements the
     * RangeReader interface, such as local files, HTTP servers, or cloud storage.
     *
     * @param rangeReaderSupplier the range reader to use
     * @throws IOException if an I/O error occurs
     * @throws InvalidHeaderException if the file has an invalid header
     */
    public PMTilesReader(Supplier<SeekableByteChannel> rangeReaderSupplier) throws IOException, InvalidHeaderException {
        this.channelSupplier = Objects.requireNonNull(rangeReaderSupplier, "rangeReaderSupplier cannot be null");
        try (SeekableByteChannel channel = channel()) {
            this.header = PMTilesHeader.readHeader(channel);
        }
        this.directoryCache = Caffeine.newBuilder()
                .softValues()
                .expireAfterAccess(expireAfterAccess)
                .build(this::readDirectory);
    }

    private SeekableByteChannel channel() throws IOException {
        try {
            return channelSupplier.get();
        } catch (UncheckedIOException e) {
            throw e.getCause();
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
        return getTile(TileIndex.xyz((long) x, (long) y, z));
    }

    public Optional<ByteBuffer> getTile(TileIndex tileIndex) throws IOException {
        return getTile(tileIndex, Function.identity());
    }

    public <D> Optional<D> getTile(TileIndex tileIndex, Function<ByteBuffer, D> mapper) throws IOException {
        if (tileIndex.z() < 0) throw new IllegalArgumentException("z can't be < 0");
        if (tileIndex.x() < 0) throw new IllegalArgumentException("x can't be < 0");
        if (tileIndex.y() < 0) throw new IllegalArgumentException("y can't be < 0");

        long tileId = HilbertCurve.tileIndexToTileId(tileIndex);

        // Find the tile in the directory structure
        Optional<ByteRange> tileLocation = findTileLocation(tileId);
        if (log.isDebugEnabled()) {
            log.debug(
                    "PMTilesReader.getTile({}): tileId={}, location: {}", tileIndex, tileId, tileLocation.orElse(null));
        }
        return tileLocation.map(this::readTile).map(mapper);
    }

    public Stream<TileIndex> getTileIndices() {
        IntStream zooms = IntStream.rangeClosed(header.minZoom(), header.maxZoom());
        return zooms.mapToObj(Integer::valueOf).flatMap(this::getTileIndicesByZoomLevel);
    }

    /**
     * Returns a stream of all tile indices present in the PMTiles file at the specified zoom level.
     * <p>
     * This method traverses the sparse directory structure of the PMTiles file and collects
     * all tiles that exist at the given zoom level. Unlike a continuous TileMatrix grid,
     * PMTiles files contain only the tiles that were actually written to the file.
     * <p>
     * The returned stream provides an efficient way to iterate over all tiles at a zoom level
     * without having to test each possible tile coordinate for existence.
     *
     * @param zoomLevel the zoom level to query (0-based)
     * @return a stream of TileIndex objects representing all tiles present at the zoom level
     * @throws IOException if an I/O error occurs while reading the directory structure
     * @throws UnsupportedCompressionException if the compression type is not supported
     */
    public Stream<TileIndex> getTileIndicesByZoomLevel(int zoomLevel) {
        if (zoomLevel < 0 || zoomLevel > 31) {
            throw new IllegalArgumentException("Zoom level must be between 0 and 31, got: " + zoomLevel);
        }

        List<TileIndex> tileIndices = new java.util.ArrayList<>();
        try {
            collectTileIndicesForZoomLevel(
                    ByteRange.of(header.rootDirOffset(), (int) header.rootDirBytes()), zoomLevel, tileIndices, false);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return tileIndices.stream();
    }

    /**
     * Reads and returns the decompressed tile data
     * @param absolutePosition tile location (offset/length)
     * @throws UncheckedIOException
     * @return the tile contents
     */
    private ByteBuffer readTile(ByteRange absolutePosition) {
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
        if (parsedMetadata == null) {
            parsedMetadata = parseMetadata(getMetadataAsString());
        }
        return parsedMetadata;
    }

    static PMTilesMetadata parseMetadata(String jsonMetadata) throws IOException {
        if (jsonMetadata == null || jsonMetadata.isBlank()) {
            // Return empty metadata if no metadata is present
            return PMTilesMetadata.of(null);
        }

        try {
            return objectMapper.readValue(jsonMetadata, PMTilesMetadata.class);
        } catch (Exception e) {
            throw new IOException("Failed to parse PMTiles metadata JSON: " + e.getMessage() + "\n" + jsonMetadata, e);
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
        return searchDirectory(ByteRange.of(rootDirOffset, rootDirLength), tileId);
    }

    /**
     * Recursively searches directories for a tile entry.
     *
     * @param dirOffset the offset of the directory to search
     * @param dirLength the length of the directory data
     * @param tileId the tile ID to find
     * @return the absolute tile location if found, or empty if not found
     * @throws IOException if an I/O error occurs
     * @throws UnsupportedCompressionException if the compression type is not supported
     */
    private Optional<ByteRange> searchDirectory(ByteRange entryRange, final long tileId)
            throws IOException, UnsupportedCompressionException {

        PMTilesDirectory entries = getDirectory(entryRange);

        // Find entry that might contain our tileId
        Optional<PMTilesEntry> entry = findEntryForTileId(entries, tileId);

        if (entry.isEmpty()) {
            return Optional.empty();
        }

        PMTilesEntry found = entry.get();

        if (found.isLeaf()) {
            // Recursively search the leaf directory
            return searchDirectory(header.leafDirDataRange(found), tileId);
        } else {
            // This is a tile entry
            return Optional.of(found).map(header::tileDataRange);
        }
    }

    private PMTilesDirectory getDirectory(ByteRange directoryRange)
            throws IOException, UnsupportedCompressionException {
        try {
            return directoryCache.get(directoryRange);
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    /**
     * {@link #directoryCache} loading function
     */
    private PMTilesDirectory readDirectory(ByteRange directoryRange) {
        // Read and deserialize directory
        try {
            long start = System.nanoTime();
            ByteBuffer dirBytes = readDirectoryBytes(directoryRange);
            long read = System.nanoTime() - start;

            PMTilesDirectory directory = DirectoryUtil.deserializeDirectory(dirBytes);

            long decode = System.nanoTime() - read - start;
            if (log.isDebugEnabled()) {
                log.debug("--> PMTilesDirectory lookup: [%,d +%,d], read: %,dms, decode: %,dms: %s"
                        .formatted(
                                directoryRange.offset(),
                                directoryRange.length(),
                                Duration.ofNanos(read).toMillis(),
                                Duration.ofNanos(decode).toMillis(),
                                directory));
            }
            return directory;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Searches for a directory entry that contains the specified tile ID using binary search.
     * This method handles both regular tile entries (with run lengths) and leaf directory entries.
     *
     * @param entries the directory entries to search (must be sorted by tileId)
     * @param tileId the tile ID to search for
     * @return the directory entry that contains the tile, or empty if no suitable entry found
     */
    private Optional<PMTilesEntry> findEntryForTileId(PMTilesDirectory entries, long tileId) {
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
     * @param entries the directory entries that was searched
     * @param insertionPoint the index where the target would be inserted (from binary search)
     * @param tileId the tile ID we're searching for
     * @return the containing entry if found, or empty if no suitable entry exists
     */
    private Optional<PMTilesEntry> findContainingEntry(PMTilesDirectory entries, int insertionPoint, long tileId) {
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

    /**
     * Recursively collects all tile indices at the specified zoom level from the directory structure.
     *
     * @param entryRange the directory entry range to search
     * @param targetZoomLevel the zoom level to collect tiles for
     * @param tileIndices the list to collect tile indices into
     * @param isLeafDir whether this is a leaf directory
     * @throws IOException if an I/O error occurs
     * @throws UnsupportedCompressionException if the compression type is not supported
     */
    private void collectTileIndicesForZoomLevel(
            ByteRange entryRange, int targetZoomLevel, List<TileIndex> tileIndices, boolean isLeafDir)
            throws IOException, UnsupportedCompressionException {

        PMTilesDirectory entries = getDirectory(entryRange);

        for (PMTilesEntry entry : entries) {
            if (entry.isLeaf()) {
                // Recursively search the leaf directory
                collectTileIndicesForZoomLevel(header.leafDirDataRange(entry), targetZoomLevel, tileIndices, true);
            } else {
                // This is a tile entry - check if it's at our target zoom level
                TileIndex tileCoord = HilbertCurve.tileIdToTileIndex(entry.tileId());
                if (tileCoord.z() == targetZoomLevel) {
                    // Add the tile and any tiles in its run
                    for (int i = 0; i < entry.runLength(); i++) {
                        TileIndex currentTile = HilbertCurve.tileIdToTileIndex(entry.tileId() + i);
                        if (currentTile.z() == targetZoomLevel) {
                            tileIndices.add(currentTile);
                        }
                    }
                }
            }
        }
    }

    private ByteBuffer readData(ByteRange range, byte compression) {
        ByteBuffer buffer = ByteBufferPool.getDefault().borrowHeap(range.length());
        try (SeekableByteChannel channel = channelSupplier.get()) {
            channel.position(range.offset());
            channel.read(buffer);
            buffer.flip();
            return CompressionUtil.decompress(buffer, compression);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            ByteBufferPool.getDefault().returnBuffer(buffer);
        }
    }

    static String toString(ByteBuffer byteBuffer) {
        try {
            return IOUtils.toString(new ByteBufferInputStream(byteBuffer), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
