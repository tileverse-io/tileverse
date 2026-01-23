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

import static io.tileverse.pmtiles.CompressionUtil.decompressingInputStream;
import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.tileverse.cache.CacheManager;
import io.tileverse.io.ByteRange;
import io.tileverse.io.IOFunction;
import io.tileverse.jackson.databind.pmtiles.v3.PMTilesMetadata;
import io.tileverse.rangereader.RangeReader;
import io.tileverse.rangereader.file.FileRangeReader;
import io.tileverse.tiling.pyramid.TileIndex;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

/**
 * Reader for PMTiles files that provides access to tiles and metadata.
 * <p>
 * This class implements the PMTiles format specification, providing a clean API
 * for accessing tile data, metadata, and directory structures within a PMTiles
 * file.
 * <p>
 * It relies on a {@code Supplier<SeekableByteChannel>}, from which it will
 * <strong>acquire and close</strong> a {@link SeekableByteChannel} upon each
 * I/O operation.
 * <p>
 * Therefore this reader does not own the underlying input source and won't
 * close it explicitly unless it provides a new instance on each supplied byte
 * channel.
 * <p>
 * It is recommended to use the {@link RangeReader} library for random access to
 * the underlying data source, allowing for efficient reading from local files,
 * HTTP servers, or cloud storage; though it's not mandatory.
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
@NullMarked
public class PMTilesReader implements AutoCloseable {

    private final HilbertCurve hilbertCurve = new HilbertCurve();

    @Nullable
    private RangeReader rangeReader;

    private final Supplier<SeekableByteChannel> channelSupplier;

    private final String pmtilesUri;

    private final DirectoryCache directoryCache;

    private final PMTilesHeader header;

    private final PMTilesMetadata parsedMetadata;

    /**
     * Creates a new PMTilesReader for the specified file.
     * <p>
     * This constructor creates a {@code Supplier<SeekableByteChannel>} that will
     * open and close the file upon each I/O operation.
     *
     * @param path the path to the PMTiles file
     * @throws IOException            if an I/O error occurs
     * @throws InvalidHeaderException if the file has an invalid header
     */
    public PMTilesReader(Path path) throws IOException {
        this(FileRangeReader.of(path));
    }

    /**
     * Creates a new PMTilesReader using the specified RangeReader.
     * <p>
     * This constructor allows reading PMTiles from any source that implements the
     * RangeReader interface, such as local files, HTTP servers, or cloud storage.
     * <p>
     * Note the {@code rangeReader} is owned by this instance and will be {@link RangeReader#close() closed}
     * when {@link #close()} is called on this instance.
     *
     * @param rangeReader range reader to read data from
     * @throws IOException            if an I/O error occurs
     * @throws InvalidHeaderException if the file has an invalid header
     */
    public PMTilesReader(RangeReader rangeReader) throws IOException {
        this(rangeReader.getSourceIdentifier(), rangeReader::asByteChannel);
        this.rangeReader = rangeReader;
    }

    /**
     * Creates a new PMTilesReader using the specified {@code channelSupplier}
     * <p>
     * This constructor allows reading PMTiles from any source providing a
     * {@link SeekableByteChannel} for each read operation.
     * <p>
     * Usually you'd use {@link RangeReader#asByteChannel() RangeReader::asByteChannel)}, though this constructor
     * allows to use other data sources that don't implement {@code RangeReader}.
     *
     * @param pmtilesUri      a unique identifier for the PMTiles source (e.g., file path or URI).
     *                        This identifier is crucial for caching, as it allows the internal
     *                        {@link CacheManager} to share cache entries across multiple reader instances
     *                        pointing to the same resource.
     * @param channelSupplier supplier of short-lived {@link SeekableByteChannel}s
     *                        to use on each read operation
     * @throws IOException            if an I/O error occurs
     * @throws InvalidHeaderException if the file has an invalid header
     */
    public PMTilesReader(String pmtilesUri, Supplier<SeekableByteChannel> channelSupplier) throws IOException {
        requireNonNull(pmtilesUri);
        this.channelSupplier = requireNonNull(channelSupplier, "rangeReaderSupplier cannot be null");
        this.header = PMTilesReader.readHeader(channelSupplier);
        this.directoryCache = new DirectoryCache(pmtilesUri, header, channelSupplier);
        this.pmtilesUri = pmtilesUri;
        this.parsedMetadata = parseMetadata();
    }

    static PMTilesHeader readHeader(Supplier<? extends ReadableByteChannel> channelSupplier) throws IOException {
        try (ReadableByteChannel channel = channelSupplier.get()) {
            return PMTilesHeader.deserialize(channel);
        }
    }

    @Override
    public void close() throws IOException {
        if (rangeReader != null) {
            rangeReader.close();
        }
        directoryCache.invalidateAll();
    }

    public PMTilesReader cacheManager(CacheManager cacheManager) {
        directoryCache.setCacheManager(cacheManager);
        return this;
    }

    io.tileverse.cache.CacheStats cacheStats() {
        return directoryCache.stats();
    }

    /**
     * Returns the unique identifier for this PMTiles source.
     *
     * @return the unique URI or identifier provided at construction time.
     */
    public String getSourceIdentifier() {
        return pmtilesUri;
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
     * Computes the tile id for a given tile index
     *
     * @param tileIndex the tile coordinates.
     * @return the scalar PMTiles ID.
     * @throws IllegalArgumentException if the zoom level > 26 or x/y are out of bounds.
     */
    public long getTileId(TileIndex tileIndex) {
        requireNonNull(tileIndex, "tileIndex is null");
        if (tileIndex.z() < 0) throw new IllegalArgumentException("z can't be < 0");
        if (tileIndex.x() < 0) throw new IllegalArgumentException("x can't be < 0");
        if (tileIndex.y() < 0) throw new IllegalArgumentException("y can't be < 0");
        return hilbertCurve.tileIndexToTileId(tileIndex);
    }

    /**
     * Decodes a scalar PMTiles Tile ID into (z, x, y) coordinates.
     *
     * @param tileId the global PMTiles identifier (must be positive).
     * @return the decoded {@link TileIndex}.
     * @throws IllegalArgumentException if {@code tileId} is negative or exceeds the max zoom limit.
     */
    public TileIndex getTileIndex(long tileId) {
        return hilbertCurve.tileIdToTileIndex(tileId);
    }

    /**
     * Computes the list of {@link TileIndex} for an entry, expending
     * {@link PMTilesEntry#tileId() tileId} {@link PMTilesEntry#runLength()
     * runLength} times.
     * @throws IllegalArgumentException if {@code tileEntry} is a {@link PMTilesEntry#isLeaf() leaf} directory
     * @see #getTileIndex(long)
     */
    public List<TileIndex> getTileIndices(PMTilesEntry tileEntry) {
        if (tileEntry.runLength() == 1) {
            return List.of(getTileIndex(tileEntry.tileId()));
        }
        List<TileIndex> indices = new ArrayList<>(tileEntry.runLength());
        getTileIndices(tileEntry, indices::add);
        return indices;
    }

    public void getTileIndices(PMTilesEntry tileEntry, Consumer<TileIndex> consumer) {
        requireNonNull(tileEntry);
        requireNonNull(consumer);
        if (tileEntry.isLeaf()) {
            throw new IllegalArgumentException("entry shall be a tiles entry: " + tileEntry);
        }
        tileEntry.forEachId(tileId -> {
            TileIndex tileIndex = getTileIndex(tileId);
            consumer.accept(tileIndex);
        });
    }

    /**
     * Gets a tile by its TileIndex.
     * <p>
     * This is a shortcut for {@link #getTile(long) getTile(getTileId(tileIndex))}.
     *
     * @param tileIndex the tile coordinates
     * @return an Optional containing the tile data as a ByteBuffer if found, or
     *         empty if not
     * @throws IOException if an I/O error occurs
     */
    public Optional<ByteBuffer> getTile(TileIndex tileIndex) throws IOException {
        return getTile(getTileId(tileIndex));
    }

    /**
     * Gets a tile by its tile id.
     * @param tileId the scalar PMTiles ID
     * @return an Optional containing the tile data as a ByteBuffer if found, or
     *         empty if not
     * @throws IOException if an I/O error occurs
     */
    public Optional<ByteBuffer> getTile(final long tileId) throws IOException {
        IOFunction<InputStream, ByteBuffer> f = in -> ByteBuffer.wrap(in.readAllBytes());
        return getTile(tileId, f);
    }

    public <D> Optional<D> getTile(final long tileId, IOFunction<InputStream, D> mapper) throws IOException {
        try {
            Optional<ByteRange> tileLocation = findTileLocation(tileId);
            return tileLocation.map(range -> loadTile(range, mapper));
        } catch (UncheckedIOException uioe) {
            throw uioe.getCause();
        }
    }

    private <D> D loadTile(ByteRange range, IOFunction<InputStream, D> mapper) {
        final byte compression = header.tileCompression();

        try (SeekableByteChannel channel = channel();
                InputStream in = decompressingInputStream(channel, range, compression)) {
            return mapper.apply(in);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Stream<TileIndex> getTileIndices() {
        IntStream zooms = IntStream.rangeClosed(header.minZoom(), header.maxZoom());
        return zooms.mapToObj(Integer::valueOf).flatMap(this::getTileIndicesByZoomLevel);
    }

    /**
     * Returns a stream of all tile indices present in the PMTiles file at the
     * specified zoom level.
     * <p>
     * This method traverses the sparse directory structure of the PMTiles file and
     * collects all tiles that exist at the given zoom level. Unlike a continuous
     * TileMatrix grid, PMTiles files contain only the tiles that were actually
     * written to the file.
     * <p>
     * The returned stream provides an efficient way to iterate over all tiles at a
     * zoom level without having to test each possible tile coordinate for
     * existence.
     *
     * @param zoomLevel the zoom level to query (0-based)
     * @return a stream of TileIndex objects representing all tiles present at the
     *         zoom level
     * @throws UncheckedIOException if an I/O error occurs while reading the
     *                              directory structure
     */
    public Stream<TileIndex> getTileIndicesByZoomLevel(int zoomLevel) {
        if (zoomLevel < 0 || zoomLevel > 31) {
            throw new IllegalArgumentException("Zoom level must be between 0 and 31, got: " + zoomLevel);
        }

        List<TileIndex> tileIndices = new java.util.ArrayList<>();
        try {
            ByteRange entryRange = header.rootDirectory();
            collectTileIndicesForZoomLevel(entryRange, zoomLevel, tileIndices);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return tileIndices.stream();
    }

    /**
     * Gets the metadata as a parsed JSON string.
     *
     * @return the metadata as a JSON string
     * @throws IOException                     if an I/O error occurs
     * @throws UnsupportedCompressionException if the compression type is not
     *                                         supported
     */
    public String getMetadataAsString() throws IOException {
        final ByteRange metadataRange = header.jsonMetadata();
        final byte compression = header.internalCompression();

        try (SeekableByteChannel channel = channel();
                InputStream in = decompressingInputStream(channel, metadataRange, compression)) {
            return IOUtils.toString(in, StandardCharsets.UTF_8);
        }
    }

    /**
     * Gets the metadata as a parsed {@link PMTilesMetadata} object.
     * <p>
     * This provides structured access to the metadata fields with proper type
     * conversion.
     *
     * @return the metadata as a PMTilesMetadata object
     */
    public PMTilesMetadata getMetadata() {
        return parsedMetadata;
    }

    /**
     * @throws IOException                     if an I/O error occurs or JSON
     *                                         parsing fails
     * @throws UnsupportedCompressionException if the compression type is not
     *                                         supported
     */
    private PMTilesMetadata parseMetadata() throws IOException {
        String jsonMetadata = getMetadataAsString();
        return parseMetadata(jsonMetadata);
    }

    static PMTilesMetadata parseMetadata(String jsonMetadata) throws IOException {
        if (jsonMetadata.isBlank()) {
            // Return empty metadata if no metadata is present
            return PMTilesMetadata.empty();
        }
        try {
            final ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(jsonMetadata, PMTilesMetadata.class);
        } catch (Exception e) {
            throw new IOException("Failed to parse PMTiles metadata JSON: " + e.getMessage() + "\n" + jsonMetadata, e);
        }
    }

    /**
     * Finds the location of a tile in the PMTiles file using recursive directory
     * traversal.
     *
     * @param tileId the ID of the tile to find
     * @return the location of the tile, or empty if the tile doesn't exist
     * @throws IOException                     if an I/O error occurs
     * @throws UnsupportedCompressionException if the compression type is not
     *                                         supported
     */
    private Optional<ByteRange> findTileLocation(long tileId) throws IOException {
        return searchDirectory(header.rootDirectory(), tileId);
    }

    /**
     * Recursively searches directories for a tile entry.
     *
     * @param dirOffset the offset of the directory to search
     * @param dirLength the length of the directory data
     * @param tileId    the tile ID to find
     * @return the absolute tile location if found, or empty if not found
     * @throws IOException                     if an I/O error occurs
     * @throws UnsupportedCompressionException if the compression type is not
     *                                         supported
     */
    private Optional<ByteRange> searchDirectory(ByteRange dirEntryRange, final long tileId) throws IOException {

        final PMTilesDirectory dirEntry = directoryCache.getDirectory(dirEntryRange);

        // Find entry that might contain our tileId
        Optional<PMTilesEntry> entry = findEntryForTileId(dirEntry, tileId);

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

    /**
     * Searches for a directory entry that contains the specified tile ID using
     * binary search. This method handles both regular tile entries (with run
     * lengths) and leaf directory entries.
     *
     * @param entries the directory entries to search (must be sorted by tileId)
     * @param tileId  the tile ID to search for
     * @return the directory entry that contains the tile, or empty if no suitable
     *         entry found
     */
    private Optional<PMTilesEntry> findEntryForTileId(PMTilesDirectory entries, final long tileId) {
        int low = 0;
        int high = entries.size() - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            PMTilesEntry entry = entries.getEntry(mid);

            if (tileId < entry.tileId()) {
                high = mid - 1;
            } else if (entry.isLeaf()) {
                // For leaf entries (entries with no run length), match exact tileId
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
     * Attempts to find an entry that contains the target tileId at the binary
     * search insertion point.
     *
     * <p>
     * This method handles the PMTiles format's range-based entries where a single
     * directory entry can represent multiple consecutive tiles. After a binary
     * search fails to find an exact match, the entry just before the insertion
     * point might still contain our target tile.
     *
     * <p>
     * Two cases are handled:
     * <ul>
     * <li><b>Regular entries with run lengths</b>: An entry with tileId=100 and
     * runLength=5 represents tiles 100-104. If searching for tileId=102, this entry
     * should be returned even though 102 != 100, because 102 falls within the range
     * [100, 104].</li>
     * <li><b>Leaf directory entries</b>: These point to subdirectories that might
     * contain the target tile. Since we don't know the exact bounds of leaf
     * directories, we return the closest leaf entry as a heuristic - it might
     * contain our target in its subdirectory.</li>
     * </ul>
     *
     * @param entries        the directory entries that was searched
     * @param insertionPoint the index where the target would be inserted (from
     *                       binary search)
     * @param tileId         the tile ID we're searching for
     * @return the containing entry if found, or empty if no suitable entry exists
     */
    private Optional<PMTilesEntry> findContainingEntry(
            PMTilesDirectory entries, final int insertionPoint, final long tileId) {
        if (insertionPoint < 0) {
            return Optional.empty();
        }

        PMTilesEntry candidate = entries.getEntry(insertionPoint);

        if (candidate.isLeaf()) {
            // Return leaf directory - it might contain our tile in its subdirectory
            return Optional.of(candidate);
        } else if (candidate.runLength() > 0) {
            // Check if tileId falls within the entry's run range [tileId, tileId +
            // runLength - 1]
            long rangeEnd = candidate.tileId() + candidate.runLength() - 1;
            if (tileId <= rangeEnd) {
                return Optional.of(candidate);
            }
        }

        return Optional.empty();
    }

    public PMTilesDirectory getRootDirectory() {
        try {
            return directoryCache.getRootDirectory();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public PMTilesDirectory getDirectory(PMTilesEntry entry) {
        try {
            return directoryCache.getDirectory(entry);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Recursively collects all tile indices at the specified zoom level from the
     * directory structure.
     *
     * @param entryRange      the directory entry range to search
     * @param targetZoomLevel the zoom level to collect tiles for
     * @param tileIndices     the list to collect tile indices into
     * @throws IOException                     if an I/O error occurs
     * @throws UnsupportedCompressionException if the compression type is not
     *                                         supported
     */
    private void collectTileIndicesForZoomLevel(ByteRange entryRange, int targetZoomLevel, List<TileIndex> tileIndices)
            throws IOException {

        PMTilesDirectory entries = directoryCache.getDirectory(entryRange);

        for (PMTilesEntry entry : entries) {
            if (entry.isLeaf()) {
                // Recursively search the leaf directory
                collectTileIndicesForZoomLevel(header.leafDirDataRange(entry), targetZoomLevel, tileIndices);
            } else {
                // This is a tile entry - check if it's at our target zoom level
                TileIndex tileCoord = hilbertCurve.tileIdToTileIndex(entry.tileId());
                if (tileCoord.z() == targetZoomLevel) {
                    // Add the tile and any tiles in its run
                    for (int i = 0; i < entry.runLength(); i++) {
                        TileIndex currentTile = hilbertCurve.tileIdToTileIndex(entry.tileId() + i);
                        if (currentTile.z() == targetZoomLevel) {
                            tileIndices.add(currentTile);
                        }
                    }
                }
            }
        }
    }

    private SeekableByteChannel channel() throws IOException {
        try {
            return channelSupplier.get();
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }
}
