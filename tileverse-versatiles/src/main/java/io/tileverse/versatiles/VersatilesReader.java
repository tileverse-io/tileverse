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

import static java.util.Objects.requireNonNull;

import io.tileverse.cache.CacheManager;
import io.tileverse.io.ByteRange;
import io.tileverse.io.IOFunction;
import io.tileverse.jackson.databind.versatiles.v2.VersatilesMetadata;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageConfig;
import io.tileverse.storage.StorageFactory;
import io.tileverse.tiling.pyramid.TileIndex;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Optional;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

/**
 * Reads tiles and metadata from a Versatiles 2.0 container through a {@link RangeReader}: a local file, an HTTP server,
 * or an object in S3, Azure, or GCS.
 *
 * <p>{@link #open(URI)} opens the {@link Storage} of the parent location and a {@link RangeReader} for the file, and
 * {@link #close()} closes both. {@link #VersatilesReader(RangeReader)} takes a range reader opened by the caller and
 * closes it in {@link #close()}.
 *
 * <p>Opening a reader reads the header, the metadata, and the block index. A tile then costs one range request, plus
 * one for the tile index of its block when that index is not cached. The reader is safe for use by concurrent threads.
 *
 * <pre>{@code
 * try (VersatilesReader reader = VersatilesReader.open(URI.create("s3://bucket/tiles.versatiles"))) {
 *     Optional<ByteBuffer> tile = reader.getTile(TileIndex.zxy(10, 885, 412));
 * }
 * }</pre>
 */
@NullMarked
public class VersatilesReader implements AutoCloseable {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Nullable
    private final Storage ownedStorage;

    private final RangeReader rangeReader;

    private final BlobReader blobReader;

    private final VersatilesHeader header;

    private final VersatilesMetadata metadata;

    private final BlockIndex blockIndex;

    private final TileIndexCache tileIndexCache;

    /**
     * Opens the archive read by {@code rangeReader}. The new reader owns {@code rangeReader} and closes it in
     * {@link #close()}. If the archive cannot be opened, the constructor closes {@code rangeReader} before it throws.
     *
     * @throws InvalidHeaderException if the file is not a Versatiles 2 container
     * @throws IOException if the metadata or the block index cannot be read
     */
    public VersatilesReader(RangeReader rangeReader) throws IOException {
        this(null, rangeReader);
    }

    private VersatilesReader(@Nullable Storage ownedStorage, RangeReader rangeReader) throws IOException {
        this.ownedStorage = ownedStorage;
        this.rangeReader = requireNonNull(rangeReader, "rangeReader");
        try {
            this.blobReader = new BlobReader(rangeReader);
            this.header = readHeader(rangeReader);
            this.metadata = parseMetadata(readMetadataJson());
            this.blockIndex = readBlockIndex();
            this.tileIndexCache = new TileIndexCache(rangeReader.getSourceIdentifier(), blobReader);
        } catch (IOException | RuntimeException e) {
            closeAfterFailure(rangeReader, e);
            throw e;
        }
    }

    /** Opens the Versatiles file at {@code path}; equivalent to {@code open(path.toUri())}. */
    public static VersatilesReader open(Path path) throws IOException {
        return open(path.toUri());
    }

    /**
     * Opens the Versatiles file at {@code uri} with the default storage configuration; equivalent to {@code open(new
     * StorageConfig(uri))}.
     *
     * @throws IllegalArgumentException if the URI does not include a key segment
     */
    public static VersatilesReader open(URI uri) throws IOException {
        return open(new StorageConfig(uri));
    }

    /**
     * Opens the Versatiles file at the {@link StorageConfig#baseUri() baseUri} of {@code config}. The parameters of
     * {@code config}, such as credentials, endpoints, and timeouts, configure the {@link Storage} opened for the parent
     * location. The returned reader owns that storage and its range reader, and closes both in {@link #close()}.
     *
     * @param config configuration whose {@code baseUri} points at the Versatiles file
     * @return a new {@code VersatilesReader}
     * @throws IllegalArgumentException if {@code config.baseUri()} is null or has no key segment
     * @throws IOException if the file cannot be opened, or is not a Versatiles 2 container
     */
    @SuppressWarnings("java:S2095") // the Storage and its RangeReader are owned and closed by the VersatilesReader
    public static VersatilesReader open(StorageConfig config) throws IOException {
        URI versatilesUri = config.baseUri();
        if (versatilesUri == null) {
            throw new IllegalArgumentException("config.baseUri must be provided");
        }
        URI parent = parentOf(versatilesUri);
        StorageConfig parentConfig = config.copy().baseUri(parent);
        Storage storage = StorageFactory.open(parentConfig);
        try {
            RangeReader reader = storage.openRangeReader(versatilesUri);
            return new VersatilesReader(storage, reader);
        } catch (IOException | RuntimeException e) {
            closeAfterFailure(storage, e);
            throw e;
        }
    }

    /**
     * Returns the parent location of {@code uri}: the URI up to the last slash of its path, without the query and the
     * fragment. A signed URL may hold slashes in its query.
     *
     * @throws IllegalArgumentException if the path of {@code uri} has no key segment
     */
    static URI parentOf(URI uri) {
        String path = uri.getRawPath();
        if (path == null || path.isEmpty() || path.endsWith("/")) {
            throw new IllegalArgumentException("Versatiles URI must include a key: " + uri);
        }
        String full = uri.toString();
        String location = full.split("[?#]", 2)[0];
        int lastSlash = location.lastIndexOf('/');
        return URI.create(location.substring(0, lastSlash + 1));
    }

    private static void closeAfterFailure(Closeable resource, Exception failure) {
        try {
            resource.close();
        } catch (IOException | RuntimeException e) {
            failure.addSuppressed(e);
        }
    }

    /**
     * The versatiles tool writes no block index at all, not even an empty Brotli stream, for an archive without tiles.
     */
    private BlockIndex readBlockIndex() throws IOException {
        ByteRange range = header.blockIndexRange();
        if (range.length() == 0) {
            return BlockIndex.empty();
        }
        return blobReader.readBrotli(range, BlockIndex::parse);
    }

    private static VersatilesHeader readHeader(RangeReader rangeReader) throws IOException {
        try (ReadableByteChannel channel = rangeReader.get()) {
            return VersatilesHeader.deserialize(channel);
        }
    }

    /** Closes the range reader, and the storage when this reader opened it. */
    @Override
    public void close() throws IOException {
        tileIndexCache.invalidateAll();
        try {
            rangeReader.close();
        } finally {
            closeOwnedStorage();
        }
    }

    private void closeOwnedStorage() throws IOException {
        if (ownedStorage != null) {
            ownedStorage.close();
        }
    }

    /**
     * Keeps the tile indexes read by this reader in {@code cacheManager} instead of {@link CacheManager#getDefault()}.
     * Readers of the same archive share entries in the same cache manager.
     *
     * @return this reader
     */
    public VersatilesReader cacheManager(CacheManager cacheManager) {
        tileIndexCache.setCacheManager(cacheManager);
        return this;
    }

    /** Returns the identifier of the file, as reported by its {@link RangeReader}. */
    public String getSourceIdentifier() {
        return rangeReader.getSourceIdentifier();
    }

    /** Returns the header of the archive. */
    public VersatilesHeader getHeader() {
        return header;
    }

    /** Returns the format of the tile bodies. */
    public TileFormat getTileFormat() {
        return header.tileFormat();
    }

    /** Returns the compression of the metadata and of the tile bodies. */
    public Compression getCompression() {
        return header.compression();
    }

    /** Returns the lowest zoom level of the archive. */
    public int getMinZoom() {
        return header.minZoom();
    }

    /** Returns the highest zoom level of the archive. */
    public int getMaxZoom() {
        return header.maxZoom();
    }

    /** Returns the metadata of the archive, parsed when the reader opened it. */
    public VersatilesMetadata getMetadata() {
        return metadata;
    }

    /**
     * Reads the metadata JSON document of the archive, decompressed. Returns an empty string when the archive has no
     * metadata.
     */
    public String getMetadataAsString() throws IOException {
        return readMetadataJson();
    }

    /**
     * Returns the decompressed bytes of the tile at {@code (z, x, y)}; equivalent to {@code getTile(TileIndex.zxy(z, x,
     * y))}.
     */
    public Optional<ByteBuffer> getTile(int z, long x, long y) throws IOException {
        return getTile(TileIndex.zxy(z, x, y));
    }

    /**
     * Returns the decompressed bytes of a tile, or empty if the archive has no tile at {@code tileIndex}. The buffer is
     * ready to read: do not flip it.
     */
    public Optional<ByteBuffer> getTile(TileIndex tileIndex) throws IOException {
        return getTile(tileIndex, in -> ByteBuffer.wrap(in.readAllBytes()));
    }

    /**
     * Reads a tile and returns the result of {@code decoder}, or empty if the archive has no tile at {@code tileIndex}.
     * The stream handed to {@code decoder} holds the decompressed tile and is only valid during the call.
     *
     * @param tileIndex the zoom level, column, and row of the tile, with row 0 at the north
     * @param decoder turns the decompressed tile into the returned value
     */
    public <D> Optional<D> getTile(TileIndex tileIndex, IOFunction<InputStream, D> decoder) throws IOException {
        Optional<ByteRange> location = findTile(tileIndex);
        if (location.isEmpty()) {
            return Optional.empty();
        }
        D tile = blobReader.read(location.get(), header.compression(), decoder);
        return Optional.ofNullable(tile);
    }

    private Optional<ByteRange> findTile(TileIndex tileIndex) throws IOException {
        Optional<BlockIndexEntry> block = blockIndex.findBlock(tileIndex.z(), tileIndex.x(), tileIndex.y());
        if (block.isEmpty()) {
            return Optional.empty();
        }
        return findTileInBlock(block.get(), tileIndex);
    }

    private Optional<ByteRange> findTileInBlock(BlockIndexEntry block, TileIndex tileIndex) throws IOException {
        TileIndexBlob tileIndexBlob = tileIndexCache.getTileIndex(block);
        int slotIndex = block.slotIndex(tileIndex.x(), tileIndex.y());
        Optional<TileIndexBlob.Slot> slot = tileIndexBlob.slot(slotIndex);
        return slot.map(block::tileRange);
    }

    private String readMetadataJson() throws IOException {
        ByteRange range = header.metadataRange();
        if (range.length() == 0) {
            return "";
        }
        return blobReader.read(
                range, header.compression(), in -> new String(in.readAllBytes(), StandardCharsets.UTF_8));
    }

    private static VersatilesMetadata parseMetadata(String json) throws IOException {
        if (json.isBlank()) {
            return VersatilesMetadata.empty();
        }
        try {
            return JSON.readValue(json, VersatilesMetadata.class);
        } catch (JacksonException e) {
            throw new IOException("Failed to parse Versatiles metadata JSON: " + e.getMessage(), e);
        }
    }

    /** Returns the block index, for tests. */
    BlockIndex getBlockIndex() {
        return blockIndex;
    }
}
