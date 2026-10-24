/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.versatiles;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tileverse.cache.CacheManager;
import io.tileverse.jackson.databind.tilejson.v3.VectorLayer;
import io.tileverse.jackson.databind.versatiles.v2.VersatilesMetadata;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageConfig;
import io.tileverse.storage.StorageFactory;
import io.tileverse.storage.TransientStorageException;
import io.tileverse.tiling.pyramid.TileIndex;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongPredicate;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class VersatilesReaderTest {

    @TempDir
    Path tmp;

    Path andorra;

    @BeforeEach
    void copyFixture() throws IOException {
        andorra = VersatilesTestData.andorra(tmp);
    }

    @Test
    void openByPathReadsHeader() throws IOException {
        try (VersatilesReader r = VersatilesReader.open(andorra)) {
            assertThat(r.getTileFormat()).isEqualTo(TileFormat.PBF);
            assertThat(r.getCompression()).isEqualTo(Compression.GZIP);
            assertThat(r.getMinZoom()).isZero();
            assertThat(r.getMaxZoom()).isEqualTo(14);
        }
    }

    @Test
    void openByUriUsesStorageFactory() throws IOException {
        try (VersatilesReader r = VersatilesReader.open(andorra.toUri())) {
            assertThat(r.getTileFormat()).isEqualTo(TileFormat.PBF);
        }
    }

    @Test
    void truncatedArchiveFailsToOpen() throws IOException {
        Path truncated = truncatedCopy();

        assertThatThrownBy(() -> VersatilesReader.open(truncated)).isInstanceOf(IOException.class);
    }

    @Test
    void failedOpenClosesRangeReader() throws IOException {
        Path truncated = truncatedCopy();

        try (Storage storage = StorageFactory.open(tmp.toUri())) {
            CloseRecordingRangeReader reader =
                    new CloseRecordingRangeReader(storage.openRangeReader(truncated.toUri()), null);

            assertThatThrownBy(() -> new VersatilesReader(reader)).isInstanceOf(IOException.class);
            assertThat(reader.closed()).isTrue();
        }
    }

    // The open failure explains the problem; the close failure must not replace it.
    @Test
    void closeFailureIsSuppressed() throws IOException {
        Path truncated = truncatedCopy();
        IOException closeFailure = new IOException("close failed");

        try (Storage storage = StorageFactory.open(tmp.toUri())) {
            CloseRecordingRangeReader reader =
                    new CloseRecordingRangeReader(storage.openRangeReader(truncated.toUri()), closeFailure);

            assertThatThrownBy(() -> new VersatilesReader(reader))
                    .isInstanceOf(IOException.class)
                    .isNotSameAs(closeFailure)
                    .hasSuppressedException(closeFailure);
        }
    }

    // The block index sits at the end of the file: cutting the file damages it.
    private Path truncatedCopy() throws IOException {
        byte[] bytes = Files.readAllBytes(andorra);
        return Files.write(tmp.resolve("truncated.versatiles"), Arrays.copyOf(bytes, bytes.length - 100));
    }

    @Test
    void metadataListsShortbreadLayers() throws IOException {
        try (VersatilesReader r = VersatilesReader.open(andorra)) {
            VersatilesMetadata md = r.getMetadata();
            assertThat(md.name()).isEqualTo("Shortbread");
            List<String> ids = md.vectorLayers().stream().map(VectorLayer::id).toList();
            assertThat(ids).containsExactlyInAnyOrderElementsOf(VersatilesTestData.andorraLayerNames());
        }
    }

    @Test
    void rawMetadataIsValidJson() throws IOException {
        try (VersatilesReader r = VersatilesReader.open(andorra)) {
            String json = r.getMetadataAsString();
            assertThat(json).startsWith("{").endsWith("}").contains("\"vector_layers\"");
        }
    }

    @Test
    void archiveWithoutMetadata() throws IOException {
        Path file = new TestArchiveWriter().tile(0, 0, 0, "world").write(tmp.resolve("bare.versatiles"));

        try (VersatilesReader r = VersatilesReader.open(file)) {
            assertThat(r.getMetadataAsString()).isEmpty();
            assertThat(r.getMetadata().name()).isNull();
            assertThat(r.getMetadata().vectorLayers()).isEmpty();
        }
    }

    // The versatiles tool writes a block index of length zero when a conversion keeps no tiles.
    @Test
    void archiveWithoutTiles() throws IOException {
        Path file = new TestArchiveWriter().write(tmp.resolve("empty.versatiles"));

        try (VersatilesReader r = VersatilesReader.open(file)) {
            assertThat(r.getTile(0, 0, 0)).isEmpty();
        }
    }

    @Test
    void vectorLayersAreReadOnly() throws IOException {
        try (VersatilesReader r = VersatilesReader.open(andorra)) {
            List<VectorLayer> layers = r.getMetadata().vectorLayers();

            assertThatThrownBy(layers::clear).isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Test
    void existingTileHasBytes() throws IOException {
        try (VersatilesReader r = VersatilesReader.open(andorra)) {
            // tile (z=0, x=0, y=0) covers the whole world; Andorra is in there.
            Optional<ByteBuffer> tile = r.getTile(0, 0, 0);
            assertThat(tile).isPresent();
            assertThat(tile.get().remaining()).isPositive();
        }
    }

    // The PMTiles copy of the Andorra fixture holds a tile of 182531 bytes at this position. A reader counting rows
    // from the south would find no tile there.
    @Test
    void rowsCountFromTheNorth() throws IOException {
        try (VersatilesReader r = VersatilesReader.open(andorra)) {
            Optional<ByteBuffer> tile = r.getTile(14, 8261, 6050);
            assertThat(tile).isPresent();
            assertThat(tile.get().remaining()).isEqualTo(182_531);
        }
    }

    @Test
    void getTileEmptyForOutOfRangeZoom() throws IOException {
        try (VersatilesReader r = VersatilesReader.open(andorra)) {
            assertThat(r.getTile(15, 0, 0)).isEmpty();
        }
    }

    @Test
    void tileOutsideArchiveIsEmpty() throws IOException {
        try (VersatilesReader r = VersatilesReader.open(andorra)) {
            // (z=10, x=0, y=0) is somewhere in the Atlantic, not in the Andorra archive.
            assertThat(r.getTile(TileIndex.xyz(0L, 0L, 10))).isEmpty();
        }
    }

    // Column -1 shares block column 0 with column 255 under integer division.
    @Test
    void negativeColumnIsEmpty() throws IOException {
        Path file = new TestArchiveWriter()
                .tile(8, 0, 0, "west")
                .tile(8, 255, 0, "east")
                .write(tmp.resolve("columns.versatiles"));

        try (VersatilesReader r = VersatilesReader.open(file)) {
            assertThat(text(r.getTile(8, 255, 0))).hasValue("east");
            assertThat(r.getTile(8, -1, 0)).isEmpty();
        }
    }

    @Test
    void negativeRowIsEmpty() throws IOException {
        Path file = new TestArchiveWriter()
                .tile(8, 0, 0, "north")
                .tile(8, 0, 255, "south")
                .write(tmp.resolve("rows.versatiles"));

        try (VersatilesReader r = VersatilesReader.open(file)) {
            assertThat(text(r.getTile(8, 0, 255))).hasValue("south");
            assertThat(r.getTile(8, 0, -1)).isEmpty();
        }
    }

    // The tile index of a block lists its tiles row by row; (2, 3) falls in the block range but has no tile.
    @Test
    void tileIndexIsRowMajor() throws IOException {
        Path file = new TestArchiveWriter()
                .tile(3, 1, 2, "a")
                .tile(3, 2, 2, "b")
                .tile(3, 1, 3, "c")
                .write(tmp.resolve("slots.versatiles"));

        try (VersatilesReader r = VersatilesReader.open(file)) {
            assertThat(text(r.getTile(3, 1, 2))).hasValue("a");
            assertThat(text(r.getTile(3, 2, 2))).hasValue("b");
            assertThat(text(r.getTile(3, 1, 3))).hasValue("c");
            assertThat(r.getTile(3, 2, 3)).isEmpty();
        }
    }

    // The block of column 300 starts at column 256, and its tile range starts at (300, 5), where there is no tile
    @Test
    void firstTileSkipsEmptySlots() throws IOException {
        Path file = new TestArchiveWriter()
                .tile(9, 301, 5, "first")
                .tile(9, 300, 6, "second")
                .write(tmp.resolve("first.versatiles"));

        try (VersatilesReader r = VersatilesReader.open(file)) {
            assertThat(r.firstTileIndex()).hasValue(TileIndex.zxy(9, 301, 5));
        }
    }

    @Test
    void noFirstTileWithoutTiles() throws IOException {
        Path file = new TestArchiveWriter().write(tmp.resolve("empty.versatiles"));

        try (VersatilesReader r = VersatilesReader.open(file)) {
            assertThat(r.firstTileIndex()).isEmpty();
        }
    }

    @ParameterizedTest
    @MethodSource("compressions")
    void readsEachCompression(Compression compression) throws IOException {
        Path file = new TestArchiveWriter()
                .compression(compression)
                .metadata("{\"name\":\"compressed\"}")
                .tile(5, 16, 11, "tile body")
                .write(tmp.resolve("compressed.versatiles"));

        try (VersatilesReader r = VersatilesReader.open(file)) {
            assertThat(r.getCompression()).isEqualTo(compression);
            assertThat(r.getMetadata().name()).isEqualTo("compressed");
            assertThat(text(r.getTile(5, 16, 11))).hasValue("tile body");
        }
    }

    static Stream<Compression> compressions() {
        return Stream.of(Compression.NONE, Compression.GZIP, Compression.BROTLI, Compression.ZSTD);
    }

    // The tile at this position is 182531 bytes after decompression, larger than the read buffer of a decompressor.
    @Test
    void tileReadIsOneRequest() throws IOException {
        try (Storage storage = StorageFactory.open(tmp.toUri());
                CountingRangeReader counting = new CountingRangeReader(storage.openRangeReader(andorra.toUri()));
                VersatilesReader r = new VersatilesReader(counting)) {
            r.getTile(14, 8261, 6050);
            counting.reset();

            r.getTile(14, 8261, 6050);

            assertThat(counting.requests()).isEqualTo(1);
        }
    }

    @Test
    void tileIndexesUseCacheManager() throws IOException {
        Path file = new TestArchiveWriter().tile(2, 1, 1, "tile").write(tmp.resolve("cached.versatiles"));
        CacheManager cacheManager = CacheManager.newInstance();

        try (Storage storage = StorageFactory.open(tmp.toUri());
                CountingRangeReader counting = new CountingRangeReader(storage.openRangeReader(file.toUri()));
                VersatilesReader r = new VersatilesReader(counting).cacheManager(cacheManager)) {
            r.getTile(2, 1, 1);
            cacheManager.invalidateAll();
            counting.reset();

            r.getTile(2, 1, 1);

            // one request for the tile index, dropped from the cache manager, and one for the tile
            assertThat(counting.requests()).isEqualTo(2);
        }
    }

    // A caller retrying on TransientStorageException must see that type, not the cause it wraps.
    @Test
    void storageErrorKeepsItsType() throws IOException {
        Path file = new TestArchiveWriter().tile(2, 1, 1, "tile").write(tmp.resolve("failing.versatiles"));
        long tileIndexOffset = VersatilesHeader.HEADER_BYTES + "tile".length();

        try (Storage storage = StorageFactory.open(tmp.toUri());
                FailingRangeReader failing =
                        new FailingRangeReader(storage.openRangeReader(file.toUri()), o -> o == tileIndexOffset);
                VersatilesReader r = new VersatilesReader(failing)) {
            assertThatThrownBy(() -> r.getTile(2, 1, 1)).isInstanceOf(TransientStorageException.class);
        }
    }

    // A signed URL may hold slashes in its query; the parent location ends at the path.
    @ParameterizedTest
    @MethodSource("parentLocations")
    void parentKeepsLocation(String uri, String parent) {
        assertThat(VersatilesReader.parentOf(URI.create(uri))).isEqualTo(URI.create(parent));
    }

    static Stream<Arguments> parentLocations() {
        return Stream.of(
                Arguments.of("https://cdn.example/x/t.versatiles?hdnts=acl=/x/*~hmac=1", "https://cdn.example/x/"),
                Arguments.of("https://host/tiles.versatiles#part", "https://host/"),
                Arguments.of("s3://bucket/dir/tiles.versatiles", "s3://bucket/dir/"),
                Arguments.of("file:///data/tiles.versatiles", "file:///data/"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"s3://bucket", "s3://bucket/dir/", "https://host/?file=/tiles.versatiles"})
    void uriWithoutKeyIsRejected(String uri) {
        URI withoutKey = URI.create(uri);

        assertThatThrownBy(() -> VersatilesReader.parentOf(withoutKey)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void configWithoutUriIsRejected() {
        StorageConfig withoutUri = StorageConfig.withDefaults(List.of());

        assertThatThrownBy(() -> VersatilesReader.open(withoutUri))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("config.baseUri must be provided");
    }

    @Test
    void atLeastOneTilePerZoomLevel() throws IOException {
        // Smoke-test the block-index/tile-index path against the zoom levels present in the archive.
        try (VersatilesReader r = VersatilesReader.open(andorra)) {
            BlockIndex idx = r.getBlockIndex();
            for (int z = r.getMinZoom(); z <= r.getMaxZoom(); z++) {
                final int zoom = z;
                Optional<BlockIndexEntry> any =
                        idx.entries().stream().filter(e -> e.level() == zoom).findFirst();
                assertThat(any).as("expected a block at zoom %d", z).isPresent();
                BlockIndexEntry e = any.get();
                long x = e.blockCol() * 256 + e.colMin();
                long y = e.blockRow() * 256 + e.rowMin();
                Optional<ByteBuffer> tile = r.getTile(z, x, y);
                assertThat(tile).as("tile at z=%d x=%d y=%d", z, x, y).isPresent();
            }
        }
    }

    private static Optional<String> text(Optional<ByteBuffer> tile) {
        return tile.map(buffer -> StandardCharsets.UTF_8.decode(buffer).toString());
    }

    /** Forwards to a range reader; subclasses observe or alter the range reads. */
    private abstract static class ForwardingRangeReader implements RangeReader {

        final RangeReader delegate;

        ForwardingRangeReader(RangeReader delegate) {
            this.delegate = delegate;
        }

        @Override
        public int readRange(long offset, int length, ByteBuffer target) {
            return delegate.readRange(offset, length, target);
        }

        @Override
        public OptionalLong size() {
            return delegate.size();
        }

        @Override
        public String getSourceIdentifier() {
            return delegate.getSourceIdentifier();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    /** Counts the range requests sent to the wrapped reader. */
    private static final class CountingRangeReader extends ForwardingRangeReader {

        private final AtomicInteger requests = new AtomicInteger();

        CountingRangeReader(RangeReader delegate) {
            super(delegate);
        }

        int requests() {
            return requests.get();
        }

        void reset() {
            requests.set(0);
        }

        @Override
        public int readRange(long offset, int length, ByteBuffer target) {
            requests.incrementAndGet();
            return super.readRange(offset, length, target);
        }
    }

    /** Records the call to close, and fails it with {@code closeFailure} when one is given. */
    private static final class CloseRecordingRangeReader extends ForwardingRangeReader {

        private final IOException closeFailure;

        private boolean closed;

        CloseRecordingRangeReader(RangeReader delegate, IOException closeFailure) {
            super(delegate);
            this.closeFailure = closeFailure;
        }

        boolean closed() {
            return closed;
        }

        @Override
        public void close() throws IOException {
            closed = true;
            super.close();
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }

    /** Fails the reads starting at the offsets matched by {@code failAt}, as a timed out remote read would. */
    private static final class FailingRangeReader extends ForwardingRangeReader {

        private final LongPredicate failAt;

        FailingRangeReader(RangeReader delegate, LongPredicate failAt) {
            super(delegate);
            this.failAt = failAt;
        }

        @Override
        public int readRange(long offset, int length, ByteBuffer target) {
            if (failAt.test(offset)) {
                throw new TransientStorageException("Range read failed", new IOException("request timed out"));
            }
            return super.readRange(offset, length, target);
        }
    }
}
