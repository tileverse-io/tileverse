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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tileverse.cache.CacheManager;
import io.tileverse.cache.CacheStats;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.StorageFactory;
import io.tileverse.storage.cache.CachingRangeReader;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestMethodOrder;
import org.threeten.bp.Instant;

@Slf4j
@Disabled
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PMTilesReaderBigFileIT {

    private CacheManager cacheManager;
    private String testName;
    private ForkJoinPool forkJoinPool;

    @BeforeEach
    void setup(TestInfo testInfo) {
        this.testName = testInfo.getDisplayName();
        this.cacheManager = CacheManager.newInstance();
        forkJoinPool = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
    }

    @AfterEach
    void tearDown() {
        Map<String, CacheStats> stats = this.cacheManager.stats();
        stats.forEach((name, cacheStats) -> log.debug("\t{}: {}", name, cacheStats));
        this.cacheManager.invalidateAll();
        forkJoinPool.shutdownNow();
        System.gc();
    }

    static final record TileIndexCount(long dirEntries, long tileEntries, long runLengths, long tileIndices) {

        TileIndexCount() {
            this(0, 0, 0, 0);
        }

        TileIndexCount add(TileIndexCount tc) {
            return new TileIndexCount(
                    this.dirEntries() + tc.dirEntries(),
                    this.tileEntries() + tc.tileEntries(),
                    this.runLengths() + tc.runLengths(),
                    this.tileIndices() + tc.tileIndices());
        }
    }

    static final record TileCount(long tileDataCount, long tileDataSize) {

        TileCount() {
            this(0, 0);
        }

        TileCount add(TileCount tc) {
            return new TileCount(this.tileDataCount() + tc.tileDataCount(), this.tileDataSize() + tc.tileDataSize());
        }
    }

    private PMTilesReader fileRangeReader() throws IOException {
        return pmtilesReader(openRangeReader(
                Paths.get("/Volumes/geodata/pmtiles/protomaps/v4.pmtiles").toUri()));
    }

    private RangeReader httpRangeReader() throws IOException {
        return openRangeReader(URI.create("http://localhost:10000/pmtiles/protomaps/v4.pmtiles"));
    }

    /**
     * Open a parent {@link io.tileverse.storage.Storage} for the URI's container and return a {@link RangeReader} for
     * the leaf, bundled so closing the reader releases the Storage too.
     */
    private static RangeReader openRangeReader(URI leaf) throws IOException {
        String full = leaf.toString();
        int lastSlash = full.lastIndexOf('/');
        URI parent = URI.create(full.substring(0, lastSlash + 1));
        io.tileverse.storage.Storage storage = StorageFactory.open(parent);
        try {
            RangeReader inner = storage.openRangeReader(leaf);
            return new RangeReader() {
                @Override
                public int readRange(long offset, int length, ByteBuffer target) {
                    return inner.readRange(offset, length, target);
                }

                @Override
                public java.util.OptionalLong size() {
                    return inner.size();
                }

                @Override
                public String getSourceIdentifier() {
                    return inner.getSourceIdentifier();
                }

                @Override
                public void close() throws IOException {
                    try {
                        inner.close();
                    } finally {
                        storage.close();
                    }
                }
            };
        } catch (RuntimeException e) {
            try {
                storage.close();
            } catch (IOException ignored) {
                // best-effort
            }
            throw e;
        }
    }

    @SuppressWarnings("resource")
    private PMTilesReader pmtilesReader(RangeReader rangeReader) throws IOException {
        return new PMTilesReader(rangeReader).cacheManager(cacheManager);
    }

    private RangeReader caching(RangeReader rangeReader) {
        return CachingRangeReader.builder(rangeReader)
                .cacheManager(cacheManager)
                .build();
    }

    private PMTilesReader httpReader() throws IOException {
        RangeReader http = httpRangeReader();
        return pmtilesReader(http);
    }

    @Test
    @Order(1)
    void traverseAllFile() throws IOException {
        try (PMTilesReader reader = fileRangeReader()) {
            traverseAllSequentially(reader, 2);
        }
    }

    @Test
    @Order(2)
    void traverseAllHttp() throws IOException {
        try (PMTilesReader reader = httpReader()) {
            traverseAllSequentially(reader, 2);
        }
    }

    @Test
    @Order(3)
    void traverseAllHttpCaching() throws IOException {
        try (PMTilesReader reader = pmtilesReader(caching(httpRangeReader()))) {
            traverseAllSequentially(reader, 2);
        }
    }

    @Test
    @Order(4)
    void traverseAllFileConcurrently() throws IOException {
        try (PMTilesReader reader = fileRangeReader()) {
            traverseAllConcurrently(reader, 2);
        }
    }

    @Test
    @Order(5)
    void traverseAllHttpConcurrently() throws IOException {
        try (PMTilesReader reader = httpReader()) {
            traverseAllConcurrently(reader, 2);
        }
    }

    @Test
    @Order(6)
    void traverseAllHttpCachingConcurrently() throws IOException {
        try (PMTilesReader reader = pmtilesReader(caching(httpRangeReader()))) {
            traverseAllConcurrently(reader, 2);
        }
    }

    private void traverseAllSequentially(PMTilesReader reader, int count) {
        log.debug("----- {}({}) -----", testName, count);
        log.debug("\t{}", reader.getHeader());
        IntStream.range(0, count).forEach(i -> traverseAllIndices(reader));
    }

    private void traverseAllConcurrently(PMTilesReader reader, int concurrency) {
        log.debug("----- {}({}) -----", testName, concurrency);
        log.debug("\t{}", reader.getHeader());
        @SuppressWarnings("rawtypes")
        CompletableFuture[] all = IntStream.range(0, concurrency)
                .mapToObj(i -> CompletableFuture.runAsync(() -> traverseAllIndices(reader)))
                .toArray(CompletableFuture[]::new);
        CompletableFuture.allOf(all).join();
    }

    private void traverseAllIndices(PMTilesReader reader) {
        final Runtime runtime = Runtime.getRuntime();
        final long memInitial = runtime.totalMemory() - runtime.freeMemory();
        final var start = Instant.now();

        PMTilesDirectory rootDirectory = reader.getRootDirectory();
        TileIndexCount tileCount = traverseDirectory(reader, rootDirectory);

        final var end = Instant.now();
        assertNotNull(tileCount);

        final Duration ellapsed = Duration.ofMillis(end.toEpochMilli() - start.toEpochMilli());
        long memFinal = runtime.totalMemory() - runtime.freeMemory();
        log.debug("""
                \tDirectories: %,d, Tile entries: %,d, Tile indices: %,d, runLengths: %,d, traversal: %s
                \tMemory initial: %,dMB, final: %,dMB""".formatted(
                        tileCount.dirEntries(),
                        tileCount.tileEntries(),
                        tileCount.tileIndices(),
                        tileCount.runLengths(),
                        ellapsed,
                        memInitial / (1024 * 1024),
                        memFinal / (1024 * 1024)));

        Map<String, CacheStats> stats = CacheManager.getDefault().stats();
        stats.forEach((name, s) -> log.debug("{}: {}", name, s));

        assertThat(tileCount.runLengths()).isEqualTo(tileCount.tileIndices());
    }

    private TileIndexCount traverseDirectory(PMTilesReader reader, PMTilesDirectory directory) {

        TileIndexCount directoryTilesCount = traverseTileIndices(reader, directory);

        TileIndexCount subdirectoriesCount = directory
                .directoryEntries()
                // .limit(1)
                .parallel()
                .map(reader::getDirectory)
                .map(subdir -> traverseDirectory(reader, subdir))
                .reduce(new TileIndexCount(), TileIndexCount::add);

        TileIndexCount reduced = subdirectoriesCount;
        return reduced.add(directoryTilesCount);
    }

    private TileIndexCount traverseTileIndices(PMTilesReader reader, PMTilesDirectory directory) {
        final long directoryEntryCount = directory.numDirectoryEntries();
        final long tileEntryCount = directory.numTileEntries();

        final LongAdder runLengthSum = new LongAdder();
        final LongAdder tileIndexCount = new LongAdder();

        directory.tileEntries().parallel().forEach(tileEntry -> {
            runLengthSum.add(tileEntry.runLength());
            reader.getTileIndices(tileEntry, tileIndex -> {
                tileIndexCount.add(1);
                // test TileIndex to tileId too
                long tileId = reader.getTileId(tileIndex);
                assertTrue(tileEntry.contains(tileId));
            });
        });

        return new TileIndexCount(
                directoryEntryCount, tileEntryCount, runLengthSum.longValue(), tileIndexCount.longValue());
    }
}
