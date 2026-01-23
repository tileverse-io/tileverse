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
import io.tileverse.rangereader.RangeReader;
import io.tileverse.rangereader.cache.CachingRangeReader;
import io.tileverse.rangereader.file.FileRangeReader;
import io.tileverse.rangereader.http.HttpRangeReader;
import io.tileverse.tiling.pyramid.TileIndex;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestMethodOrder;
import org.threeten.bp.Instant;

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
        stats.forEach((name, cacheStats) -> System.err.printf("\t%s: %s%n", name, cacheStats));
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
        RangeReader file = FileRangeReader.of(Paths.get("/Users/groldan/wip/pmtiles/data/pmtiles.io/v4.pmtiles"));
        return pmtilesReader(file);
    }

    private RangeReader httpRangeReader() {
        return HttpRangeReader.of("http://localhost:10000/pmtiles.io/v4.pmtiles");
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
        System.err.printf("----- %s(%d) -----%n", testName, count);
        System.err.println("\t" + reader.getHeader());
        IntStream.range(0, count).forEach(i -> traverseAllIndices(reader));
    }

    private void traverseAllConcurrently(PMTilesReader reader, int concurrency) {
        System.err.printf("----- %s(%d) -----%n", testName, concurrency);
        System.err.println("\t" + reader.getHeader());
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
        System.err.printf(
                """
				\tDirectories: %,d, Tile entries: %,d, Tile indices: %,d, runLengths: %,d, traversal: %s
				\tMemory initial: %,dMB, final: %,dMB%n
				""",
                tileCount.dirEntries(),
                tileCount.tileEntries(),
                tileCount.tileIndices(),
                tileCount.runLengths(),
                ellapsed,
                memInitial / (1024 * 1024),
                memFinal / (1024 * 1024));

        Map<String, CacheStats> stats = CacheManager.getDefault().stats();
        stats.forEach((name, s) -> System.err.printf("%s: %s%n", name, s));

        assertThat(tileCount.runLengths()).isEqualTo(tileCount.tileIndices());
        //        System.err.printf(
        //                """
        //				\tDirectories: %,d, Tile entries: %,d, Tile indices: %,d, runLengths: %,d, Data tiles: %,d, Data size:
        // %.2fGB, traversal: %s
        //				\tMemory initial: %,dMB, final: %,dMB%n
        //				""",
        //                tileCount.dirEntries(),
        //                tileCount.tileEntries(),
        //                tileCount.tileIndices(),
        //                tileCount.runLengths(),
        //                ellapsed,
        //                memInitial / (1024 * 1024),
        //                memFinal / (1024 * 1024));
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

    //    private TileIndexCount traverseTiles(PMTilesReader reader, PMTilesDirectory directory) {
    //        final long directoryEntryCount = directory.numDirectoryEntries();
    //        final long tileEntryCount = directory.numTileEntries();
    //
    //        final LongAdder runLengthSum = new LongAdder();
    //        final LongAdder tileIndexCount = new LongAdder();
    //        final LongAdder tileDataCount = new LongAdder();
    //        final LongAdder tileDataSize = new LongAdder();
    //
    //        directory.tileEntries().parallel().forEach(tileEntry -> {
    //            runLengthSum.add(tileEntry.runLength());
    //            reader.getTileIndices(tileEntry, tileIndex -> {
    //                tileIndexCount.add(1);
    //                int tileSize = tileDataSize(reader, tileIndex);
    //                if (tileSize > 0) {
    //                    tileDataCount.add(1);
    //                    tileDataSize.add(tileSize);
    //                }
    //            });
    //        });
    //
    //        return new TileIndexCount(
    //                directoryEntryCount,
    //                tileEntryCount,
    //                runLengthSum.longValue(),
    //                tileIndexCount.longValue(),
    //                tileDataCount.longValue(),
    //                tileDataSize.longValue());
    //    }

    private int tileDataSize(PMTilesReader reader, TileIndex tileIndex) {
        try {
            return reader.getTile(tileIndex).map(ByteBuffer::remaining).orElse(0);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void printDirectory(PMTilesDirectory directory, long tileCount, long dirCount, String indent) {
        System.out.printf("%s%s (%,d directories, %,d tiles)%n", indent, directory, dirCount, tileCount);
    }

    private void printTile(PMTilesReader reader, PMTilesEntry tileEntry, String indent) {
        if (tileEntry.runLength() > 1) {
            TileIndex tileIndex = reader.getTileIndex(tileEntry.tileId());
            System.out.printf("%s%s%s %s%n", indent, indent, tileEntry, tileIndex);
        }
    }
}
