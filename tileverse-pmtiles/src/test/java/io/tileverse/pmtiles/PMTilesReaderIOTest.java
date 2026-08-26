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
package io.tileverse.pmtiles;

import static org.assertj.core.api.Assertions.assertThat;

import io.tileverse.cache.CacheManager;
import io.tileverse.io.ByteRange;
import io.tileverse.storage.block.BlockAlignedRangeReader;
import io.tileverse.storage.cache.CachingRangeReader;
import io.tileverse.tiling.pyramid.TileIndex;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Pins the backend I/O shape of PMTilesReader operations: header, metadata, and directory reads arrive as their
 * covering 64 KiB blocks through the internal aligner; tile reads stay exact ranges. Nothing streams byte-by-byte
 * through a channel.
 */
class PMTilesReaderIOTest {

    private static final int HEADER_SIZE = 127;
    private static final int BLOCK_SIZE = 64 * 1024;

    private PMTilesTestData.RecordingRangeReader recording;
    private PMTilesReader reader;

    @BeforeEach
    void setup(@TempDir Path tmpFolder) throws IOException {
        this.recording = PMTilesTestData.recording(PMTilesTestData.andorraFileRangeReader(tmpFolder));
        this.reader = new PMTilesReader(recording);
        this.reader.cacheManager(CacheManager.newInstance());
    }

    @AfterEach
    void tearDown() throws IOException {
        reader.close();
    }

    /** The 64 KiB blocks on the absolute grid covering {@code range}, in ascending order. */
    private static List<ByteRange> blocksCovering(ByteRange range) {
        long firstBlock = range.offset() & ~((long) BLOCK_SIZE - 1);
        long lastBlockEnd = ((range.end() - 1) & ~((long) BLOCK_SIZE - 1)) + BLOCK_SIZE;
        List<ByteRange> blocks = new ArrayList<>();
        for (long start = firstBlock; start < lastBlockEnd; start += BLOCK_SIZE) {
            blocks.add(ByteRange.of(start, BLOCK_SIZE));
        }
        return blocks;
    }

    @Test
    void constructionFetchesWarmBootstrapBlocks() {
        PMTilesHeader header = reader.getHeader();
        List<ByteRange> reads = recording.reads();

        List<ByteRange> expected = new ArrayList<>(blocksCovering(ByteRange.of(0, HEADER_SIZE)));
        expected.addAll(blocksCovering(header.jsonMetadata()));
        assertThat(reads)
                .as("construction reads the header's covering block, then the metadata's covering blocks")
                .containsExactlyElementsOf(expected);
    }

    @Test
    void metadataReadArrivesAsItsBlocks() throws IOException {
        ByteRange metadata = reader.getHeader().jsonMetadata();
        recording.clearRecordings();

        reader.getMetadataAsString();

        assertThat(recording.reads()).containsExactlyElementsOf(blocksCovering(metadata));
    }

    @Test
    void rootDirectoryReadArrivesAsItsBlocks() {
        ByteRange rootDirectory = reader.getHeader().rootDirectory();
        recording.clearRecordings();

        reader.getRootDirectory();

        assertThat(recording.reads()).containsExactlyElementsOf(blocksCovering(rootDirectory));
    }

    @Test
    void tileLoadReadsDirectoryBlocksAndExactTile() throws IOException {
        PMTilesHeader header = reader.getHeader();
        recording.clearRecordings();

        TileIndex minZoomTile = firstTileAt(header.minZoom());
        Optional<ByteBuffer> tile = reader.getTile(minZoomTile);

        assertThat(tile).isPresent();
        List<ByteRange> reads = recording.reads();
        List<ByteRange> rootBlocks = blocksCovering(header.rootDirectory());
        assertThat(reads.subList(0, rootBlocks.size()))
                .as("directory read arrives as its blocks")
                .containsExactlyElementsOf(rootBlocks);
        ByteRange tileRead = reads.get(reads.size() - 1);
        assertThat(tileRead.offset())
                .as("tile bytes stay exact, outside every declared region")
                .isGreaterThanOrEqualTo(header.tileDataOffset());
        assertThat(tileRead.offset() % BLOCK_SIZE != 0 || tileRead.length() != BLOCK_SIZE)
                .as("the tile read is not block-quantized")
                .isTrue();
        for (ByteRange read : reads) {
            assertThat(read.length()).as("no byte-at-a-time streaming").isGreaterThan(1);
        }
    }

    @Test
    void declaredRegionsMatchTheHeaderLayout() {
        PMTilesHeader header = reader.getHeader();

        List<BlockAlignedRangeReader.Region> regions = reader.alignedReader().regions();

        assertThat(regions).isNotEmpty();
        assertThat(regions.get(0).start()).isZero();
        long metadataEnd = header.jsonMetadataOffset() + header.jsonMetadataBytes();
        assertThat(regions.stream()
                        .anyMatch(
                                region -> region.start() <= header.jsonMetadataOffset() && region.end() >= metadataEnd))
                .as("metadata range inside the region union")
                .isTrue();
    }

    @Test
    void cacheUnderTheReaderDeduplicatesWarmBlockFetches(@TempDir Path cacheTmp) throws IOException {
        PMTilesTestData.RecordingRangeReader backend =
                PMTilesTestData.recording(PMTilesTestData.andorraFileRangeReader(cacheTmp));
        try (PMTilesReader cached = new PMTilesReader(CachingRangeReader.of(backend))) {
            cached.cacheManager(CacheManager.newInstance());
            PMTilesHeader header = cached.getHeader();
            cached.getRootDirectory();
            cached.getMetadataAsString();

            List<ByteRange> expected = new ArrayList<>(blocksCovering(ByteRange.of(0, HEADER_SIZE)));
            expected.addAll(blocksCovering(header.jsonMetadata()));
            List<ByteRange> distinctExpected = expected.stream().distinct().toList();
            assertThat(backend.reads())
                    .as("the warm bootstrap block also serves the root directory, metadata blocks fetch once, "
                            + "and re-reads hit the cache")
                    .containsExactlyElementsOf(distinctExpected);
        }
    }

    private TileIndex firstTileAt(int zoom) {
        PMTilesDirectory root = reader.getRootDirectory();
        for (PMTilesEntry entry : root) {
            if (!entry.isLeaf()) {
                TileIndex index = reader.getTileIndex(entry.tileId());
                if (index.z() == zoom) {
                    return index;
                }
            }
        }
        throw new IllegalStateException("fixture has no direct tile entry at zoom " + zoom);
    }
}
