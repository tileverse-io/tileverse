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
package io.tileverse.storage.block;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tileverse.io.ByteRange;
import io.tileverse.storage.ByteArrayRangeReader;
import io.tileverse.storage.RangeRequest;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for region declaration, mutation, and batch propagation in BlockAlignedRangeReader. */
class BlockAlignedRegionsTest {

    private static final int SIZE = 200 * 1024;
    private static final int BLOCK = 4096;

    private byte[] data;
    private ByteArrayRangeReader recorder;

    @BeforeEach
    void setUp() {
        data = new byte[SIZE];
        for (int i = 0; i < SIZE; i++) {
            data[i] = (byte) (i % 251);
        }
        recorder = new ByteArrayRangeReader(data);
    }

    @Test
    void builderWithoutRegionsPassesThrough() throws IOException {
        try (BlockAlignedRangeReader reader =
                BlockAlignedRangeReader.builder(recorder).blockSize(BLOCK).build()) {
            assertThat(reader.regions()).isEmpty();
            ByteBuffer target = ByteBuffer.allocate(100);
            assertThat(reader.readRange(10_000, 100, target)).isEqualTo(100);
            assertThat(recorder.singleReads()).containsExactly(new ByteRange(10_000, 100));
            assertThat(recorder.batchReads()).isEmpty();
        }
    }

    @Test
    void constructorsAlignTheWholeFile() throws IOException {
        try (BlockAlignedRangeReader reader = new BlockAlignedRangeReader(recorder, BLOCK)) {
            assertThat(reader.regions()).containsExactly(new BlockAlignedRangeReader.Region(0, Long.MAX_VALUE));
        }
    }

    @Test
    void requestFullyInsideARegionExpandsToBlocks() throws IOException {
        try (BlockAlignedRangeReader reader = BlockAlignedRangeReader.builder(recorder)
                .blockSize(BLOCK)
                .alignRegion(0, 64 * 1024)
                .build()) {
            ByteBuffer target = ByteBuffer.allocate(200);
            // [6000, 6200) spans blocks [4096, 8192)
            assertThat(reader.readRange(6000, 200, target)).isEqualTo(200);
            assertThat(recorder.batchReads()).containsExactly(List.of(new ByteRange(4096, BLOCK)));
            assertContent(target.flip(), 6000, 200);

            // spanning two blocks -> one batch of two block requests
            recorder.clearRecordings();
            ByteBuffer spanning = ByteBuffer.allocate(2000);
            assertThat(reader.readRange(7000, 2000, spanning)).isEqualTo(2000);
            assertThat(recorder.batchReads())
                    .containsExactly(List.of(new ByteRange(4096, BLOCK), new ByteRange(8192, BLOCK)));
            assertContent(spanning.flip(), 7000, 2000);
        }
    }

    @Test
    void straddlingAndOutsideRequestsPassThroughExact() throws IOException {
        try (BlockAlignedRangeReader reader = BlockAlignedRangeReader.builder(recorder)
                .blockSize(BLOCK)
                .alignRegion(0, 10_000)
                .build()) {
            ByteBuffer straddling = ByteBuffer.allocate(2000);
            // starts inside [0, 10000) but ends outside: exact pass-through
            assertThat(reader.readRange(9000, 2000, straddling)).isEqualTo(2000);
            ByteBuffer outside = ByteBuffer.allocate(100);
            assertThat(reader.readRange(50_000, 100, outside)).isEqualTo(100);
            assertThat(recorder.singleReads()).containsExactly(new ByteRange(9000, 2000), new ByteRange(50_000, 100));
            assertThat(recorder.batchReads()).isEmpty();
        }
    }

    @Test
    void regionsNormalizeToADisjointSortedUnion() throws IOException {
        try (BlockAlignedRangeReader reader =
                BlockAlignedRangeReader.builder(recorder).blockSize(BLOCK).build()) {
            reader.alignRegion(10_000, 5000);
            reader.alignRegion(20_000, 5000);
            reader.alignRegion(14_000, 6001); // bridges both
            assertThat(reader.regions()).containsExactly(new BlockAlignedRangeReader.Region(10_000, 25_000));
            reader.alignRegion(25_000, 1000); // adjacent: merges
            assertThat(reader.regions()).containsExactly(new BlockAlignedRangeReader.Region(10_000, 26_000));
            reader.clearRegions();
            assertThat(reader.regions()).isEmpty();
        }
    }

    @Test
    void lateDeclarationOnlyReshapesFutureRequests() throws IOException {
        try (BlockAlignedRangeReader reader =
                BlockAlignedRangeReader.builder(recorder).blockSize(BLOCK).build()) {
            ByteBuffer before = ByteBuffer.allocate(100);
            reader.readRange(6000, 100, before);
            reader.alignRegion(0, 64 * 1024);
            ByteBuffer after = ByteBuffer.allocate(100);
            reader.readRange(6000, 100, after);
            // ByteArrayRangeReader.readRanges() executes each block through readRange(), which also
            // records into singleReads(): the exact pre-declaration read, then the one block fetched
            // for the post-declaration aligned read.
            assertThat(recorder.singleReads()).containsExactly(new ByteRange(6000, 100), new ByteRange(4096, BLOCK));
            assertThat(recorder.batchReads()).containsExactly(List.of(new ByteRange(4096, BLOCK)));
        }
    }

    @Test
    void readRangesForwardsOneMixedBatchWithDedupedBlocks() throws IOException {
        try (BlockAlignedRangeReader reader = BlockAlignedRangeReader.builder(recorder)
                .blockSize(BLOCK)
                .alignRegion(0, 64 * 1024)
                .build()) {
            ByteBuffer a = ByteBuffer.allocate(100);
            ByteBuffer b = ByteBuffer.allocate(200);
            ByteBuffer c = ByteBuffer.allocate(300);
            // a and b live in block [4096, 8192): deduped; c is outside the region: pass-through
            int[] read = reader.readRanges(List.of(
                    RangeRequest.of(5000, 100, a), RangeRequest.of(6000, 200, b), RangeRequest.of(100_000, 300, c)));
            assertThat(read).containsExactly(100, 200, 300);
            assertThat(recorder.batchReads())
                    .containsExactly(List.of(new ByteRange(4096, BLOCK), new ByteRange(100_000, 300)));
            assertContent(a.flip(), 5000, 100);
            assertContent(b.flip(), 6000, 200);
            assertContent(c.flip(), 100_000, 300);
        }
    }

    @Test
    void alignedReadNearEofReturnsShortCount() throws IOException {
        try (BlockAlignedRangeReader reader = new BlockAlignedRangeReader(recorder, BLOCK)) {
            ByteBuffer target = ByteBuffer.allocate(2000);
            int read = reader.readRange(SIZE - 100, 2000, target);
            assertThat(read).isEqualTo(100);
            assertContent(target.flip(), SIZE - 100, 100);
        }
    }

    @Test
    void identifierIsTheDelegates() throws IOException {
        try (BlockAlignedRangeReader reader = new BlockAlignedRangeReader(recorder, BLOCK)) {
            assertThat(reader.getSourceIdentifier()).isEqualTo(recorder.getSourceIdentifier());
        }
    }

    @Test
    void invalidRegionsAreRejected() throws IOException {
        try (BlockAlignedRangeReader reader =
                BlockAlignedRangeReader.builder(recorder).blockSize(BLOCK).build()) {
            assertThatThrownBy(() -> reader.alignRegion(-1, 10)).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> reader.alignRegion(0, 0)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void concurrentReadsDuringDeclarationStayConsistent() throws Exception {
        try (BlockAlignedRangeReader reader =
                BlockAlignedRangeReader.builder(recorder).blockSize(BLOCK).build()) {
            ExecutorService executor = Executors.newFixedThreadPool(4);
            try {
                CountDownLatch start = new CountDownLatch(1);
                List<Future<Boolean>> futures = new ArrayList<>();
                for (int t = 0; t < 4; t++) {
                    final int offset = 10_000 + t * 3000;
                    futures.add(executor.submit(() -> {
                        start.await();
                        for (int i = 0; i < 200; i++) {
                            ByteBuffer target = ByteBuffer.allocate(500);
                            int read = reader.readRange(offset, 500, target);
                            if (read != 500) {
                                return false;
                            }
                            target.flip();
                            for (int j = 0; j < 500; j++) {
                                if (target.get() != (byte) ((offset + j) % 251)) {
                                    return false;
                                }
                            }
                        }
                        return true;
                    }));
                }
                start.countDown();
                for (int i = 0; i < 100; i++) {
                    reader.alignRegion(i * 1000L, 1000);
                    reader.clearRegions();
                }
                for (Future<Boolean> future : futures) {
                    assertThat(future.get(30, TimeUnit.SECONDS)).isTrue();
                }
            } finally {
                executor.shutdownNow();
            }
        }
    }

    private void assertContent(ByteBuffer buffer, int offset, int length) {
        assertThat(buffer.remaining()).isEqualTo(length);
        for (int i = 0; i < length; i++) {
            assertThat(buffer.get()).as("byte at " + (offset + i)).isEqualTo((byte) ((offset + i) % 251));
        }
    }
}
