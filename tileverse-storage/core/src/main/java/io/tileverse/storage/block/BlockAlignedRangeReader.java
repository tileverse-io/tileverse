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
package io.tileverse.storage.block;

import static java.util.Objects.requireNonNull;

import io.tileverse.io.ByteBufferPool;
import io.tileverse.io.ByteBufferPool.PooledByteBuffer;
import io.tileverse.io.ByteRange;
import io.tileverse.storage.AbstractRangeReader;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.RangeRequest;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

/**
 * A decorator that block-aligns reads inside declared byte regions and passes every other read through untouched.
 *
 * <p>Alignment uses an absolute grid computed from file offset 0 with a fixed power-of-2 block size. A request fully
 * inside the declared region union expands to the blocks that cover it; the blocks of one call are fetched with a
 * single {@link RangeReader#readRanges(List)} on the delegate. Stacked above a
 * {@link io.tileverse.storage.cache.CachingRangeReader}, the cache then stores whole blocks under stable keys:
 * block-grain caching inside regions, exact-grain outside.
 *
 * <p>Regions are runtime-mutable: format readers declare them as they discover the file layout (header, directories,
 * index arrays). Declarations normalize into a sorted disjoint union; declaring a region never changes the bytes
 * returned, only how they are fetched and cached, and never affects requests already in flight. Because the grid is
 * absolute, a late declaration leaves previously cached exact ranges valid.
 *
 * <p>Amplification bound: an aligned request fetches at most {@code length + 2 * (blockSize - 1)} bytes; each distinct
 * block is fetched at most once per call; pass-through requests fetch exactly what was asked. Blocks near a region end
 * may extend up to one block past it, and blocks near EOF simply come back short.
 *
 * <p>Thread safety: region declarations ({@link #alignRegion(long, long)}, {@link #alignWholeFile()},
 * {@link #clearRegions()}) are safe to call concurrently with reads; every read is served from a single immutable
 * snapshot of the regions in effect when it began.
 *
 * <p>Memory profile: aligned reads borrow pooled heap scratch buffers proportional to the blocks covering the request,
 * released before the call returns.
 *
 * <p>The constructors align the whole file. The builder starts with no regions: reads pass through until
 * {@link #alignRegion(long, long)} or {@link #alignWholeFile()} is called.
 */
public class BlockAlignedRangeReader extends AbstractRangeReader implements RangeReader {

    /** Default block size (64 KB) */
    public static final int DEFAULT_BLOCK_SIZE = 64 * 1024;

    private static final Region[] NO_REGIONS = new Region[0];
    private static final Region[] WHOLE_FILE = {new Region(0, Long.MAX_VALUE)};

    private final RangeReader delegate;
    private final int blockSize;

    @SuppressWarnings("java:S3077") // copy-on-write: a new array is published per mutation, elements are
    // never written after publish
    private volatile Region[] regions;

    /**
     * A half-open interval {@code [start, end)} of absolute file offsets inside which reads are block-aligned.
     *
     * @param start the first offset of the region
     * @param end the first offset after the region
     */
    public record Region(long start, long end) {
        public Region {
            if (start < 0 || end <= start) {
                throw new IllegalArgumentException("invalid region [" + start + ", " + end + ")");
            }
        }
    }

    /**
     * Creates a whole-file aligner with the default block size (64 KB).
     *
     * @param delegate the underlying RangeReader to delegate to
     */
    public BlockAlignedRangeReader(RangeReader delegate) {
        this(delegate, DEFAULT_BLOCK_SIZE);
    }

    /**
     * Creates a whole-file aligner with the given block size.
     *
     * @param delegate the underlying RangeReader to delegate to
     * @param blockSize the block size to align reads to, must be a power of 2
     * @throws IllegalArgumentException if blockSize is not a positive power of 2
     */
    public BlockAlignedRangeReader(RangeReader delegate, int blockSize) {
        this(delegate, blockSize, WHOLE_FILE);
    }

    private BlockAlignedRangeReader(RangeReader delegate, int blockSize, Region[] regions) {
        this.delegate = requireNonNull(delegate, "Delegate RangeReader cannot be null");
        checkBlockSize(blockSize);
        this.blockSize = blockSize;
        this.regions = regions;
    }

    private static void checkBlockSize(int blockSize) {
        if (blockSize <= 0 || (blockSize & (blockSize - 1)) != 0) {
            throw new IllegalArgumentException("Block size must be a positive power of 2: " + blockSize);
        }
    }

    /**
     * Gets the block size used for aligning reads.
     *
     * @return The block size in bytes
     */
    public int getBlockSize() {
        return blockSize;
    }

    /**
     * Declares that reads fully inside {@code [offset, offset + length)} are block-aligned. Overlapping and adjacent
     * declarations merge into the existing union.
     *
     * @param offset the first offset of the region
     * @param length the region length in bytes, must be positive
     */
    public synchronized void alignRegion(long offset, long length) {
        if (length <= 0) {
            throw new IllegalArgumentException("region length must be positive: " + length);
        }
        long end = offset + length;
        if (end < 0) { // overflow: saturate to open-ended
            end = Long.MAX_VALUE;
        }
        this.regions = merge(this.regions, new Region(offset, end));
    }

    /**
     * Declares the region covering {@code range}.
     *
     * @param range the byte range to align
     */
    public void alignRegion(ByteRange range) {
        alignRegion(range.offset(), range.length());
    }

    /** Declares one region covering the whole file. */
    public synchronized void alignWholeFile() {
        this.regions = WHOLE_FILE;
    }

    /** Removes every declared region; subsequent reads pass through untouched. */
    public synchronized void clearRegions() {
        this.regions = NO_REGIONS;
    }

    /**
     * Returns the current normalized region union, sorted by start offset.
     *
     * @return an immutable snapshot of the declared regions
     */
    public List<Region> regions() {
        return List.of(regions);
    }

    private static Region[] merge(Region[] current, Region added) {
        List<Region> result = new ArrayList<>(current.length + 1);
        long start = added.start();
        long end = added.end();
        for (Region region : current) {
            boolean disjoint = region.end() < start || end < region.start();
            if (disjoint) {
                result.add(region);
            } else { // overlapping or adjacent: absorb into the added region
                start = Math.min(start, region.start());
                end = Math.max(end, region.end());
            }
        }
        result.add(new Region(start, end));
        result.sort((a, b) -> Long.compare(a.start(), b.start()));
        return result.toArray(Region[]::new);
    }

    private static boolean fullyInside(Region[] snapshot, long offset, int length) {
        long end = offset + length;
        for (Region region : snapshot) {
            if (region.start() > offset) {
                return false; // sorted: no earlier region can cover the start
            }
            if (region.end() >= end) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected int readRangeNoFlip(final long offset, final int actualLength, ByteBuffer target) {
        Region[] snapshot = this.regions;
        if (!fullyInside(snapshot, offset, actualLength)) {
            return delegate.readRange(offset, actualLength, target);
        }
        List<ByteRange> blocks = blocksCovering(offset, actualLength);
        List<PooledByteBuffer> scratches = new ArrayList<>(blocks.size());
        try {
            List<RangeRequest> blockRequests = new ArrayList<>(blocks.size());
            for (ByteRange block : blocks) {
                PooledByteBuffer scratch = ByteBufferPool.heapBuffer(blockSize);
                scratches.add(scratch);
                blockRequests.add(new RangeRequest(block, scratch.buffer().clear()));
            }
            int[] blockRead = delegate.readRanges(blockRequests);
            return copyFromBlocks(offset, actualLength, target, blocks, blockRequests, blockRead);
        } finally {
            scratches.forEach(PooledByteBuffer::close);
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Results come back in request order. Each distinct block is fetched at most once per call; an aligned request
     * fetches at most {@code length + 2 * (blockSize - 1)} bytes. Requests outside the declared region union are
     * forwarded to the delegate exactly as given.
     */
    @Override
    public int[] readRanges(List<RangeRequest> requests) {
        RangeRequest.validate(requests);
        Region[] snapshot = this.regions;
        int[] read = new int[requests.size()];

        List<RangeRequest> downstream = new ArrayList<>();
        int[] passThroughIndex = new int[requests.size()]; // downstream index per pass-through request, -1 otherwise
        Map<Long, Integer> blockIndex = new LinkedHashMap<>(); // block start -> downstream index
        List<PooledByteBuffer> scratches = new ArrayList<>();
        try {
            partitionRequests(requests, snapshot, downstream, passThroughIndex, blockIndex, scratches);
            if (downstream.isEmpty()) {
                return read;
            }
            int[] downstreamRead = delegate.readRanges(downstream);
            for (int i = 0; i < requests.size(); i++) {
                RangeRequest request = requests.get(i);
                ByteRange range = request.range();
                if (range.length() == 0) {
                    continue;
                }
                if (passThroughIndex[i] >= 0) {
                    read[i] = downstreamRead[passThroughIndex[i]];
                } else {
                    read[i] = copyOut(range, request.target(), blockIndex, downstream, downstreamRead);
                }
            }
            return read;
        } finally {
            scratches.forEach(PooledByteBuffer::close);
        }
    }

    private void partitionRequests(
            List<RangeRequest> requests,
            Region[] snapshot,
            List<RangeRequest> downstream,
            int[] passThroughIndex,
            Map<Long, Integer> blockIndex,
            List<PooledByteBuffer> scratches) {
        for (int i = 0; i < requests.size(); i++) {
            RangeRequest request = requests.get(i);
            ByteRange range = request.range();
            passThroughIndex[i] = -1;
            if (range.length() == 0) {
                continue;
            }
            if (fullyInside(snapshot, range.offset(), range.length())) {
                for (ByteRange block : blocksCovering(range.offset(), range.length())) {
                    blockIndex.computeIfAbsent(block.offset(), start -> {
                        PooledByteBuffer scratch = ByteBufferPool.heapBuffer(blockSize);
                        scratches.add(scratch);
                        downstream.add(new RangeRequest(block, scratch.buffer().clear()));
                        return downstream.size() - 1;
                    });
                }
            } else {
                downstream.add(request);
                passThroughIndex[i] = downstream.size() - 1;
            }
        }
    }

    private List<ByteRange> blocksCovering(long offset, int length) {
        final long blockMask = blockSize - 1L;
        final long firstBlock = offset & ~blockMask;
        final long lastBlock = (offset + length - 1) & ~blockMask;
        List<ByteRange> blocks = new ArrayList<>((int) ((lastBlock - firstBlock) / blockSize) + 1);
        for (long start = firstBlock; start <= lastBlock; start += blockSize) {
            blocks.add(new ByteRange(start, blockSize));
        }
        return blocks;
    }

    private int copyFromBlocks(
            long offset,
            int length,
            ByteBuffer target,
            List<ByteRange> blocks,
            List<RangeRequest> blockRequests,
            int[] blockRead) {
        int copied = 0;
        long position = offset;
        final long end = offset + length;
        for (int b = 0; b < blocks.size() && position < end; b++) {
            ByteRange block = blocks.get(b);
            long availableEnd = block.offset() + blockRead[b];
            if (availableEnd > position) {
                ByteBuffer scratch = blockRequests.get(b).target().duplicate().flip();
                int from = (int) (position - block.offset());
                int to = (int) (Math.min(availableEnd, end) - block.offset());
                scratch.position(from).limit(to);
                target.put(scratch);
                copied += to - from;
                position = block.offset() + to;
            }
            if (availableEnd < block.end()) {
                break; // short block: EOF falls inside or before it, no further block has data
            }
        }
        return copied;
    }

    private int copyOut(
            ByteRange range,
            ByteBuffer target,
            Map<Long, Integer> blockIndex,
            List<RangeRequest> downstream,
            int[] downstreamRead) {
        List<ByteRange> blocks = blocksCovering(range.offset(), range.length());
        List<RangeRequest> blockRequests = new ArrayList<>(blocks.size());
        int[] blockRead = new int[blocks.size()];
        for (int b = 0; b < blocks.size(); b++) {
            int index = blockIndex.get(blocks.get(b).offset());
            blockRequests.add(downstream.get(index));
            blockRead[b] = downstreamRead[index];
        }
        return copyFromBlocks(range.offset(), range.length(), target, blocks, blockRequests, blockRead);
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

    /**
     * Creates a new builder for BlockAlignedRangeReader. The built reader passes reads through until a region is
     * declared, on the builder or later on the instance.
     *
     * @param delegate the decorated range reader
     * @return a new builder instance
     */
    public static Builder builder(RangeReader delegate) {
        return new Builder(delegate);
    }

    /** Builder for BlockAlignedRangeReader. */
    public static class Builder {
        private RangeReader delegate;
        private int blockSize = DEFAULT_BLOCK_SIZE;
        private Region[] regions = NO_REGIONS;

        private Builder(RangeReader delegate) {
            this.delegate = delegate;
        }

        /**
         * Sets the delegate RangeReader to wrap.
         *
         * @param delegate the delegate RangeReader
         * @return this builder
         */
        public Builder delegate(RangeReader delegate) {
            this.delegate = requireNonNull(delegate, "Delegate cannot be null");
            return this;
        }

        /**
         * Sets the block size for alignment.
         *
         * @param blockSize the block size (must be a positive power of 2)
         * @return this builder
         */
        public Builder blockSize(int blockSize) {
            checkBlockSize(blockSize);
            this.blockSize = blockSize;
            return this;
        }

        /**
         * Declares a region to align; may be called several times, declarations merge.
         *
         * @param offset the first offset of the region
         * @param length the region length in bytes, must be positive
         * @return this builder
         */
        public Builder alignRegion(long offset, long length) {
            if (length <= 0) {
                throw new IllegalArgumentException("region length must be positive: " + length);
            }
            long end = offset + length;
            if (end < 0) {
                end = Long.MAX_VALUE;
            }
            this.regions = merge(this.regions, new Region(offset, end));
            return this;
        }

        /**
         * Declares the region covering {@code range}.
         *
         * @param range the byte range to align
         * @return this builder
         */
        public Builder alignRegion(ByteRange range) {
            return alignRegion(range.offset(), range.length());
        }

        /**
         * Declares one region covering the whole file, reproducing the constructor behavior.
         *
         * @return this builder
         */
        public Builder alignWholeFile() {
            this.regions = WHOLE_FILE;
            return this;
        }

        /**
         * Builds the BlockAlignedRangeReader.
         *
         * @return a new BlockAlignedRangeReader instance
         * @throws IllegalStateException if delegate is not set
         */
        public BlockAlignedRangeReader build() {
            if (delegate == null) {
                throw new IllegalStateException("Delegate RangeReader must be set");
            }
            return new BlockAlignedRangeReader(delegate, blockSize, regions);
        }
    }
}
