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
package io.tileverse.rangereader.block;

import io.tileverse.io.ByteBufferPool;
import io.tileverse.rangereader.AbstractRangeReader;
import io.tileverse.rangereader.RangeReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * A decorator for RangeReader that aligns all read requests to fixed-size
 * blocks.
 * <p>
 * This implementation ensures that all reads to the underlying reader are
 * aligned to block boundaries, which helps with caching efficiency. It also
 * prevents overlapping range requests, as all reads will be aligned to the same
 * block boundaries regardless of the original offset requested.
 * <p>
 * When a range is requested that crosses block boundaries, this reader will
 * read all necessary blocks and return only the requested portion.
 */
public class BlockAlignedRangeReader extends AbstractRangeReader implements RangeReader {

    /** Default block size (64 KB) */
    public static final int DEFAULT_BLOCK_SIZE = 64 * 1024;

    private final RangeReader delegate;
    private final int blockSize;

    /**
     * Creates a new BlockAlignedRangeReader with the default block size (64 KB).
     *
     * @param delegate The underlying RangeReader to delegate to
     */
    public BlockAlignedRangeReader(RangeReader delegate) {
        this(delegate, DEFAULT_BLOCK_SIZE);
    }

    /**
     * Creates a new BlockAlignedRangeReader with the specified block size.
     *
     * @param delegate  The underlying RangeReader to delegate to
     * @param blockSize The block size to align reads to, must be a power of 2
     * @throws IllegalArgumentException If blockSize is not a power of 2
     */
    public BlockAlignedRangeReader(RangeReader delegate, int blockSize) {
        this.delegate = Objects.requireNonNull(delegate, "Delegate RangeReader cannot be null");

        // Validate block size is a power of 2
        if (blockSize <= 0 || (blockSize & (blockSize - 1)) != 0) {
            throw new IllegalArgumentException("Block size must be a positive power of 2");
        }

        this.blockSize = blockSize;
    }

    /**
     * Gets the block size used for aligning reads.
     *
     * @return The block size in bytes
     */
    public int getBlockSize() {
        return blockSize;
    }

    @Override
    protected int readRangeNoFlip(final long offset, final int actualLength, ByteBuffer target) throws IOException {
        // Calculate block-aligned range boundaries
        final long blockMask = blockSize - 1;
        final long alignedOffset = offset & ~blockMask;
        final long endOffset = offset + actualLength;
        final long alignedEndOffset = (endOffset + blockMask) & ~blockMask;

        // Calculate number of blocks we need to read, at least one block
        final int numBlocks = Math.max(1, (int) ((alignedEndOffset - alignedOffset) / blockSize));

        if (numBlocks == 1 && target.remaining() >= blockSize) {
            final int initialPosition = target.position();
            final int blockOffset = (int) (offset - alignedOffset);
            int readCount = delegate.readRange(alignedOffset, blockSize, target);

            // With NIO conventions: delegate advances position by bytes written
            // We need to adjust to only include the requested range within the block
            if (readCount > blockOffset) {
                int availableBytes = readCount - blockOffset;
                int bytesToReturn = Math.min(availableBytes, actualLength);

                // Move data to start at initialPosition if there's a block offset
                if (blockOffset > 0 && readCount > blockOffset) {
                    // Need to shift the data left to remove the unwanted prefix
                    target.position(initialPosition + blockOffset);
                    ByteBuffer src = target.slice().limit(bytesToReturn);
                    target.position(initialPosition);
                    target.put(src);
                }

                target.position(initialPosition + bytesToReturn);
                return bytesToReturn;
            } else {
                // Not enough data read to cover the block offset
                target.position(initialPosition);
                return 0;
            }
        }

        // Keep a small working buffer to read blocks
        final ByteBufferPool pool = ByteBufferPool.getDefault();
        final ByteBuffer blockBuffer = pool.borrowDirect(blockSize);
        try {
            // Position tracking for partial blocks
            int bytesRemaining = actualLength;

            // Read each block individually
            for (int i = 0; i < numBlocks && bytesRemaining > 0; i++) {
                // Calculate the block offset for this iteration
                long blockOffset = alignedOffset + (i * blockSize);

                // Calculate how much data in this block is relevant to our request
                long blockEndOffset = blockOffset + blockSize;
                long readStartOffset = Math.max(blockOffset, offset);
                long readEndOffset = Math.min(blockEndOffset, endOffset);
                int blockReadSize = (int) (readEndOffset - readStartOffset);

                if (blockReadSize <= 0) {
                    continue; // Skip this block if it doesn't contain data we need
                }

                // Read the block from the delegate
                blockBuffer.clear();
                delegate.readRange(blockOffset, blockSize, blockBuffer);

                // Flip buffer to prepare for reading (delegate follows NIO conventions)
                blockBuffer.flip();

                // Calculate position within the block for our data
                int blockPosition = (int) (readStartOffset - blockOffset);

                // Position and limit the buffer to only get the data we want
                if (blockBuffer.remaining() <= blockPosition) {
                    // We've reached the end of the data
                    break;
                }

                blockBuffer.position(blockBuffer.position() + blockPosition);
                int availableInBlock = blockBuffer.remaining();
                int toCopy = Math.min(blockReadSize, availableInBlock);
                blockBuffer.limit(blockBuffer.position() + toCopy);

                // Copy this block's contribution to the target
                target.put(blockBuffer);

                // Update tracking
                bytesRemaining -= toCopy;
            }

            // Calculate how many bytes were actually read
            int bytesRead = actualLength - bytesRemaining;
            return bytesRead;
        } finally {
            pool.returnBuffer(blockBuffer);
        }
    }

    @Override
    public OptionalLong size() throws IOException {
        return delegate.size();
    }

    @Override
    public String getSourceIdentifier() {
        return "block-aligned[" + blockSize + "]:" + delegate.getSourceIdentifier();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    /**
     * Creates a new builder for BlockAlignedRangeReader.
     * @param delegate the decorated range reader
     * @return a new builder instance
     */
    public static Builder builder(RangeReader delegate) {
        return new Builder(delegate);
    }

    /**
     * Builder for BlockAlignedRangeReader.
     */
    public static class Builder {
        private RangeReader delegate;
        private int blockSize = DEFAULT_BLOCK_SIZE;

        private Builder(RangeReader delegate) {
            this.delegate = delegate;
        }

        /**
         * Sets the delegate RangeReader to wrap with block alignment.
         *
         * @param delegate the delegate RangeReader
         * @return this builder
         */
        public Builder delegate(RangeReader delegate) {
            this.delegate = Objects.requireNonNull(delegate, "Delegate cannot be null");
            return this;
        }

        /**
         * Sets the block size for alignment.
         *
         * @param blockSize the block size (must be a positive power of 2)
         * @return this builder
         */
        public Builder blockSize(int blockSize) {
            if (blockSize <= 0) {
                throw new IllegalArgumentException("Block size must be positive: " + blockSize);
            }
            // Validate block size is a power of 2
            if ((blockSize & (blockSize - 1)) != 0) {
                throw new IllegalArgumentException("Block size must be a power of 2: " + blockSize);
            }
            this.blockSize = blockSize;
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

            return new BlockAlignedRangeReader(delegate, blockSize);
        }
    }
}
