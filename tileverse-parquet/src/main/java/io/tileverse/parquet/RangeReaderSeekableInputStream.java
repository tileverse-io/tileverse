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
package io.tileverse.parquet;

import io.tileverse.io.ByteBufferPool;
import io.tileverse.io.ByteBufferPool.PooledByteBuffer;
import io.tileverse.rangereader.RangeReader;
import io.tileverse.rangereader.adapters.RangeReaderSeekableByteChannel;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.io.SeekableInputStream;

/**
 * A {@link SeekableInputStream} backed by a {@link RangeReaderSeekableByteChannel}.
 * <p>
 * All read operations delegate directly to the channel, which in turn delegates
 * to the thread-safe {@link RangeReader#readRange} method. The channel maintains
 * the current position, so this stream does not need its own position tracking.
 * <p>
 * This class is not thread-safe. Parquet readers create one stream per read context.
 * <p>
 * Closing this stream closes the channel view but does <strong>not</strong> close
 * the underlying {@code RangeReader}.
 */
class RangeReaderSeekableInputStream extends SeekableInputStream {

    private final RangeReaderSeekableByteChannel channel;

    RangeReaderSeekableInputStream(RangeReader rangeReader) {
        this.channel = RangeReaderSeekableByteChannel.of(rangeReader);
    }

    @Override
    public long getPos() throws IOException {
        return channel.position();
    }

    @Override
    public void seek(long newPos) throws IOException {
        channel.position(newPos);
    }

    @Override
    public int read() throws IOException {
        try (PooledByteBuffer pooledByteBuffer = ByteBufferPool.heapBuffer(1)) {
            ByteBuffer buf = pooledByteBuffer.buffer();
            int n = channel.read(buf);
            if (n < 0) {
                return -1;
            }
            buf.flip();
            return buf.get() & 0xFF;
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return len == 0 ? 0 : channel.read(ByteBuffer.wrap(b, off, len));
    }

    @Override
    public void readFully(byte[] bytes) throws IOException {
        readFully(bytes, 0, bytes.length);
    }

    @Override
    public void readFully(byte[] bytes, int start, int len) throws IOException {
        readFully(ByteBuffer.wrap(bytes, start, len));
    }

    @Override
    public int read(ByteBuffer buf) throws IOException {
        return channel.read(buf);
    }

    @Override
    public void readFully(ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            int bytesRead = channel.read(buf);
            if (bytesRead < 0) {
                throw new EOFException(
                        "Reached the end of stream with %d bytes left to read".formatted(buf.remaining()));
            }
        }
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
