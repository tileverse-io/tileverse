/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.http;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * The body of an answer to a ranged GET, read up to a byte count as in {@link HttpRangeReader}: reads stop at the
 * count, and a body going on past it fails. Subclasses decide whether a body ending before the count fails.
 */
abstract class CappedBodyInputStream extends InputStream {

    private final InputStream body;
    private final long byteCount;
    private final byte[] singleByte = new byte[1];
    private long remaining;
    private boolean longerBody;
    private boolean closed;

    /**
     * @param body the body of the answer
     * @param byteCount the number of bytes to read from {@code body}
     */
    CappedBodyInputStream(InputStream body, long byteCount) {
        this.body = Objects.requireNonNull(body);
        this.byteCount = byteCount;
        this.remaining = byteCount;
    }

    @Override
    public int read() throws IOException {
        int read = read(singleByte, 0, 1);
        if (read == 1) {
            return Byte.toUnsignedInt(singleByte[0]);
        }
        return -1;
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
        Objects.checkFromIndexSize(offset, length, buffer.length);
        if (closed) {
            throw new IOException("Stream closed");
        }
        if (length == 0) {
            return 0;
        }
        if (remaining == 0) {
            return endOfCount();
        }
        int read = body.read(buffer, offset, (int) Math.min(length, remaining));
        if (read == -1) {
            checkEndBeforeCount(byteCount - remaining);
            return -1;
        }
        remaining -= read;
        if (remaining == 0) {
            // a caller reading exactly the byte count never reads again
            requireEndOfBody();
        }
        return read;
    }

    @Override
    public int available() throws IOException {
        return (int) Math.min(body.available(), remaining);
    }

    @Override
    public void close() throws IOException {
        closed = true;
        body.close();
    }

    /**
     * Checks a body ending after {@code received} bytes, before the byte count.
     *
     * @throws IOException if such a body fails the read
     */
    protected abstract void checkEndBeforeCount(long received) throws IOException;

    /** Returns the failure of a body going on past the byte count. */
    protected abstract IOException longerBodyFailure();

    /** Returns the number of bytes to read from the body. */
    protected long byteCount() {
        return byteCount;
    }

    /** Returns the end of the stream, failing every read after a body found longer than the byte count. */
    private int endOfCount() throws IOException {
        if (longerBody) {
            throw longerBodyFailure();
        }
        return -1;
    }

    private void requireEndOfBody() throws IOException {
        longerBody = body.read() != -1;
        if (longerBody) {
            throw longerBodyFailure();
        }
    }
}
