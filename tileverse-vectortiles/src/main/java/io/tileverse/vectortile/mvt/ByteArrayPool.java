/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Byte arrays reused across the decodes of tiles read from a stream. The length of a stream is unknown until its end,
 * and a decoded tile keeps a copy of exact length. The stream is read into a borrowed array, grown until the tile fits,
 * and borrowing spares the growing arrays of each decode.
 *
 * <p>A borrowed array is valid only during the call to the {@link ArrayFunction}. The pool keeps released arrays up to
 * a total length, and drops the arrays beyond it.
 */
final class ByteArrayPool {

    /** A function of the first {@code length} bytes of {@code array}. */
    @FunctionalInterface
    interface ArrayFunction<T> {
        T apply(byte[] array, int length) throws IOException;
    }

    private final int initialLength;
    private final long maxFreeBytes;
    private final Queue<byte[]> free = new ConcurrentLinkedQueue<>();
    private final AtomicLong freeBytes = new AtomicLong();

    /**
     * @param initialLength the length of a new array for a stream, doubled until the stream fits
     * @param maxFreeBytes the total length of the arrays kept between calls
     */
    ByteArrayPool(int initialLength, long maxFreeBytes) {
        this.initialLength = initialLength;
        this.maxFreeBytes = maxFreeBytes;
    }

    /** Reads {@code in} to its end into a pooled array, and applies {@code function} to the bytes read. */
    <T> T withContentOf(InputStream in, ArrayFunction<T> function) throws IOException {
        byte[] array = borrow();
        try {
            int length = 0;
            int read;
            while ((read = in.read(array, length, array.length - length)) != -1) {
                length += read;
                if (length == array.length) {
                    array = Arrays.copyOf(array, 2 * array.length);
                }
            }
            return function.apply(array, length);
        } finally {
            release(array);
        }
    }

    // A pooled array starts at the initial length and only grows: any of them can start a read
    private byte[] borrow() {
        byte[] array = free.poll();
        if (array == null) {
            return new byte[initialLength];
        }
        freeBytes.addAndGet(-array.length);
        return array;
    }

    private void release(byte[] array) {
        if (freeBytes.addAndGet(array.length) > maxFreeBytes) {
            freeBytes.addAndGet(-array.length);
            return;
        }
        free.offer(array);
    }
}
