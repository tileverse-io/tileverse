/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Named.named;

import io.tileverse.io.ByteBufferPool.PooledByteBuffer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class ByteBufferInputStreamTest {

    private static final byte[] TEXT = "before|tile data|after".getBytes(StandardCharsets.US_ASCII);

    private static final byte[] MIDDLE = "tile data".getBytes(StandardCharsets.US_ASCII);

    static Stream<Named<ByteBuffer>> buffers() {
        ByteBuffer direct = ByteBuffer.allocateDirect(TEXT.length);
        direct.put(TEXT).flip();
        return Stream.of(named("heap", ByteBuffer.wrap(TEXT)), named("direct", direct));
    }

    @ParameterizedTest
    @MethodSource("buffers")
    void readsPositionToLimit(ByteBuffer buffer) throws IOException {
        ByteBuffer middle = aroundMiddle(buffer);
        try (InputStream stream = ByteBufferInputStream.over(middle)) {
            assertThat(stream.readAllBytes()).isEqualTo(MIDDLE);
        }
        assertThat(middle.position()).isEqualTo(middle.limit());
    }

    @ParameterizedTest
    @MethodSource("buffers")
    void transfersPositionToLimit(ByteBuffer buffer) throws IOException {
        ByteBuffer middle = aroundMiddle(buffer);
        ByteArrayOutputStream target = new ByteArrayOutputStream();
        try (InputStream stream = ByteBufferInputStream.over(middle)) {
            assertThat(stream.transferTo(target)).isEqualTo(MIDDLE.length);
        }
        assertThat(target.toByteArray()).isEqualTo(MIDDLE);
        assertThat(middle.position()).isEqualTo(middle.limit());
    }

    @Test
    void closedStreamFails() {
        ByteBufferInputStream stream = ByteBufferInputStream.over(ByteBuffer.wrap(TEXT));
        stream.close();
        assertThatThrownBy(stream::read).isInstanceOf(IOException.class);
        assertThatThrownBy(stream::readAllBytes).isInstanceOf(IOException.class);
    }

    @Test
    void closingReturnsPooledBuffer() {
        ByteBufferPool pool = ByteBufferPool.builder().build();
        PooledByteBuffer pooled = pool.borrowHeap(MIDDLE.length);
        ByteBufferInputStream stream = ByteBufferInputStream.releasing(pooled);
        stream.close();
        stream.close();
        assertThat(pool.getHeapPoolStatistics().returned()).isEqualTo(1);
    }

    /** Returns a duplicate of {@code buffer} whose readable bytes are {@link #MIDDLE}. */
    private static ByteBuffer aroundMiddle(ByteBuffer buffer) {
        ByteBuffer middle = buffer.duplicate();
        middle.position("before|".length());
        middle.limit("before|".length() + MIDDLE.length);
        return middle;
    }
}
