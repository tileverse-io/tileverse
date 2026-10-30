/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class ByteArrayPoolTest {

    @Test
    void readsBeyondInitialLength() throws IOException {
        ByteArrayPool pool = new ByteArrayPool(4, 1024);

        byte[] read = pool.withContentOf(stream("longer than four bytes"), Arrays::copyOf);

        assertThat(read).isEqualTo("longer than four bytes".getBytes(UTF_8));
    }

    @Test
    void reusesReleasedArray() throws IOException {
        ByteArrayPool pool = new ByteArrayPool(16, 1024);

        byte[] first = pool.withContentOf(stream("a"), (array, length) -> array);
        byte[] second = pool.withContentOf(stream("b"), (array, length) -> array);

        assertThat(second).isSameAs(first);
    }

    @Test
    void copiesRemainingBytes() throws IOException {
        ByteArrayPool pool = new ByteArrayPool(16, 1024);
        ByteBuffer buffer = ByteBuffer.wrap("skip,keep".getBytes(UTF_8)).position(5).asReadOnlyBuffer();

        byte[] copied = pool.withContentOf(buffer, Arrays::copyOf);

        assertThat(copied).isEqualTo("keep".getBytes(UTF_8));
        assertThat(buffer.position()).isEqualTo(5);
    }

    @Test
    void dropsArraysAboveBudget() throws IOException {
        ByteArrayPool pool = new ByteArrayPool(16, 8);

        byte[] first = pool.withContentOf(stream("a"), (array, length) -> array);
        byte[] second = pool.withContentOf(stream("b"), (array, length) -> array);

        assertThat(second).isNotSameAs(first);
    }

    @Test
    void replacesArrayTooSmallForBuffer() throws IOException {
        ByteArrayPool pool = new ByteArrayPool(4, 1024);
        byte[] small = pool.withContentOf(stream("a"), (array, length) -> array);

        byte[] large = pool.withContentOf(ByteBuffer.wrap(new byte[10]), (array, length) -> array);

        assertThat(large).isNotSameAs(small).hasSizeGreaterThanOrEqualTo(10);
    }

    private static InputStream stream(String content) {
        return new ByteArrayInputStream(content.getBytes(UTF_8));
    }
}
