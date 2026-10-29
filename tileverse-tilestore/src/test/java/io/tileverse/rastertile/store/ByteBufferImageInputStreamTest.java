/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.rastertile.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.imageio.stream.ImageInputStream;
import org.junit.jupiter.api.Test;

class ByteBufferImageInputStreamTest {

    @Test
    void readsRemainingBytes() throws IOException {
        try (ImageInputStream stream = new ByteBufferImageInputStream(bytesTwoToFive())) {
            byte[] target = new byte[8];

            assertThat(stream.read(target, 0, target.length)).isEqualTo(4);
            assertThat(Arrays.copyOf(target, 4)).isEqualTo(new byte[] {2, 3, 4, 5});
            assertThat(stream.read()).isEqualTo(-1);
        }
    }

    @Test
    void keepsBufferPosition() throws IOException {
        ByteBuffer buffer = bytesTwoToFive();
        try (ImageInputStream stream = new ByteBufferImageInputStream(buffer)) {
            stream.readFully(new byte[4]);

            assertThat(buffer.position()).isEqualTo(2);
        }
    }

    @Test
    void lengthIsRemaining() throws IOException {
        try (ImageInputStream stream = new ByteBufferImageInputStream(bytesTwoToFive())) {
            assertThat(stream.length()).isEqualTo(4);
        }
    }

    @Test
    void seeksBackward() throws IOException {
        try (ImageInputStream stream = new ByteBufferImageInputStream(bytesTwoToFive())) {
            stream.readFully(new byte[3]);
            stream.seek(1);

            assertThat(stream.read()).isEqualTo(3);
        }
    }

    @Test
    void readsReadOnlyBuffer() throws IOException {
        ByteBuffer readOnly = bytesTwoToFive().asReadOnlyBuffer();
        try (ImageInputStream stream = new ByteBufferImageInputStream(readOnly)) {
            assertThat(stream.read()).isEqualTo(2);
        }
    }

    @Test
    void readAfterCloseFails() throws IOException {
        ImageInputStream stream = new ByteBufferImageInputStream(bytesTwoToFive());
        stream.close();

        assertThatThrownBy(stream::read).isInstanceOf(IOException.class);
    }

    @Test
    void readOutsideArrayFails() throws IOException {
        try (ImageInputStream stream = new ByteBufferImageInputStream(bytesTwoToFive())) {
            byte[] target = new byte[4];

            assertThatThrownBy(() -> stream.read(target, 2, 4)).isInstanceOf(IndexOutOfBoundsException.class);
        }
    }

    /** A buffer over the bytes 0 to 7, with the bytes 2 to 5 remaining. */
    private static ByteBuffer bytesTwoToFive() {
        byte[] bytes = {0, 1, 2, 3, 4, 5, 6, 7};
        return ByteBuffer.wrap(bytes).position(2).limit(6);
    }
}
