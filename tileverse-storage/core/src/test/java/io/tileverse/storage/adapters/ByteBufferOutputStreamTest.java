/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tileverse.storage.adapters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class ByteBufferOutputStreamTest {

    private static byte[] bytes(int count) {
        byte[] out = new byte[count];
        for (int i = 0; i < count; i++) {
            out[i] = (byte) (i * 7 + 1);
        }
        return out;
    }

    private static byte[] contents(ByteBuffer buffer, int from, int length) {
        byte[] out = new byte[length];
        buffer.get(from, out);
        return out;
    }

    @Test
    void writesLandAtTheCurrentPositionAndAdvanceIt() {
        ByteBuffer target = ByteBuffer.allocate(16);
        target.position(4);
        ByteBufferOutputStream stream = new ByteBufferOutputStream(target, 8);

        stream.write(new byte[] {1, 2, 3}, 0, 3);
        stream.write(4);

        assertThat(stream.bytesWritten()).isEqualTo(4);
        assertThat(target.position()).isEqualTo(8);
        assertThat(target.limit()).isEqualTo(16);
        assertThat(contents(target, 4, 4)).containsExactly(1, 2, 3, 4);
    }

    @Test
    void fillsADirectBuffer() {
        ByteBuffer target = ByteBuffer.allocateDirect(12);
        target.position(2);
        ByteBufferOutputStream stream = new ByteBufferOutputStream(target, 8);
        byte[] body = bytes(8);

        stream.write(body, 0, 5);
        stream.write(body, 5, 3);

        assertThat(target.position()).isEqualTo(10);
        assertThat(contents(target, 2, 8)).containsExactly(body);
    }

    @Test
    void transferToFillsTheBufferFromAnInputStream() throws IOException {
        ByteBuffer target = ByteBuffer.allocate(64);
        target.position(10);
        byte[] body = bytes(40);
        ByteBufferOutputStream stream = new ByteBufferOutputStream(target, 40);

        long transferred = new ByteArrayInputStream(body).transferTo(stream);

        assertThat(transferred).isEqualTo(40);
        assertThat(stream.bytesWritten()).isEqualTo(40);
        assertThat(target.position()).isEqualTo(50);
        assertThat(contents(target, 10, 40)).containsExactly(body);
    }

    @Test
    void rejectsAWritePastTheAcceptedCountBeforeTouchingTheBuffer() {
        ByteBuffer target = ByteBuffer.allocate(16);
        ByteBufferOutputStream stream = new ByteBufferOutputStream(target, 4);
        stream.write(new byte[] {1, 2, 3}, 0, 3);

        assertThatThrownBy(() -> stream.write(new byte[] {4, 5}, 0, 2)).isInstanceOf(BufferOverflowException.class);

        assertThat(target.position()).isEqualTo(3);
        assertThat(stream.bytesWritten()).isEqualTo(3);
        stream.write(4);
        assertThat(stream.bytesWritten()).isEqualTo(4);
        assertThatThrownBy(() -> stream.write(5)).isInstanceOf(BufferOverflowException.class);
    }

    @Test
    void acceptsNothingWhenMaxBytesIsZero() {
        ByteBuffer target = ByteBuffer.allocate(4);
        ByteBufferOutputStream stream = new ByteBufferOutputStream(target, 0);

        assertThatThrownBy(() -> stream.write(1)).isInstanceOf(BufferOverflowException.class);
        assertThat(target.position()).isZero();
    }

    @Test
    void rejectsACountOutsideTheRemainingCapacity() {
        ByteBuffer target = ByteBuffer.allocate(4);

        assertThatThrownBy(() -> new ByteBufferOutputStream(target, 5)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ByteBufferOutputStream(target, -1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsASourceRangeOutsideTheArray() {
        ByteBuffer target = ByteBuffer.allocate(16);
        ByteBufferOutputStream stream = new ByteBufferOutputStream(target, 8);
        byte[] source = new byte[4];

        assertThatThrownBy(() -> stream.write(source, 2, 4)).isInstanceOf(IndexOutOfBoundsException.class);

        assertThat(target.position()).isZero();
        assertThat(stream.bytesWritten()).isZero();
    }

    @Test
    void rejectsANullTarget() {
        assertThatThrownBy(() -> new ByteBufferOutputStream(null, 0)).isInstanceOf(NullPointerException.class);
    }
}
