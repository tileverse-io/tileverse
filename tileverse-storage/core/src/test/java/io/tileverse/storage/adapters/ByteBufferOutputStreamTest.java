/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.adapters;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import org.junit.jupiter.api.Test;

// Code written against 2.1 catches the exception type of this package
@SuppressWarnings("removal")
class ByteBufferOutputStreamTest {

    @Test
    void overflowThrowsOldType() {
        ByteBufferOutputStream stream = new ByteBufferOutputStream(ByteBuffer.allocate(4), 2);
        byte[] bytes = new byte[3];
        assertThatThrownBy(() -> stream.write(bytes, 0, 3)).isExactlyInstanceOf(ByteBufferSinkException.class);
    }

    @Test
    void closedThrowsOldType() {
        ByteBufferOutputStream stream = new ByteBufferOutputStream(ByteBuffer.allocate(4), 2);
        stream.close();
        assertThatThrownBy(() -> stream.write(1)).isExactlyInstanceOf(ByteBufferSinkException.class);
    }

    @Test
    void refusedWriteKeepsCause() {
        ByteBuffer readOnly = ByteBuffer.allocate(4).asReadOnlyBuffer();
        ByteBufferOutputStream stream = new ByteBufferOutputStream(readOnly, 2);
        assertThatThrownBy(() -> stream.write(1))
                .isExactlyInstanceOf(ByteBufferSinkException.class)
                .hasCauseInstanceOf(ReadOnlyBufferException.class);
    }
}
