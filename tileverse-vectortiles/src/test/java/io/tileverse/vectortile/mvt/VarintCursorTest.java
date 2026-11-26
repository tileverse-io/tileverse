/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;

class VarintCursorTest {

    /** 1, 300 and 70000: varints of one, two and three bytes. */
    private static final byte[] THREE_VARINTS =
            new ProtoWriter().varint(1).varint(300).varint(70000).toByteArray();

    @Test
    void countsVarints() {
        VarintCursor cursor = cursor(THREE_VARINTS);

        assertThat(cursor.size()).isEqualTo(3);
    }

    @Test
    void readsInOrder() {
        VarintCursor cursor = cursor(THREE_VARINTS);

        assertThat(cursor.next()).isEqualTo(1);
        assertThat(cursor.next()).isEqualTo(300);
        assertThat(cursor.next()).isEqualTo(70000);
        assertThat(cursor.hasNext()).isFalse();
    }

    // -6 as a 64-bit value in ten bytes, then as an unsigned 32-bit value in five bytes
    @Test
    void keepsLowThirtyTwoBits() {
        VarintCursor cursor =
                cursor(new ProtoWriter().varint(-6L).varint(0xFFFFFFFAL).toByteArray());

        assertThat(cursor.next()).isEqualTo(-6);
        assertThat(cursor.next()).isEqualTo(-6);
        assertThat(cursor.hasNext()).isFalse();
    }

    @Test
    void sizeIncludesReadVarints() {
        VarintCursor cursor = cursor(THREE_VARINTS);
        cursor.next();
        cursor.next();

        assertThat(cursor.size()).isEqualTo(3);
        assertThat(cursor.remaining()).isEqualTo(1);
    }

    // The size counted before the reset belongs to the first range
    @Test
    void resetsToOtherRange() {
        VarintCursor cursor = cursor(THREE_VARINTS);
        cursor.next();
        cursor.size();
        byte[] other = new ProtoWriter().varint(5).varint(6).toByteArray();

        cursor.reset(other, 1, 2);

        assertThat(cursor.position()).isZero();
        assertThat(cursor.size()).isEqualTo(1);
        assertThat(cursor.next()).isEqualTo(6);
        assertThat(cursor.hasNext()).isFalse();
    }

    @Test
    void failsPastEnd() {
        VarintCursor cursor = cursor(new ProtoWriter().varint(1).toByteArray());
        cursor.next();

        assertThatThrownBy(cursor::next).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void rejectsCutRun() {
        byte[] bytes = new ProtoWriter().raw(0x01, 0x80).toByteArray();

        assertThatThrownBy(() -> new VarintCursor(bytes, 0, 2)).isInstanceOf(IllegalArgumentException.class);
    }

    // The byte before the range has its high bit set
    @Test
    void acceptsEmptyRange() {
        byte[] bytes = new ProtoWriter().raw(0x80, 0x01).toByteArray();

        VarintCursor cursor = new VarintCursor(bytes, 1, 1);

        assertThat(cursor.size()).isZero();
        assertThat(cursor.hasNext()).isFalse();
    }

    // 300, then 1 and 300 between bytes 2 and 5, then 5
    @Test
    void readsSubRange() {
        byte[] bytes =
                new ProtoWriter().varint(300).varint(1).varint(300).varint(5).toByteArray();

        VarintCursor cursor = new VarintCursor(bytes, 2, 5);

        assertThat(cursor.size()).isEqualTo(2);
        assertThat(cursor.next()).isEqualTo(1);
        assertThat(cursor.next()).isEqualTo(300);
        assertThat(cursor.hasNext()).isFalse();
    }

    private static VarintCursor cursor(byte[] bytes) {
        return new VarintCursor(bytes, 0, bytes.length);
    }
}
