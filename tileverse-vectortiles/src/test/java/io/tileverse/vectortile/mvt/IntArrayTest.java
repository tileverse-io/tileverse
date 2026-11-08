/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class IntArrayTest {

    @Test
    void growsPastInitialCapacity() {
        IntArray array = arrayOfFirstInts(1000);

        int[] firstInts = IntStream.range(0, 1000).toArray();
        assertThat(array.size()).isEqualTo(1000);
        assertThat(array.get(999)).isEqualTo(999);
        assertThat(array.array()).startsWith(firstInts);
    }

    @Test
    void setReplacesValue() {
        IntArray array = arrayOfFirstInts(3);

        array.set(1, 20);

        assertThat(array.size()).isEqualTo(3);
        assertThat(array.get(1)).isEqualTo(20);
    }

    @Test
    void truncateKeepsPrefix() {
        IntArray array = arrayOfFirstInts(5);

        array.truncate(2);

        assertThat(array.size()).isEqualTo(2);
        assertThat(array.get(1)).isEqualTo(1);
    }

    // Past the initial capacity: a clear allocating a new array would shrink it
    @Test
    void clearKeepsCapacity() {
        IntArray array = arrayOfFirstInts(100);
        int capacity = array.capacity();

        array.clear();

        assertThat(array.isEmpty()).isTrue();
        assertThat(array.capacity()).isEqualTo(capacity);
    }

    // The backing array still holds the truncated values: the checks compare with the size
    @Test
    void rejectsIndexPastSize() {
        IntArray array = arrayOfFirstInts(3);
        array.truncate(1);

        assertThatThrownBy(() -> array.get(1)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> array.set(1, 0)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> array.truncate(2)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    /** An array holding 0, 1, ... up to {@code count - 1}. */
    private static IntArray arrayOfFirstInts(int count) {
        IntArray array = new IntArray();
        for (int i = 0; i < count; i++) {
            array.add(i);
        }
        return array;
    }
}
