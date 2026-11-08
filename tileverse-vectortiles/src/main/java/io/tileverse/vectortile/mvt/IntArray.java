/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import java.util.Arrays;
import java.util.Objects;

/**
 * A growing array of ints, such as the geometry commands or the tags of a feature. It holds the values unboxed, and
 * {@link #clear()} keeps its array: an instance reused from feature to feature stops allocating once it fits the
 * largest feature.
 */
final class IntArray {

    private static final int INITIAL_CAPACITY = 16;

    private int[] values = new int[INITIAL_CAPACITY];
    private int size;

    /** Appends the value. A full array is copied into one twice as long. */
    void add(int value) {
        if (size == values.length) {
            values = Arrays.copyOf(values, 2 * values.length);
        }
        values[size++] = value;
    }

    /**
     * Returns the value at the index.
     *
     * @throws IndexOutOfBoundsException if the index is negative or at least {@link #size()}
     */
    int get(int index) {
        Objects.checkIndex(index, size);
        return values[index];
    }

    /**
     * Replaces the value at the index.
     *
     * @throws IndexOutOfBoundsException if the index is negative or at least {@link #size()}
     */
    void set(int index, int value) {
        Objects.checkIndex(index, size);
        values[index] = value;
    }

    /** The number of values. */
    int size() {
        return size;
    }

    /** Whether the array holds no value. */
    boolean isEmpty() {
        return size == 0;
    }

    /** Removes the values. The instance keeps its array for the next values. */
    void clear() {
        size = 0;
    }

    /**
     * Keeps the first {@code size} values and removes the others.
     *
     * @throws IndexOutOfBoundsException if {@code size} is negative or above {@link #size()}
     */
    void truncate(int size) {
        Objects.checkFromToIndex(0, size, this.size);
        this.size = size;
    }

    /**
     * The backing array: its first {@link #size()} elements are the values. {@link #add} replaces it with a longer
     * array when it is full.
     */
    int[] array() {
        return values;
    }

    /** The length of the backing array, at least {@link #size()}. */
    int capacity() {
        return values.length;
    }
}
