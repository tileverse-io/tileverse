/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import io.tileverse.vectortile.model.VectorTile;
import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Sizes of the objects held by a decoded tile, for {@link VectorTile#heapSize()}, and by a tile under construction, for
 * {@link VectorTileBuilder#heapSize()}. The sizes follow the object layout of 64-bit HotSpot with compressed
 * references: 12-byte object headers, 16-byte array headers, 4-byte references, and sizes rounded up to a multiple of 8
 * bytes. A string takes one byte per char when each char is at most 0xFF, as with the compact strings of the JDK.
 */
final class HeapSize {

    static final int OBJECT_HEADER = 12;
    static final int ARRAY_HEADER = 16;
    static final int REFERENCE = 4;

    private static final int ALIGNMENT = 8;

    /** A String object: a reference to its byte array, the coder byte, the hash int and the hashIsZero boolean. */
    private static final long STRING_OBJECT = object(REFERENCE + 1 + Integer.BYTES + 1);

    /** A BigInteger object: the signum, a reference to its magnitude array, and four cached ints. */
    private static final long BIG_INTEGER_OBJECT = object(Integer.BYTES + REFERENCE + 4 * Integer.BYTES);

    private HeapSize() {}

    /** The size of an object with {@code fieldBytes} bytes of fields. */
    static long object(int fieldBytes) {
        return roundUpToAlignment(OBJECT_HEADER + (long) fieldBytes);
    }

    /** The size of an array of {@code length} elements of {@code elementBytes} bytes each. */
    static long array(int elementBytes, int length) {
        return roundUpToAlignment(ARRAY_HEADER + elementBytes * (long) length);
    }

    private static long roundUpToAlignment(long size) {
        return (size + ALIGNMENT - 1) / ALIGNMENT * ALIGNMENT;
    }

    /** The size of a string and of its byte array. */
    static long string(String value) {
        int bytesPerChar = isLatin1(value) ? 1 : 2;
        return STRING_OBJECT + array(bytesPerChar, value.length());
    }

    private static boolean isLatin1(String value) {
        for (int i = 0; i < value.length(); i++) {
            if (value.charAt(i) > 0xFF) {
                return false;
            }
        }
        return true;
    }

    /**
     * The size of a value of a layer: one returned by {@link LayerIndex#value(int)}, or one given to
     * {@link FeatureBuilder#attribute(String, Object)}. The index boxes its values with {@code valueOf}. Shared
     * instances count zero: the two Booleans, and the Longs from -128 to 127. An unsigned value of the index counts as
     * a box of a long.
     *
     * <p>A box of an int, a short, a byte or a char counts as an object with four bytes of fields, and another number
     * as a box of a long. A {@code BigInteger} counts with its magnitude, an array of ints. A {@code BigDecimal} and a
     * value of another type count as their text.
     */
    static long value(Object value) {
        if (value == null || value instanceof Boolean) {
            return 0;
        }
        if (value instanceof Long number) {
            return boxedLong(number);
        }
        if (value instanceof OpenLayer.UnsignedValue) {
            return object(Long.BYTES);
        }
        if (value instanceof Double) {
            return object(Double.BYTES);
        }
        if (value instanceof Float) {
            return object(Float.BYTES);
        }
        if (value instanceof String text) {
            return string(text);
        }
        return givenValue(value);
    }

    private static long boxedLong(Long number) {
        long primitive = number.longValue();
        boolean shared = primitive >= -128 && primitive <= 127;
        return shared ? 0 : object(Long.BYTES);
    }

    /** The size of a value of a type never returned by the index: a value given to a feature builder. */
    private static long givenValue(Object value) {
        if (isBoxOfIntOrSmaller(value)) {
            return object(Integer.BYTES);
        }
        if (value instanceof BigInteger number) {
            return bigInteger(number);
        }
        // A BigDecimal grows with its digits, as its text does
        if (value instanceof Number && !(value instanceof BigDecimal)) {
            return object(Long.BYTES);
        }
        return string(value.toString());
    }

    /**
     * The size of a BigInteger and of its magnitude: an array of ints, one per 32 bits of the absolute value. The bit
     * length of a negative power of two is one bit short of its magnitude: -2^64 has 64 bits, and 2^64 takes 65.
     */
    private static long bigInteger(BigInteger value) {
        int magnitudeInts = (value.abs().bitLength() + Integer.SIZE - 1) / Integer.SIZE;
        return BIG_INTEGER_OBJECT + array(Integer.BYTES, magnitudeInts);
    }

    private static boolean isBoxOfIntOrSmaller(Object value) {
        return value instanceof Integer
                || value instanceof Short
                || value instanceof Byte
                || value instanceof Character;
    }
}
