/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class HeapSizeTest {

    @Test
    void roundsToEightBytes() {
        assertThat(HeapSize.object(0)).isEqualTo(16);
        assertThat(HeapSize.object(28)).isEqualTo(40);
        assertThat(HeapSize.array(1, 0)).isEqualTo(16);
        assertThat(HeapSize.array(1, 9)).isEqualTo(32);
        assertThat(HeapSize.array(4, 3)).isEqualTo(32);
    }

    @Test
    void sizesLatin1Strings() {
        String cologne = "K\u00f6ln";

        assertThat(HeapSize.string("abc")).isEqualTo(48);
        assertThat(HeapSize.string(cologne)).isEqualTo(48);
    }

    @Test
    void sizesUtf16Strings() {
        String moscow = "\u041c\u043e\u0441\u043a\u0432\u0430";

        assertThat(HeapSize.string(moscow)).isEqualTo(56);
    }

    @Test
    void sizesBoxedValues() {
        assertThat(HeapSize.value(null)).isZero();
        assertThat(HeapSize.value(Boolean.TRUE)).isZero();
        assertThat(HeapSize.value(Long.valueOf(127))).isZero();
        assertThat(HeapSize.value(Long.valueOf(-128))).isZero();
        assertThat(HeapSize.value(Long.valueOf(128))).isEqualTo(24);
        assertThat(HeapSize.value(Double.valueOf(1.5))).isEqualTo(24);
        assertThat(HeapSize.value(Float.valueOf(1.5f))).isEqualTo(16);
        assertThat(HeapSize.value("abc")).isEqualTo(48);
    }

    @ParameterizedTest
    @MethodSource("otherValueTypes")
    void sizesOtherValueTypes(Object value, long expected) {
        assertThat(HeapSize.value(value)).isEqualTo(expected);
    }

    static Stream<Arguments> otherValueTypes() {
        return Stream.of(
                Arguments.of(Integer.valueOf(1000), 16L),
                Arguments.of(Short.valueOf((short) 1000), 16L),
                Arguments.of(Byte.valueOf((byte) 100), 16L),
                Arguments.of(Character.valueOf('c'), 16L),
                Arguments.of(new BigDecimal("1.10"), HeapSize.string("1.10")),
                // 2^1000 has 1001 bits: 32 ints of magnitude, after the 40 bytes of the object
                Arguments.of(BigInteger.ONE.shiftLeft(1000), 40 + HeapSize.array(Integer.BYTES, 32)),
                // -2^64 has 64 bits in two's complement, and its magnitude 2^64 takes 65 bits: 3 ints
                Arguments.of(BigInteger.ONE.shiftLeft(64).negate(), 40 + HeapSize.array(Integer.BYTES, 3)),
                Arguments.of(new AtomicLong(1000), 24L),
                Arguments.of(LocalDate.of(2026, 11, 5), HeapSize.string("2026-11-05")));
    }
}
