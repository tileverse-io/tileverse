/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import org.junit.jupiter.api.Test;

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

    @Test
    void rejectsOtherValueTypes() {
        Integer value = Integer.valueOf(1000);

        assertThatIllegalArgumentException().isThrownBy(() -> HeapSize.value(value));
    }
}
