/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tileverse.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class ByteRangeTest {

    @Test
    void endIsExclusive() {
        assertThat(ByteRange.of(100, 50).end()).isEqualTo(150L);
        assertThat(ByteRange.of(0, 0).end()).isZero();
    }

    @Test
    void containsIsHalfOpen() {
        ByteRange range = ByteRange.of(100, 50);
        assertThat(range.contains(100)).isTrue();
        assertThat(range.contains(149)).isTrue();
        assertThat(range.contains(150)).isFalse();
        assertThat(range.contains(99)).isFalse();
        assertThat(ByteRange.of(100, 0).contains(100)).isFalse();
    }

    @Test
    void overlapsIsSymmetricAndHalfOpen() {
        ByteRange range = ByteRange.of(100, 50);
        assertThat(range.overlaps(ByteRange.of(149, 10))).isTrue();
        assertThat(range.overlaps(ByteRange.of(150, 10))).isFalse();
        assertThat(range.overlaps(ByteRange.of(90, 10))).isFalse();
        assertThat(range.overlaps(ByteRange.of(90, 11))).isTrue();
        assertThat(range.overlaps(ByteRange.of(120, 0))).isFalse();
        assertThat(ByteRange.of(120, 0).overlaps(range)).isFalse();
    }

    @Test
    void negativeLengthMessageNamesTheLength() {
        assertThatThrownBy(() -> ByteRange.of(5, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("-1");
    }
}
