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
package io.tileverse.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.OptionalLong;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ContentRangeTest {

    static Stream<Arguments> headers() {
        return Stream.of(
                Arguments.of("bytes 0-9/1234", OptionalLong.of(1234L)),
                Arguments.of("bytes */5", OptionalLong.of(5L)),
                Arguments.of("bytes 0-9/*", OptionalLong.empty()),
                Arguments.of("bytes 0-9", OptionalLong.empty()),
                Arguments.of("garbage", OptionalLong.empty()),
                Arguments.of("bytes 0-9/notanumber", OptionalLong.empty()),
                Arguments.of("", OptionalLong.empty()),
                Arguments.of(null, OptionalLong.empty()));
    }

    @ParameterizedTest
    @MethodSource("headers")
    void totalOf(String header, OptionalLong expected) {
        assertThat(ContentRange.totalOf(header)).isEqualTo(expected);
    }

    @Test
    void bytesOfParsesASatisfiedRange() {
        ContentRange.Bytes parsed = ContentRange.bytesOf("bytes 100-199/1234").orElseThrow();
        assertThat(parsed.firstPos()).isEqualTo(100);
        assertThat(parsed.lastPos()).isEqualTo(199);
        assertThat(parsed.length()).isEqualTo(100);
        assertThat(parsed.total()).hasValue(1234);
    }

    @Test
    void bytesOfAcceptsAnUnknownTotal() {
        ContentRange.Bytes parsed = ContentRange.bytesOf("bytes 0-9/*").orElseThrow();
        assertThat(parsed.length()).isEqualTo(10);
        assertThat(parsed.total()).isEmpty();
    }

    @Test
    void bytesOfRejectsUnsatisfiedAndMalformedValues() {
        assertThat(ContentRange.bytesOf(null)).isEmpty();
        assertThat(ContentRange.bytesOf("bytes */1234")).isEmpty();
        assertThat(ContentRange.bytesOf("garbage")).isEmpty();
        assertThat(ContentRange.bytesOf("bytes 199-100/1234")).isEmpty();
        assertThat(ContentRange.bytesOf("bytes 100-199")).isEmpty();
    }
}
