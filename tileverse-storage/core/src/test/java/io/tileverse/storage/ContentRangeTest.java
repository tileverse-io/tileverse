/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
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
