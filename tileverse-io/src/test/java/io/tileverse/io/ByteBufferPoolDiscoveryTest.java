/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.io;

import static org.assertj.core.api.Assertions.assertThat;

import io.tileverse.io.ByteBufferPool.PooledByteBuffer;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link ByteBufferPool#getDefault()} discovers a registered provider through {@code ServiceLoader} and
 * that the static borrow entrypoints route through it. A {@link DelegatingTestByteBufferPool} is registered in
 * {@code src/test/resources/META-INF/services} as the sole provider.
 */
class ByteBufferPoolDiscoveryTest {

    @Test
    void getDefault_usesServiceLoaderProvider() {
        assertThat(ByteBufferPool.getDefault()).isInstanceOf(DelegatingTestByteBufferPool.class);
    }

    @Test
    void directBuffer_routesThroughDiscoveredProvider() {
        try (PooledByteBuffer pooled = ByteBufferPool.directBuffer(2048)) {
            assertThat(pooled.buffer().isDirect()).isTrue();
            assertThat(pooled.buffer().capacity()).isEqualTo(2048);
        }
    }
}
