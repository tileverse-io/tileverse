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
