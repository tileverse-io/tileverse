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
import java.nio.ByteBuffer;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Verifies the pluggable allocation backend: provider selection, that an explicitly supplied
 * {@link ByteBufferAllocator} receives all allocation and free calls, the built-in fallback, and {@code ServiceLoader}
 * discovery on the default pool.
 */
class ByteBufferAllocatorTest {

    @Test
    void selectAllocator_noProviders_returnsBuiltin() {
        ByteBufferAllocator selected = ByteBufferPool.selectAllocator(List.of());

        assertThat(selected).isInstanceOf(BuiltinByteBufferAllocator.class);
    }

    @Test
    void selectAllocator_singleProvider_returnsIt() {
        CountingByteBufferAllocator provider = new CountingByteBufferAllocator();

        assertThat(ByteBufferPool.selectAllocator(List.of(provider))).isSameAs(provider);
    }

    @Test
    void selectAllocator_multipleProviders_returnsFirst() {
        CountingByteBufferAllocator first = new CountingByteBufferAllocator();
        CountingByteBufferAllocator second = new CountingByteBufferAllocator();

        assertThat(ByteBufferPool.selectAllocator(List.of(first, second))).isSameAs(first);
    }

    @Test
    void explicitAllocator_routesDirectAllocationAndFree() {
        CountingByteBufferAllocator allocator = new CountingByteBufferAllocator();
        ByteBufferPool pool = new ByteBufferPool(2, 2, 1024, allocator);

        try (PooledByteBuffer pooled = pool.borrowDirect(1024)) {
            assertThat(pooled.buffer().isDirect()).isTrue();
        }

        assertThat(allocator.directAllocations()).isEqualTo(1);
        assertThat(allocator.frees()).isZero(); // returned to the pool, not freed

        pool.clear(); // the pooled buffer is released through the allocator

        assertThat(allocator.frees()).isEqualTo(1);
    }

    @Test
    void explicitAllocator_routesHeapAllocation() {
        CountingByteBufferAllocator allocator = new CountingByteBufferAllocator();
        ByteBufferPool pool = new ByteBufferPool(2, 2, 1024, allocator);

        try (PooledByteBuffer pooled = pool.borrowHeap(1024)) {
            assertThat(pooled.buffer().isDirect()).isFalse();
        }

        assertThat(allocator.heapAllocations()).isEqualTo(1);
    }

    @Test
    void builtinAllocator_allocatesBothKindsAndFreeToleratesEach() {
        BuiltinByteBufferAllocator allocator = BuiltinByteBufferAllocator.INSTANCE;

        ByteBuffer direct = allocator.allocateDirect(1024);
        ByteBuffer heap = allocator.allocateHeap(1024);

        assertThat(direct.isDirect()).isTrue();
        assertThat(heap.isDirect()).isFalse();

        allocator.free(heap); // no-op for heap, must not throw
        allocator.free(direct); // releases native memory, must not throw
    }

    @Test
    void getDefault_usesServiceLoaderProvider() {
        // src/test/resources/META-INF/services registers CountingByteBufferAllocator as the sole provider
        assertThat(ByteBufferPool.getDefault().allocator()).isInstanceOf(CountingByteBufferAllocator.class);
    }
}
