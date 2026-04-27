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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class StorageCapabilitiesTest {

    @Test
    void rangeReadOnlyHasCorrectFlags() {
        StorageCapabilities c = StorageCapabilities.rangeReadOnly();
        assertThat(c.rangeReads()).isTrue();
        assertThat(c.streamingReads()).isTrue();
        assertThat(c.list()).isFalse();
        assertThat(c.writes()).isFalse();
        assertThat(c.serverSideCopy()).isFalse();
        assertThat(c.atomicMove()).isFalse();
        assertThat(c.presignedUrls()).isFalse();
    }

    @Test
    void requireListThrowsWhenListUnsupported() {
        StorageCapabilities c = StorageCapabilities.rangeReadOnly();
        assertThatThrownBy(c::requireList)
                .isInstanceOf(UnsupportedCapabilityException.class)
                .hasMessageContaining("list");
    }

    @Test
    void requireWritesThrowsWhenWritesUnsupported() {
        StorageCapabilities c = StorageCapabilities.rangeReadOnly();
        assertThatThrownBy(c::requireWrites)
                .isInstanceOf(UnsupportedCapabilityException.class)
                .hasMessageContaining("writes");
    }

    @Test
    void builderProducesSupportedCapabilities() {
        StorageCapabilities c = StorageCapabilities.builder()
                .rangeReads(true)
                .streamingReads(true)
                .stat(true)
                .list(true)
                .hierarchicalList(true)
                .writes(true)
                .multipartUpload(true)
                .multipartThresholdBytes(8L * 1024 * 1024)
                .conditionalWrite(true)
                .bulkDelete(true)
                .bulkDeleteBatchLimit(1000)
                .serverSideCopy(true)
                .atomicMove(false)
                .presignedUrls(true)
                .maxPresignTtl(Optional.of(Duration.ofDays(7)))
                .strongReadAfterWrite(true)
                .build();
        assertThat(c.list()).isTrue();
        assertThat(c.bulkDeleteBatchLimit()).isEqualTo(1000);
        assertThat(c.maxPresignTtl()).contains(Duration.ofDays(7));
    }
}
