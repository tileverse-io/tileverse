/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
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
