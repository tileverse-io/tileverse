/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class StorageEntryTest {

    @Test
    void fileExposesAllMetadataFields() {
        Instant now = Instant.now();
        StorageEntry.File file = new StorageEntry.File(
                "data/file.parquet",
                12345L,
                now,
                Optional.of("etag-abc"),
                Optional.empty(),
                Optional.of("application/octet-stream"),
                Map.of("source", "test"));
        assertThat(file.key()).isEqualTo("data/file.parquet");
        assertThat(file.size()).isEqualTo(12345L);
        assertThat(file.lastModified()).isEqualTo(now);
        assertThat(file.etag()).contains("etag-abc");
        assertThat(file.userMetadata()).containsEntry("source", "test");
    }

    @Test
    void prefixIsKeyOnly() {
        StorageEntry.Prefix p = new StorageEntry.Prefix("year=2024/");
        assertThat(p.key()).isEqualTo("year=2024/");
    }

    @Test
    void directoryHasOptionalLastModified() {
        StorageEntry.Directory d = new StorageEntry.Directory("dir/", Optional.empty());
        assertThat(d.key()).isEqualTo("dir/");
        assertThat(d.lastModified()).isEmpty();
    }

    @Test
    void sealedHierarchyExhausts() {
        StorageEntry e = new StorageEntry.File(
                "k", 1L, Instant.EPOCH, Optional.empty(), Optional.empty(), Optional.empty(), Map.of());
        String label;
        if (e instanceof StorageEntry.File) {
            label = "file";
        } else if (e instanceof StorageEntry.Prefix) {
            label = "prefix";
        } else if (e instanceof StorageEntry.Directory) {
            label = "directory";
        } else {
            throw new AssertionError("unreachable - sealed");
        }
        assertThat(label).isEqualTo("file");
    }

    @Test
    void fileVersionIdDefaultsToEmpty() {
        StorageEntry.File f = new StorageEntry.File(
                "k",
                10L,
                java.time.Instant.now(),
                java.util.Optional.empty(),
                java.util.Optional.empty(),
                java.util.Optional.empty(),
                java.util.Map.of());
        assertThat(f.versionId()).isEmpty();
    }

    @Test
    void fileVersionIdRoundTrips() {
        StorageEntry.File f = new StorageEntry.File(
                "k",
                10L,
                java.time.Instant.now(),
                java.util.Optional.of("\"abc\""),
                java.util.Optional.of("v123"),
                java.util.Optional.empty(),
                java.util.Map.of());
        assertThat(f.versionId()).hasValue("v123");
    }
}
