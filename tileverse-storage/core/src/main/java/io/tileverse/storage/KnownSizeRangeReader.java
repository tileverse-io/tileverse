/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Forwarding {@link RangeReader} that answers {@link #size()} and {@link #version()} from a storage entry, typically
 * obtained from a listing or a stat, saving the delegate's lazy metadata request. An entry without a version leaves
 * {@link #version()} to the delegate. Everything else, the source identity included, forwards to the delegate
 * unchanged.
 *
 * <p>The size and the version are those of the entry when it was read. If the object is replaced afterwards, reads
 * return whatever the backend serves for the requested offsets.
 */
final class KnownSizeRangeReader implements RangeReader {

    private final RangeReader delegate;
    private final long size;
    private final Optional<String> entryVersion;

    private KnownSizeRangeReader(RangeReader delegate, StorageEntry.File entry) {
        this.delegate = delegate;
        this.size = entry.size();
        this.entryVersion = versionOf(entry);
    }

    static RangeReader of(RangeReader delegate, StorageEntry.File entry) {
        return new KnownSizeRangeReader(delegate, entry);
    }

    /**
     * Returns the version token of an entry: its ETag when present, else its version id, else its size and
     * last-modified time.
     *
     * <p>A backend reports {@link Instant#EPOCH} when it does not know the last-modified time. The size alone cannot
     * tell two versions apart, hence such an entry has no version.
     */
    static Optional<String> versionOf(StorageEntry.File entry) {
        if (entry.etag().isPresent()) {
            return entry.etag();
        }
        if (entry.versionId().isPresent()) {
            return entry.versionId();
        }
        Instant lastModified = entry.lastModified();
        if (lastModified.isAfter(Instant.EPOCH)) {
            return Optional.of(entry.size() + "-" + lastModified.toEpochMilli());
        }
        return Optional.empty();
    }

    @Override
    public int readRange(long offset, int length, ByteBuffer target) {
        return delegate.readRange(offset, length, target);
    }

    @Override
    public BatchReadResult readRanges(List<RangeRequest> requests) {
        return delegate.readRanges(requests);
    }

    @Override
    public OptionalLong size() {
        return OptionalLong.of(size);
    }

    @Override
    public Optional<String> version() {
        if (entryVersion.isPresent()) {
            return entryVersion;
        }
        return delegate.version();
    }

    @Override
    public String getSourceIdentifier() {
        return delegate.getSourceIdentifier();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
