/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import org.jspecify.annotations.NullMarked;

/**
 * Returned by {@code Storage.read(String, ReadOptions)}: a streaming body together with the post-fetch object metadata.
 * Closing the handle closes the underlying stream and releases the connection.
 *
 * <p>Cloud SDKs return both the body and the response headers (ETag, content type, last-modified, version) from a
 * single GET. {@link ReadHandle} exposes both so callers do not need a separate stat round trip.
 */
@NullMarked
public record ReadHandle(InputStream content, StorageEntry.File metadata) implements Closeable {

    public ReadHandle {
        if (content == null) {
            throw new IllegalArgumentException("content is required");
        }
        if (metadata == null) {
            throw new IllegalArgumentException("metadata is required");
        }
    }

    @Override
    public void close() throws IOException {
        content.close();
    }
}
