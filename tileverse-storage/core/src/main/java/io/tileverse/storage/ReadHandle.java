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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import org.jspecify.annotations.NullMarked;

/**
 * Returned by {@code Storage.read(String, ReadOptions)}: a streaming body together with the post-fetch object metadata.
 * Closing the handle closes the underlying stream and releases the connection.
 *
 * <p>Cloud SDKs return both the body and the response headers (ETag, content type, last-modified, version) from a
 * single GET. {@link ReadHandle} surfaces both so callers do not need a separate stat round trip.
 */
@NullMarked
public record ReadHandle(InputStream content, StorageEntry.File metadata) implements Closeable {

    public ReadHandle {
        if (content == null) throw new IllegalArgumentException("content is required");
        if (metadata == null) throw new IllegalArgumentException("metadata is required");
    }

    @Override
    public void close() throws IOException {
        content.close();
    }
}
