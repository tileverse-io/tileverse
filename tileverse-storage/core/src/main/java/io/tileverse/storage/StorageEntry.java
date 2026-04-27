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

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.jspecify.annotations.NullMarked;

/**
 * An entry returned by {@link Storage#list(String, ListOptions)}.
 *
 * <ul>
 *   <li>{@link File} - an actual blob/object with size, etag, and version metadata.
 *   <li>{@link Prefix} - a synthetic common prefix from hierarchical listing on flat-keyspace backends (S3, Azure Blob,
 *       GCS flat).
 *   <li>{@link Directory} - a real directory entry from local FS, Azure DataLake Gen2 (HNS), or GCS HNS.
 * </ul>
 *
 * <p><b>Listing entry contract:</b> when {@link StorageCapabilities#realDirectories()} is {@code true} (file, ADLS
 * Gen2, GCS HNS), non-recursive listings emit {@link Directory} entries for sub-folders. When {@code false} (S3, Azure
 * Blob, flat GCS), they emit {@link Prefix} entries instead. Recursive listings (a glob containing {@code **}) emit
 * only {@link File} entries on every backend.
 *
 * <p>Callers that don't care about the distinction between Prefix and Directory can pattern-match on {@link File} only.
 */
@NullMarked
public sealed interface StorageEntry {

    String key();

    /**
     * A blob or object entry returned by {@link Storage#list} or {@link Storage#stat}.
     *
     * <ul>
     *   <li>{@code etag} - provider ETag or equivalent opaque change-detection token; empty when not available.
     *   <li>{@code versionId} - prior-version identifier when bucket/container versioning is enabled; empty otherwise.
     *   <li>{@code contentType} - MIME type of the object; empty when not reported by the backend.
     * </ul>
     */
    record File(
            String key,
            long size,
            Instant lastModified,
            Optional<String> etag,
            Optional<String> versionId,
            Optional<String> contentType,
            Map<String, String> userMetadata)
            implements StorageEntry {

        public File {
            if (key == null) throw new IllegalArgumentException("key is required");
            if (lastModified == null) throw new IllegalArgumentException("lastModified is required");
            if (size < 0L) throw new IllegalArgumentException("size must be >= 0");
            etag = (etag != null) ? etag : Optional.empty();
            versionId = (versionId == null) ? Optional.empty() : versionId;
            contentType = (contentType == null) ? Optional.empty() : contentType;
            userMetadata = (userMetadata == null) ? Map.of() : Map.copyOf(userMetadata);
        }
    }

    record Prefix(String key) implements StorageEntry {
        public Prefix {
            if (key == null) throw new IllegalArgumentException("key is required");
        }
    }

    record Directory(String key, Optional<Instant> lastModified) implements StorageEntry {
        public Directory {
            if (key == null) throw new IllegalArgumentException("key is required");
            lastModified = (lastModified == null) ? Optional.empty() : lastModified;
        }
    }
}
