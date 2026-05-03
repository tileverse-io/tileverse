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
package io.tileverse.storage.s3;

import io.tileverse.storage.Storage;
import java.net.URI;

/**
 * Parsed S3 root URI for a {@link S3Storage}: bucket plus an optional key prefix. The prefix never starts with '/' and
 * ends with '/' if non-empty.
 */
record S3StorageBucketKey(String bucket, String prefix) {

    static S3StorageBucketKey parse(URI uri) {
        S3Reference ref = S3CompatibleUrlParser.parseS3Url(uri);
        if (ref.bucket() == null || ref.bucket().isBlank()) {
            throw new IllegalArgumentException("S3 URI is missing a bucket: " + uri);
        }
        String key = ref.key() == null ? "" : ref.key();
        // Normalize: strip leading slash, ensure trailing slash if non-empty
        while (key.startsWith("/")) key = key.substring(1);
        if (!key.isEmpty() && !key.endsWith("/")) {
            key = key + "/";
        }
        return new S3StorageBucketKey(ref.bucket(), key);
    }

    /**
     * Resolve a relative key to a full S3 key (prefix + relative). Validates the key with
     * {@link Storage#requireSafeKey}: rejects null, empty, leading slash, NUL bytes, and {@code ..}/{@code .} segments.
     * Callers that need to address the bare prefix (e.g. listing the root) should concatenate {@link #prefix()}
     * directly without going through this helper.
     */
    String resolve(String relativeKey) {
        Storage.requireSafeKey(relativeKey);
        return prefix + relativeKey;
    }

    /** Strip the prefix from a full key to recover the relative key. */
    String relativize(String fullKey) {
        if (prefix.isEmpty()) return fullKey;
        if (!fullKey.startsWith(prefix)) {
            throw new IllegalArgumentException("Key '" + fullKey + "' does not start with prefix '" + prefix + "'");
        }
        return fullKey.substring(prefix.length());
    }
}
