/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Options for {@link Storage#put} variants and {@link Storage#openOutputStream(String, WriteOptions)}.
 *
 * @param contentType optional MIME type for the object (backend-specific)
 * @param userMetadata optional key-value user metadata; empty on backends that do not support it
 * @param ifNotExists when true, fails with {@code PreconditionFailedException} if the key already exists
 * @param ifMatchEtag when present, only writes when the existing object's ETag matches (compare-and-set)
 * @param contentLength optional length hint; required by some backends for streaming put
 * @param disableMultipart when true, forces a single-shot upload (no multipart)
 */
public record WriteOptions(
        Optional<String> contentType,
        Map<String, String> userMetadata,
        boolean ifNotExists,
        Optional<String> ifMatchEtag,
        OptionalLong contentLength,
        boolean disableMultipart) {

    public WriteOptions {
        contentType = Objects.requireNonNullElse(contentType, Optional.empty());
        userMetadata = (userMetadata == null) ? Map.of() : Map.copyOf(userMetadata);
        ifMatchEtag = Objects.requireNonNullElse(ifMatchEtag, Optional.empty());
        contentLength = Objects.requireNonNullElse(contentLength, OptionalLong.empty());
    }

    public static WriteOptions defaults() {
        return new WriteOptions(Optional.empty(), Map.of(), false, Optional.empty(), OptionalLong.empty(), false);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private Optional<String> contentType = Optional.empty();
        private Map<String, String> userMetadata = new HashMap<>();
        private boolean ifNotExists;
        private Optional<String> ifMatchEtag = Optional.empty();
        private OptionalLong contentLength = OptionalLong.empty();
        private boolean disableMultipart;

        public Builder contentType(String contentType) {
            this.contentType = Optional.of(contentType);
            return this;
        }

        public Builder userMetadata(Map<String, String> userMetadata) {
            this.userMetadata = new HashMap<>(userMetadata);
            return this;
        }

        public Builder addMetadata(String key, String value) {
            this.userMetadata.put(key, value);
            return this;
        }

        public Builder ifNotExists(boolean ifNotExists) {
            this.ifNotExists = ifNotExists;
            return this;
        }

        public Builder ifMatchEtag(String etag) {
            this.ifMatchEtag = Optional.of(etag);
            return this;
        }

        public Builder contentLength(long contentLength) {
            this.contentLength = OptionalLong.of(contentLength);
            return this;
        }

        public Builder disableMultipart(boolean disableMultipart) {
            this.disableMultipart = disableMultipart;
            return this;
        }

        public WriteOptions build() {
            return new WriteOptions(
                    contentType, userMetadata, ifNotExists, ifMatchEtag, contentLength, disableMultipart);
        }
    }
}
