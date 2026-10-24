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
import org.jspecify.annotations.NullMarked;

/**
 * Options for {@link Storage#presignPut(String, java.time.Duration, PresignWriteOptions) Storage.presignPut}.
 *
 * <p>Holds only the fields that can be embedded in a presigned-URL signature: content type, user metadata, and the
 * create-only precondition. Streaming knobs ({@code disableMultipart}, {@code contentLength}) live on
 * {@link WriteOptions} because they govern client-side upload behavior, not the URL signature itself.
 */
@NullMarked
public record PresignWriteOptions(Optional<String> contentType, Map<String, String> userMetadata, boolean ifNotExists) {

    public PresignWriteOptions {
        contentType = Objects.requireNonNullElse(contentType, Optional.empty());
        userMetadata = (userMetadata == null) ? Map.of() : Map.copyOf(userMetadata);
    }

    public static PresignWriteOptions defaults() {
        return new PresignWriteOptions(Optional.empty(), Map.of(), false);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private Optional<String> contentType = Optional.empty();
        private final Map<String, String> userMetadata = new HashMap<>();
        private boolean ifNotExists;

        public Builder contentType(String contentType) {
            this.contentType = Optional.of(contentType);
            return this;
        }

        public Builder userMetadata(Map<String, String> userMetadata) {
            this.userMetadata.clear();
            this.userMetadata.putAll(userMetadata);
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

        public PresignWriteOptions build() {
            return new PresignWriteOptions(contentType, Map.copyOf(userMetadata), ifNotExists);
        }
    }
}
