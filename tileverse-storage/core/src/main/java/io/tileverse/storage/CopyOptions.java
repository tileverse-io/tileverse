/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Options for {@link Storage#copy} and {@link Storage#move} variants.
 *
 * @param ifMatchSourceEtag conditional copy: only proceed when the source ETag matches
 * @param ifNotExistsAtDestination when true, fails with {@code PreconditionFailedException} if the destination exists
 * @param overrideUserMetadata when present, replaces the source metadata at the destination instead of inheriting
 * @param preserveLastModified best-effort attempt to preserve the source's lastModified timestamp
 */
public record CopyOptions(
        Optional<String> ifMatchSourceEtag,
        boolean ifNotExistsAtDestination,
        Optional<Map<String, String>> overrideUserMetadata,
        boolean preserveLastModified) {

    public CopyOptions {
        ifMatchSourceEtag = Objects.requireNonNullElse(ifMatchSourceEtag, Optional.empty());
        overrideUserMetadata = Objects.requireNonNullElse(overrideUserMetadata, Optional.<Map<String, String>>empty())
                .map(Map::copyOf);
    }

    public static CopyOptions defaults() {
        return new CopyOptions(Optional.empty(), false, Optional.empty(), false);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private Optional<String> ifMatchSourceEtag = Optional.empty();
        private boolean ifNotExistsAtDestination = false;
        private Optional<Map<String, String>> overrideUserMetadata = Optional.empty();
        private boolean preserveLastModified = false;

        private Builder() {}

        public Builder ifMatchSourceEtag(String sourceEtag) {
            this.ifMatchSourceEtag = Optional.ofNullable(sourceEtag);
            return this;
        }

        public Builder ifNotExistsAtDestination(boolean ifNotExistsAtDestination) {
            this.ifNotExistsAtDestination = ifNotExistsAtDestination;
            return this;
        }

        public Builder overrideUserMetadata(Map<String, String> overrideUserMetadata) {
            this.overrideUserMetadata = Optional.ofNullable(overrideUserMetadata);
            return this;
        }

        public Builder preserveLastModified(boolean preserveLastModified) {
            this.preserveLastModified = preserveLastModified;
            return this;
        }

        public CopyOptions build() {
            return new CopyOptions(
                    ifMatchSourceEtag, ifNotExistsAtDestination, overrideUserMetadata, preserveLastModified);
        }
    }
}
