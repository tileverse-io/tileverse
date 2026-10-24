/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage;

import java.util.OptionalInt;

/**
 * Tuning knobs for {@link Storage#list(String, ListOptions)}.
 *
 * <p><b>Note:</b> traversal scope and glob filtering live on the {@code pattern} argument to {@code list}, not here.
 * See {@link StoragePattern} for how the pattern is parsed.
 *
 * @param pageSize backend pagination hint; empty means use the backend's default
 * @param includeUserMetadata when true, listing includes user metadata (may be expensive on backends that require a
 *     HEAD per entry)
 */
public record ListOptions(OptionalInt pageSize, boolean includeUserMetadata) {

    public ListOptions {
        pageSize = (pageSize == null) ? OptionalInt.empty() : pageSize;
    }

    public static ListOptions defaults() {
        return new ListOptions(OptionalInt.empty(), false);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private OptionalInt pageSize = OptionalInt.empty();
        private boolean includeUserMetadata;

        public Builder pageSize(int v) {
            this.pageSize = OptionalInt.of(v);
            return this;
        }

        public Builder includeUserMetadata(boolean v) {
            this.includeUserMetadata = v;
            return this;
        }

        public ListOptions build() {
            return new ListOptions(pageSize, includeUserMetadata);
        }
    }
}
