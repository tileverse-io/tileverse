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
