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
package io.tileverse.parquet.reader;

import org.apache.parquet.filter2.compat.FilterCompat;

/**
 * Immutable configuration for Parquet read behavior.
 *
 * <p>Three filter toggles control which pushdown tiers are enabled during reads:
 *
 * <ul>
 *   <li>{@code useStatsFilter} — row group pruning via column statistics (default: {@code true})
 *   <li>{@code useDictionaryFilter} — row group pruning via dictionary pages (default: {@code true})
 *   <li>{@code useColumnIndexFilter} — page-level skipping via column/offset indexes (default: {@code true})
 * </ul>
 *
 * <p>Use {@link #builder()} for customized options or {@link #defaults()} for the default configuration with all
 * filters enabled.
 */
public final class CoreParquetReadOptions {
    private final boolean useStatsFilter;
    private final boolean useDictionaryFilter;
    private final boolean useColumnIndexFilter;
    private final FilterCompat.Filter recordFilter;

    private CoreParquetReadOptions(
            boolean useStatsFilter,
            boolean useDictionaryFilter,
            boolean useColumnIndexFilter,
            FilterCompat.Filter recordFilter) {
        this.useStatsFilter = useStatsFilter;
        this.useDictionaryFilter = useDictionaryFilter;
        this.useColumnIndexFilter = useColumnIndexFilter;
        this.recordFilter = recordFilter == null ? FilterCompat.NOOP : recordFilter;
    }

    /** Returns a new builder with all defaults. */
    public static Builder builder() {
        return new Builder();
    }

    /** Returns options with all filter tiers enabled and no record filter. */
    public static CoreParquetReadOptions defaults() {
        return builder().build();
    }

    /** Returns whether statistics-based row group pruning is enabled. */
    public boolean useStatsFilter() {
        return useStatsFilter;
    }

    /** Returns whether dictionary-based row group pruning is enabled. */
    public boolean useDictionaryFilter() {
        return useDictionaryFilter;
    }

    /** Returns whether column-index page-level filtering is enabled. */
    public boolean useColumnIndexFilter() {
        return useColumnIndexFilter;
    }

    /** Returns the record-level filter, or {@link FilterCompat#NOOP} if none. */
    public FilterCompat.Filter getRecordFilter() {
        return recordFilter;
    }

    /** Returns a new builder pre-populated with this instance's values. */
    public Builder toBuilder() {
        return builder()
                .useStatsFilter(useStatsFilter)
                .useDictionaryFilter(useDictionaryFilter)
                .useColumnIndexFilter(useColumnIndexFilter)
                .withRecordFilter(recordFilter);
    }

    /** Builder for {@link CoreParquetReadOptions}. */
    public static final class Builder {
        private boolean useStatsFilter = true;
        private boolean useDictionaryFilter = true;
        private boolean useColumnIndexFilter = true;
        private FilterCompat.Filter recordFilter = FilterCompat.NOOP;

        /** Enables or disables statistics-based row group pruning. */
        public Builder useStatsFilter(boolean enabled) {
            this.useStatsFilter = enabled;
            return this;
        }

        /** Enables or disables dictionary-based row group pruning. */
        public Builder useDictionaryFilter(boolean enabled) {
            this.useDictionaryFilter = enabled;
            return this;
        }

        /** Enables or disables column-index page-level filtering. */
        public Builder useColumnIndexFilter(boolean enabled) {
            this.useColumnIndexFilter = enabled;
            return this;
        }

        /** Sets the record-level filter applied during assembly. */
        public Builder withRecordFilter(FilterCompat.Filter filter) {
            this.recordFilter = filter == null ? FilterCompat.NOOP : filter;
            return this;
        }

        /** Builds an immutable {@link CoreParquetReadOptions} instance. */
        public CoreParquetReadOptions build() {
            return new CoreParquetReadOptions(useStatsFilter, useDictionaryFilter, useColumnIndexFilter, recordFilter);
        }
    }
}
