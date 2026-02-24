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

    public static Builder builder() {
        return new Builder();
    }

    public static CoreParquetReadOptions defaults() {
        return builder().build();
    }

    public boolean useStatsFilter() {
        return useStatsFilter;
    }

    public boolean useDictionaryFilter() {
        return useDictionaryFilter;
    }

    public boolean useColumnIndexFilter() {
        return useColumnIndexFilter;
    }

    public FilterCompat.Filter getRecordFilter() {
        return recordFilter;
    }

    public Builder toBuilder() {
        return builder()
                .useStatsFilter(useStatsFilter)
                .useDictionaryFilter(useDictionaryFilter)
                .useColumnIndexFilter(useColumnIndexFilter)
                .withRecordFilter(recordFilter);
    }

    public static final class Builder {
        private boolean useStatsFilter = true;
        private boolean useDictionaryFilter = true;
        private boolean useColumnIndexFilter = true;
        private FilterCompat.Filter recordFilter = FilterCompat.NOOP;

        public Builder useStatsFilter(boolean enabled) {
            this.useStatsFilter = enabled;
            return this;
        }

        public Builder useDictionaryFilter(boolean enabled) {
            this.useDictionaryFilter = enabled;
            return this;
        }

        public Builder useColumnIndexFilter(boolean enabled) {
            this.useColumnIndexFilter = enabled;
            return this;
        }

        public Builder withRecordFilter(FilterCompat.Filter filter) {
            this.recordFilter = filter == null ? FilterCompat.NOOP : filter;
            return this;
        }

        public CoreParquetReadOptions build() {
            return new CoreParquetReadOptions(useStatsFilter, useDictionaryFilter, useColumnIndexFilter, recordFilter);
        }
    }
}
