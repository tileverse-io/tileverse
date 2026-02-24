/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 */
package io.tileverse.parquet.reader;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.junit.jupiter.api.Test;

class ParquetReadOptionsAdapterTest {

    @Test
    void fromObject_nullReturnsDefaults() {
        CoreParquetReadOptions options = ParquetReadOptionsAdapter.fromObject(null);
        assertThat(options.useStatsFilter()).isTrue();
        assertThat(options.useDictionaryFilter()).isTrue();
        assertThat(options.useColumnIndexFilter()).isTrue();
        assertThat(options.getRecordFilter()).isSameAs(FilterCompat.NOOP);
    }

    @Test
    void fromObject_withMatchingMethodsReadsAllValues() {
        FilterCompat.Filter filter = FilterCompat.get(FilterApi.lt(FilterApi.intColumn("id"), 5));
        FakeOptions fake = new FakeOptions(false, false, false, filter);

        CoreParquetReadOptions options = ParquetReadOptionsAdapter.fromObject(fake);

        assertThat(options.useStatsFilter()).isFalse();
        assertThat(options.useDictionaryFilter()).isFalse();
        assertThat(options.useColumnIndexFilter()).isFalse();
        assertThat(options.getRecordFilter()).isSameAs(filter);
    }

    @Test
    void fromObject_withMissingMethodsFallsBackToDefaults() {
        CoreParquetReadOptions options = ParquetReadOptionsAdapter.fromObject(new Object());
        assertThat(options.useStatsFilter()).isTrue();
        assertThat(options.useDictionaryFilter()).isTrue();
        assertThat(options.useColumnIndexFilter()).isTrue();
        assertThat(options.getRecordFilter()).isSameAs(FilterCompat.NOOP);
    }

    @Test
    void fromObject_whenMethodThrowsFallsBackForThatValue() {
        ThrowingOptions fake = new ThrowingOptions();
        CoreParquetReadOptions options = ParquetReadOptionsAdapter.fromObject(fake);

        assertThat(options.useStatsFilter()).isTrue();
        assertThat(options.useDictionaryFilter()).isTrue();
        assertThat(options.useColumnIndexFilter()).isTrue();
        assertThat(options.getRecordFilter()).isSameAs(FilterCompat.NOOP);
    }

    static final class FakeOptions {
        private final boolean stats;
        private final boolean dict;
        private final boolean index;
        private final FilterCompat.Filter filter;

        FakeOptions(boolean stats, boolean dict, boolean index, FilterCompat.Filter filter) {
            this.stats = stats;
            this.dict = dict;
            this.index = index;
            this.filter = filter;
        }

        public boolean useStatsFilter() {
            return stats;
        }

        public boolean useDictionaryFilter() {
            return dict;
        }

        public boolean useColumnIndexFilter() {
            return index;
        }

        public FilterCompat.Filter getRecordFilter() {
            return filter;
        }
    }

    static final class ThrowingOptions {
        public boolean useStatsFilter() {
            throw new RuntimeException("boom");
        }

        public boolean useDictionaryFilter() {
            throw new RuntimeException("boom");
        }

        public boolean useColumnIndexFilter() {
            throw new RuntimeException("boom");
        }

        public FilterCompat.Filter getRecordFilter() {
            throw new RuntimeException("boom");
        }
    }
}
