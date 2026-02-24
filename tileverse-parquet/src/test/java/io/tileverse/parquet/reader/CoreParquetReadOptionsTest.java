/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 */
package io.tileverse.parquet.reader;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.junit.jupiter.api.Test;

class CoreParquetReadOptionsTest {

    @Test
    void defaults_enableAllFiltersAndNoopRecordFilter() {
        CoreParquetReadOptions options = CoreParquetReadOptions.defaults();
        assertThat(options.useStatsFilter()).isTrue();
        assertThat(options.useDictionaryFilter()).isTrue();
        assertThat(options.useColumnIndexFilter()).isTrue();
        assertThat(options.getRecordFilter()).isSameAs(FilterCompat.NOOP);
    }

    @Test
    void builder_appliesFlagsAndRecordFilter() {
        FilterCompat.Filter filter = FilterCompat.get(FilterApi.eq(FilterApi.intColumn("id"), 1));
        CoreParquetReadOptions options = CoreParquetReadOptions.builder()
                .useStatsFilter(false)
                .useDictionaryFilter(false)
                .useColumnIndexFilter(false)
                .withRecordFilter(filter)
                .build();

        assertThat(options.useStatsFilter()).isFalse();
        assertThat(options.useDictionaryFilter()).isFalse();
        assertThat(options.useColumnIndexFilter()).isFalse();
        assertThat(options.getRecordFilter()).isSameAs(filter);
    }

    @Test
    void toBuilder_copiesValuesAndAllowsMutation() {
        FilterCompat.Filter filter = FilterCompat.get(FilterApi.gt(FilterApi.intColumn("id"), 10));
        CoreParquetReadOptions base = CoreParquetReadOptions.builder()
                .useStatsFilter(false)
                .withRecordFilter(filter)
                .build();

        CoreParquetReadOptions copy =
                base.toBuilder().useDictionaryFilter(false).build();

        assertThat(copy.useStatsFilter()).isFalse();
        assertThat(copy.useDictionaryFilter()).isFalse();
        assertThat(copy.useColumnIndexFilter()).isTrue();
        assertThat(copy.getRecordFilter()).isSameAs(filter);
    }

    @Test
    void builder_nullRecordFilterFallsBackToNoop() {
        CoreParquetReadOptions options =
                CoreParquetReadOptions.builder().withRecordFilter(null).build();

        assertThat(options.getRecordFilter()).isSameAs(FilterCompat.NOOP);
    }
}
