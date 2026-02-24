/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 */
package io.tileverse.parquet.reader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.Type;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;

class CoreMetadataModelTest {

    @Test
    void coreColumnChunkMeta_exposesAllFields() {
        CoreColumnChunkMeta meta = new CoreColumnChunkMeta(
                "a.b",
                List.of("a", "b"),
                Type.INT64,
                CompressionCodec.SNAPPY,
                10,
                111,
                1000,
                1010,
                1005,
                null,
                2000L,
                30,
                3000L,
                40);

        assertThat(meta.path()).isEqualTo("a.b");
        assertThat(meta.pathArray()).containsExactly("a", "b");
        assertThat(meta.type()).isEqualTo(Type.INT64);
        assertThat(meta.codec()).isEqualTo(CompressionCodec.SNAPPY);
        assertThat(meta.valueCount()).isEqualTo(10);
        assertThat(meta.totalCompressedSize()).isEqualTo(111);
        assertThat(meta.startOffset()).isEqualTo(1000);
        assertThat(meta.firstDataPageOffset()).isEqualTo(1010);
        assertThat(meta.dictionaryPageOffset()).isEqualTo(1005);
        assertThat(meta.hasDictionaryPage()).isTrue();
        assertThat(meta.columnIndexOffset()).isEqualTo(2000L);
        assertThat(meta.columnIndexLength()).isEqualTo(30);
        assertThat(meta.offsetIndexOffset()).isEqualTo(3000L);
        assertThat(meta.offsetIndexLength()).isEqualTo(40);
    }

    @Test
    void coreColumnChunkMeta_withoutDictionaryOffset_reportsNoDictionaryPage() {
        CoreColumnChunkMeta meta = new CoreColumnChunkMeta(
                "id",
                List.of("id"),
                Type.INT32,
                CompressionCodec.UNCOMPRESSED,
                1,
                1,
                1,
                1,
                -1,
                null,
                null,
                null,
                null,
                null);
        assertThat(meta.hasDictionaryPage()).isFalse();
    }

    @Test
    void coreRowGroupMeta_andFooter_areImmutableViews() {
        List<CoreColumnChunkMeta> mutableColumns = new ArrayList<>();
        mutableColumns.add(new CoreColumnChunkMeta(
                "id",
                List.of("id"),
                Type.INT32,
                CompressionCodec.UNCOMPRESSED,
                1,
                1,
                1,
                1,
                -1,
                null,
                null,
                null,
                null,
                null));
        CoreRowGroupMeta rowGroup = new CoreRowGroupMeta(5, mutableColumns);
        mutableColumns.clear();

        CoreParquetFooter footer = new CoreParquetFooter(
                MessageTypeParser.parseMessageType("message m { required int32 id; }"),
                Map.of("k", "v"),
                5,
                List.of(rowGroup));

        assertThat(rowGroup.rowCount()).isEqualTo(5);
        assertThat(rowGroup.columns()).hasSize(1);
        assertThat(footer.schema().getFieldCount()).isEqualTo(1);
        assertThat(footer.keyValueMetadata()).containsEntry("k", "v");
        assertThat(footer.recordCount()).isEqualTo(5);
        assertThat(footer.rowGroups()).hasSize(1);

        assertThatThrownBy(() -> footer.keyValueMetadata().put("x", "y"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> footer.rowGroups().add(rowGroup)).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> rowGroup.columns().clear()).isInstanceOf(UnsupportedOperationException.class);
    }
}
