/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 */
package io.tileverse.parquet.reader;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.parquet.format.BoundaryOrder;
import org.apache.parquet.format.ColumnIndex;
import org.apache.parquet.format.OffsetIndex;
import org.apache.parquet.format.PageLocation;
import org.apache.parquet.format.Util;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CoreColumnIndexReaderTest {

    @TempDir
    Path tempDir;

    @Test
    void readColumnIndex_returnsNullWhenOffsetsNotSet() throws Exception {
        Path file = writeTempFile(new byte[] {0});
        CoreColumnChunkMeta meta = columnChunkMeta(null, null, null, null);
        PrimitiveType type =
                Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("id");

        try (SeekableInputStream in = new LocalInputFile(file).newStream()) {
            assertThat(CoreColumnIndexReader.readColumnIndex(in, meta, type)).isNull();
        }
    }

    @Test
    void readOffsetIndex_returnsNullWhenOffsetsNotSet() throws Exception {
        Path file = writeTempFile(new byte[] {0});
        CoreColumnChunkMeta meta = columnChunkMeta(null, null, null, null);

        try (SeekableInputStream in = new LocalInputFile(file).newStream()) {
            assertThat(CoreColumnIndexReader.readOffsetIndex(in, meta)).isNull();
        }
    }

    @Test
    void readColumnIndex_deserializesThriftColumnIndex() throws Exception {
        ByteBuffer minVal = ByteBuffer.allocate(4).putInt(0, 10);
        ByteBuffer maxVal = ByteBuffer.allocate(4).putInt(0, 99);
        ColumnIndex thriftCI =
                new ColumnIndex(List.of(false), List.of(minVal), List.of(maxVal), BoundaryOrder.ASCENDING);
        thriftCI.setNull_counts(List.of(0L));

        ByteArrayOutputStream ciBytes = new ByteArrayOutputStream();
        Util.writeColumnIndex(thriftCI, ciBytes);
        byte[] ciData = ciBytes.toByteArray();

        // Write at offset 100 in the file
        byte[] fileData = new byte[100 + ciData.length];
        System.arraycopy(ciData, 0, fileData, 100, ciData.length);
        Path file = writeTempFile(fileData);

        CoreColumnChunkMeta meta = columnChunkMeta(100L, ciData.length, null, null);
        PrimitiveType type =
                Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("id");

        try (SeekableInputStream in = new LocalInputFile(file).newStream()) {
            org.apache.parquet.internal.column.columnindex.ColumnIndex result =
                    CoreColumnIndexReader.readColumnIndex(in, meta, type);
            assertThat(result).isNotNull();
            assertThat(result.getMinValues()).hasSize(1);
            assertThat(result.getMaxValues()).hasSize(1);
            assertThat(result.getNullPages()).containsExactly(false);
            assertThat(result.getNullCounts()).containsExactly(0L);
        }
    }

    @Test
    void readOffsetIndex_deserializesThriftOffsetIndex() throws Exception {
        OffsetIndex thriftOI = new OffsetIndex(List.of(new PageLocation(0, 100, 0), new PageLocation(100, 200, 50)));

        ByteArrayOutputStream oiBytes = new ByteArrayOutputStream();
        Util.writeOffsetIndex(thriftOI, oiBytes);
        byte[] oiData = oiBytes.toByteArray();

        byte[] fileData = new byte[50 + oiData.length];
        System.arraycopy(oiData, 0, fileData, 50, oiData.length);
        Path file = writeTempFile(fileData);

        CoreColumnChunkMeta meta = columnChunkMeta(null, null, 50L, oiData.length);

        try (SeekableInputStream in = new LocalInputFile(file).newStream()) {
            org.apache.parquet.internal.column.columnindex.OffsetIndex result =
                    CoreColumnIndexReader.readOffsetIndex(in, meta);
            assertThat(result).isNotNull();
            assertThat(result.getPageCount()).isEqualTo(2);
            assertThat(result.getOffset(0)).isEqualTo(0);
            assertThat(result.getOffset(1)).isEqualTo(100);
            assertThat(result.getCompressedPageSize(0)).isEqualTo(100);
            assertThat(result.getCompressedPageSize(1)).isEqualTo(200);
            assertThat(result.getFirstRowIndex(0)).isEqualTo(0);
            assertThat(result.getFirstRowIndex(1)).isEqualTo(50);
        }
    }

    private Path writeTempFile(byte[] bytes) throws IOException {
        Path file = tempDir.resolve("test-" + System.nanoTime() + ".bin");
        Files.write(file, bytes);
        return file;
    }

    private static CoreColumnChunkMeta columnChunkMeta(
            Long columnIndexOffset, Integer columnIndexLength, Long offsetIndexOffset, Integer offsetIndexLength) {
        return new CoreColumnChunkMeta(
                "id",
                List.of("id"),
                org.apache.parquet.format.Type.INT32,
                org.apache.parquet.format.CompressionCodec.UNCOMPRESSED,
                1,
                100,
                0,
                0,
                -1,
                null,
                columnIndexOffset,
                columnIndexLength,
                offsetIndexOffset,
                offsetIndexLength);
    }
}
