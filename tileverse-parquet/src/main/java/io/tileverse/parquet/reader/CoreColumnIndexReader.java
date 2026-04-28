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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Optional;
import org.apache.parquet.format.PageLocation;
import org.apache.parquet.format.Util;
import org.apache.parquet.internal.column.columnindex.BoundaryOrder;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.ColumnIndexBuilder;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndexBuilder;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.ColumnOrder.ColumnOrderName;
import org.apache.parquet.schema.PrimitiveType;

/**
 * Reads Thrift-encoded ColumnIndex and OffsetIndex structures from a Parquet file and converts them to parquet-column
 * internal representations.
 *
 * <p>This replaces the conversion logic in {@code ParquetMetadataConverter} from parquet-hadoop, removing the Hadoop
 * dependency for index reading.
 */
final class CoreColumnIndexReader {

    private CoreColumnIndexReader() {}

    /**
     * Reads the {@link ColumnIndex} for a column chunk, or returns {@code null} if the index is absent or the column
     * type does not support min/max statistics.
     *
     * @param in seekable stream positioned within the Parquet file
     * @param columnMeta metadata for the column chunk
     * @param type the primitive type of the column (for stats decoding)
     * @return the column index, or {@code null}
     * @throws IOException if an I/O error occurs
     */
    static ColumnIndex readColumnIndex(SeekableInputStream in, CoreColumnChunkMeta columnMeta, PrimitiveType type)
            throws IOException {
        Long offset = columnMeta.columnIndexOffset();
        Integer length = columnMeta.columnIndexLength();
        if (offset == null || length == null) {
            return null;
        }
        if (!isMinMaxStatsSupported(type)) {
            return null;
        }
        byte[] bytes = readBytes(in, offset, length);
        org.apache.parquet.format.ColumnIndex thriftColumnIndex = Util.readColumnIndex(new ByteArrayInputStream(bytes));
        return fromParquetColumnIndex(type, thriftColumnIndex);
    }

    /**
     * Reads the {@link OffsetIndex} for a column chunk, or returns {@code null} if absent.
     *
     * @param in seekable stream positioned within the Parquet file
     * @param columnMeta metadata for the column chunk
     * @return the offset index, or {@code null}
     * @throws IOException if an I/O error occurs
     */
    static OffsetIndex readOffsetIndex(SeekableInputStream in, CoreColumnChunkMeta columnMeta) throws IOException {
        Long offset = columnMeta.offsetIndexOffset();
        Integer length = columnMeta.offsetIndexLength();
        if (offset == null || length == null) {
            return null;
        }
        byte[] bytes = readBytes(in, offset, length);
        org.apache.parquet.format.OffsetIndex thriftOffsetIndex = Util.readOffsetIndex(new ByteArrayInputStream(bytes));
        return fromParquetOffsetIndex(thriftOffsetIndex);
    }

    private static ColumnIndex fromParquetColumnIndex(
            PrimitiveType type, org.apache.parquet.format.ColumnIndex parquetColumnIndex) {
        return ColumnIndexBuilder.build(
                type,
                fromParquetBoundaryOrder(parquetColumnIndex.getBoundary_order()),
                parquetColumnIndex.getNull_pages(),
                parquetColumnIndex.getNull_counts(),
                parquetColumnIndex.getMin_values(),
                parquetColumnIndex.getMax_values(),
                parquetColumnIndex.getRepetition_level_histograms(),
                parquetColumnIndex.getDefinition_level_histograms());
    }

    private static OffsetIndex fromParquetOffsetIndex(org.apache.parquet.format.OffsetIndex parquetOffsetIndex) {
        boolean hasUnencodedByteArrayDataBytes = parquetOffsetIndex.isSetUnencoded_byte_array_data_bytes()
                && parquetOffsetIndex.getUnencoded_byte_array_data_bytes().size()
                        == parquetOffsetIndex.getPage_locations().size();
        OffsetIndexBuilder builder = OffsetIndexBuilder.getBuilder();
        for (int i = 0; i < parquetOffsetIndex.getPage_locations().size(); i++) {
            PageLocation pageLocation = parquetOffsetIndex.getPage_locations().get(i);
            Optional<Long> unencodedBytes = hasUnencodedByteArrayDataBytes
                    ? Optional.of(parquetOffsetIndex
                            .getUnencoded_byte_array_data_bytes()
                            .get(i))
                    : Optional.empty();
            builder.add(
                    pageLocation.getOffset(),
                    pageLocation.getCompressed_page_size(),
                    pageLocation.getFirst_row_index(),
                    unencodedBytes);
        }
        return builder.build();
    }

    private static BoundaryOrder fromParquetBoundaryOrder(org.apache.parquet.format.BoundaryOrder boundaryOrder) {
        return switch (boundaryOrder) {
            case ASCENDING -> BoundaryOrder.ASCENDING;
            case DESCENDING -> BoundaryOrder.DESCENDING;
            case UNORDERED -> BoundaryOrder.UNORDERED;
        };
    }

    private static boolean isMinMaxStatsSupported(PrimitiveType type) {
        return type.columnOrder().getColumnOrderName() == ColumnOrderName.TYPE_DEFINED_ORDER;
    }

    private static byte[] readBytes(SeekableInputStream in, long offset, int length) throws IOException {
        byte[] bytes = new byte[length];
        in.seek(offset);
        in.readFully(bytes);
        return bytes;
    }
}
