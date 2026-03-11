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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.format.Type;

/**
 * Immutable metadata for a single column chunk within a Parquet row group.
 *
 * <p>Holds the column path, physical type, compression codec, byte offsets, statistics,
 * and optional column/offset index locations needed for filter pushdown and I/O.
 */
final class CoreColumnChunkMeta {
    private final String path;
    private final List<String> pathSegments;
    private final Type type;
    private final CompressionCodec codec;
    private final long valueCount;
    private final long totalCompressedSize;
    private final long startOffset;
    private final long firstDataPageOffset;
    private final long dictionaryPageOffset;
    private final Statistics statistics;
    private final Long columnIndexOffset;
    private final Integer columnIndexLength;
    private final Long offsetIndexOffset;
    private final Integer offsetIndexLength;

    CoreColumnChunkMeta(
            String path,
            List<String> pathSegments,
            Type type,
            CompressionCodec codec,
            long valueCount,
            long totalCompressedSize,
            long startOffset,
            long firstDataPageOffset,
            long dictionaryPageOffset,
            Statistics statistics,
            Long columnIndexOffset,
            Integer columnIndexLength,
            Long offsetIndexOffset,
            Integer offsetIndexLength) {
        this.path = Objects.requireNonNull(path, "path");
        this.pathSegments = Collections.unmodifiableList(Objects.requireNonNull(pathSegments, "pathSegments"));
        this.type = Objects.requireNonNull(type, "type");
        this.codec = Objects.requireNonNull(codec, "codec");
        this.valueCount = valueCount;
        this.totalCompressedSize = totalCompressedSize;
        this.startOffset = startOffset;
        this.firstDataPageOffset = firstDataPageOffset;
        this.dictionaryPageOffset = dictionaryPageOffset;
        this.statistics = statistics;
        this.columnIndexOffset = columnIndexOffset;
        this.columnIndexLength = columnIndexLength;
        this.offsetIndexOffset = offsetIndexOffset;
        this.offsetIndexLength = offsetIndexLength;
    }

    /** Returns the dot-separated column path (e.g. {@code "address.city"}). */
    String path() {
        return path;
    }

    /** Returns the column path segments as a string array. */
    String[] pathArray() {
        return pathSegments.toArray(String[]::new);
    }

    /** Returns the Parquet physical type of this column. */
    Type type() {
        return type;
    }

    /** Returns the compression codec used for this column chunk. */
    CompressionCodec codec() {
        return codec;
    }

    /** Returns the number of values (including nulls) in this column chunk. */
    long valueCount() {
        return valueCount;
    }

    /** Returns the total compressed size in bytes of this column chunk. */
    long totalCompressedSize() {
        return totalCompressedSize;
    }

    /** Returns the byte offset where this column chunk starts in the file. */
    long startOffset() {
        return startOffset;
    }

    /** Returns the byte offset of the first data page. */
    long firstDataPageOffset() {
        return firstDataPageOffset;
    }

    /** Returns the byte offset of the dictionary page, or {@code -1} if none. */
    long dictionaryPageOffset() {
        return dictionaryPageOffset;
    }

    /** Returns {@code true} if this column chunk has a dictionary page. */
    boolean hasDictionaryPage() {
        return dictionaryPageOffset > 0;
    }

    /** Returns the column statistics from the row group metadata, or {@code null} if absent. */
    Statistics statistics() {
        return statistics;
    }

    /** Returns the byte offset of the column index, or {@code null} if absent. */
    Long columnIndexOffset() {
        return columnIndexOffset;
    }

    /** Returns the length in bytes of the column index, or {@code null} if absent. */
    Integer columnIndexLength() {
        return columnIndexLength;
    }

    /** Returns the byte offset of the offset index, or {@code null} if absent. */
    Long offsetIndexOffset() {
        return offsetIndexOffset;
    }

    /** Returns the length in bytes of the offset index, or {@code null} if absent. */
    Integer offsetIndexLength() {
        return offsetIndexLength;
    }
}
