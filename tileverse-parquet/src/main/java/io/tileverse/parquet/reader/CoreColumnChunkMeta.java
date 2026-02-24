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

    String path() {
        return path;
    }

    String[] pathArray() {
        return pathSegments.toArray(String[]::new);
    }

    Type type() {
        return type;
    }

    CompressionCodec codec() {
        return codec;
    }

    long valueCount() {
        return valueCount;
    }

    long totalCompressedSize() {
        return totalCompressedSize;
    }

    long startOffset() {
        return startOffset;
    }

    long firstDataPageOffset() {
        return firstDataPageOffset;
    }

    long dictionaryPageOffset() {
        return dictionaryPageOffset;
    }

    boolean hasDictionaryPage() {
        return dictionaryPageOffset > 0;
    }

    Statistics statistics() {
        return statistics;
    }

    Long columnIndexOffset() {
        return columnIndexOffset;
    }

    Integer columnIndexLength() {
        return columnIndexLength;
    }

    Long offsetIndexOffset() {
        return offsetIndexOffset;
    }

    Integer offsetIndexLength() {
        return offsetIndexLength;
    }
}
