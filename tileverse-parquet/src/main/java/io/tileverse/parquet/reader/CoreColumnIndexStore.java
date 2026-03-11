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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

/**
 * Implements {@link ColumnIndexStore} by reading column and offset indexes from a Parquet file
 * using the offsets stored in {@link CoreColumnChunkMeta}.
 *
 * <p>Replaces {@code ColumnIndexStoreImpl} from parquet-hadoop. Reads all indexes eagerly in a
 * single stream pass for efficient I/O, especially over cloud storage.
 */
final class CoreColumnIndexStore implements ColumnIndexStore {

    /** No-op store returned when column indexes are unavailable. */
    private static final ColumnIndexStore EMPTY = new ColumnIndexStore() {
        @Override
        public ColumnIndex getColumnIndex(ColumnPath column) {
            return null;
        }

        @Override
        public OffsetIndex getOffsetIndex(ColumnPath column) {
            throw new MissingOffsetIndexException(column);
        }
    };

    private final Map<ColumnPath, ColumnIndex> columnIndexes;
    private final Map<ColumnPath, OffsetIndex> offsetIndexes;

    private CoreColumnIndexStore(
            Map<ColumnPath, ColumnIndex> columnIndexes, Map<ColumnPath, OffsetIndex> offsetIndexes) {
        this.columnIndexes = columnIndexes;
        this.offsetIndexes = offsetIndexes;
    }

    /**
     * Creates a {@link ColumnIndexStore} by reading indexes for the requested columns.
     *
     * <p>Returns a no-op store if any required offset index is missing or an I/O error occurs,
     * since column-index filtering is an optimization and must not block reading.
     */
    static ColumnIndexStore create(
            InputFile inputFile, CoreRowGroupMeta rowGroup, MessageType fileSchema, Set<ColumnPath> paths) {
        try {
            return doCreate(inputFile, rowGroup, fileSchema, paths);
        } catch (IOException e) {
            return EMPTY;
        }
    }

    private static ColumnIndexStore doCreate(
            InputFile inputFile, CoreRowGroupMeta rowGroup, MessageType fileSchema, Set<ColumnPath> paths)
            throws IOException {
        Map<String, CoreColumnChunkMeta> byPath = new HashMap<>();
        for (CoreColumnChunkMeta column : rowGroup.columns()) {
            byPath.put(column.path(), column);
        }

        Map<ColumnPath, ColumnIndex> columnIndexes = new HashMap<>();
        Map<ColumnPath, OffsetIndex> offsetIndexes = new HashMap<>();

        try (SeekableInputStream in = inputFile.newStream()) {
            for (ColumnPath path : paths) {
                CoreColumnChunkMeta meta = byPath.get(path.toDotString());
                if (meta == null) {
                    continue;
                }

                OffsetIndex offsetIndex = CoreColumnIndexReader.readOffsetIndex(in, meta);
                if (offsetIndex == null) {
                    return EMPTY;
                }
                offsetIndexes.put(path, offsetIndex);

                PrimitiveType primitiveType =
                        fileSchema.getColumnDescription(meta.pathArray()).getPrimitiveType();
                ColumnIndex columnIndex = CoreColumnIndexReader.readColumnIndex(in, meta, primitiveType);
                if (columnIndex != null) {
                    columnIndexes.put(path, columnIndex);
                }
            }
        }

        return new CoreColumnIndexStore(columnIndexes, offsetIndexes);
    }

    @Override
    public ColumnIndex getColumnIndex(ColumnPath column) {
        return columnIndexes.get(column);
    }

    @Override
    public OffsetIndex getOffsetIndex(ColumnPath column) {
        OffsetIndex oi = offsetIndexes.get(column);
        if (oi == null) {
            throw new MissingOffsetIndexException(column);
        }
        return oi;
    }
}
