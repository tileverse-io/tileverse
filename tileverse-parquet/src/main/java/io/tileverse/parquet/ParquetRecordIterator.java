/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquet;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.NoSuchElementException;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

/**
 * A {@link CloseableIterator} that reads records of type {@code T} from a {@link ParquetFileReader},
 * transparently iterating across row groups.
 * <p>
 * For each row group, a new {@link RecordReader} is created using {@link ColumnIOFactory}
 * and the provided {@link RecordMaterializer}. The materializer is reused across all row groups.
 * Records are decoded lazily on demand.
 * <p>
 * When a {@link FilterCompat.Filter} is provided, filtering is applied at three levels:
 * <ul>
 *   <li>Row group skipping (via statistics in {@link ParquetFileReader} constructor)</li>
 *   <li>Page skipping (via {@link ParquetFileReader#readNextFilteredRowGroup()})</li>
 *   <li>Record filtering (via {@link MessageColumnIO#getRecordReader(PageReadStore, RecordMaterializer, FilterCompat.Filter)})</li>
 * </ul>
 * <p>
 * Closing this iterator closes the underlying {@code ParquetFileReader}.
 * <p>
 * This class is not thread-safe.
 *
 * @param <T> the record type produced by the materializer
 */
class ParquetRecordIterator<T> implements CloseableIterator<T> {

    private final ParquetFileReader fileReader;
    private final RecordMaterializer<T> materializer;
    private final MessageType requestedSchema;
    private final MessageType fileSchema;
    private final FilterCompat.Filter filter;

    private RecordReader<T> recordReader;
    private long rowsReadInGroup;
    private long rowsInGroup;
    private T next;
    private boolean finished;

    ParquetRecordIterator(
            ParquetFileReader fileReader,
            RecordMaterializer<T> materializer,
            MessageType requestedSchema,
            MessageType fileSchema) {
        this(fileReader, materializer, requestedSchema, fileSchema, FilterCompat.NOOP);
    }

    ParquetRecordIterator(
            ParquetFileReader fileReader,
            RecordMaterializer<T> materializer,
            MessageType requestedSchema,
            MessageType fileSchema,
            FilterCompat.Filter filter) {
        this.fileReader = fileReader;
        this.materializer = materializer;
        this.requestedSchema = requestedSchema;
        this.fileSchema = fileSchema;
        this.filter = filter;
    }

    @Override
    public boolean hasNext() {
        if (next != null) {
            return true;
        }
        if (finished) {
            return false;
        }
        advance();
        return next != null;
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        T result = next;
        next = null;
        return result;
    }

    @Override
    public void close() throws IOException {
        finished = true;
        next = null;
        fileReader.close();
    }

    private void advance() {
        try {
            while (true) {
                if (recordReader != null && rowsReadInGroup < rowsInGroup) {
                    next = recordReader.read();
                    rowsReadInGroup++;
                    if (recordReader.shouldSkipCurrentRecord()) {
                        next = null;
                        continue;
                    }
                    return;
                }
                // Current row group exhausted (or first call), load next
                if (!loadNextRowGroup()) {
                    finished = true;
                    return;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private boolean loadNextRowGroup() throws IOException {
        PageReadStore rowGroup = FilterCompat.isFilteringRequired(filter)
                ? fileReader.readNextFilteredRowGroup()
                : fileReader.readNextRowGroup();
        if (rowGroup == null) {
            return false;
        }
        rowsInGroup = rowGroup.getRowCount();
        rowsReadInGroup = 0;

        ColumnIOFactory columnIOFactory = new ColumnIOFactory();
        MessageColumnIO columnIO = columnIOFactory.getColumnIO(requestedSchema, fileSchema);
        recordReader = columnIO.getRecordReader(rowGroup, materializer, filter);
        return true;
    }
}
