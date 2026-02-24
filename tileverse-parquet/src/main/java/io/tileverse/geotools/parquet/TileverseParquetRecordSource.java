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
package io.tileverse.geotools.parquet;

import io.tileverse.parquet.CloseableIterator;
import io.tileverse.parquet.ParquetDataset;
import io.tileverse.parquet.RangeReaderInputFile;
import io.tileverse.rangereader.RangeReader;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.schema.MessageType;

/**
 * Default {@link GeoParquetRecordSource} backed by Tileverse {@link RangeReader}-based Parquet
 * access.
 */
final class TileverseParquetRecordSource implements GeoParquetRecordSource {

    private final ParquetDataset dataset;

    TileverseParquetRecordSource(RangeReader rangeReader) throws IOException {
        this.dataset = ParquetDataset.open(new RangeReaderInputFile(rangeReader));
    }

    @Override
    public MessageType getSchema() {
        return dataset.getSchema();
    }

    @Override
    public Map<String, String> getKeyValueMetadata() {
        return dataset.getKeyValueMetadata();
    }

    @Override
    public CloseableIterator<GenericRecord> read() throws IOException {
        return dataset.read();
    }

    @Override
    public CloseableIterator<GenericRecord> read(Set<String> columns) throws IOException {
        return dataset.read(columns);
    }

    @Override
    public CloseableIterator<GenericRecord> read(FilterPredicate filter) throws IOException {
        return dataset.read(filter);
    }

    @Override
    public CloseableIterator<GenericRecord> read(FilterPredicate filter, Set<String> columns) throws IOException {
        return dataset.read(filter, columns);
    }
}
