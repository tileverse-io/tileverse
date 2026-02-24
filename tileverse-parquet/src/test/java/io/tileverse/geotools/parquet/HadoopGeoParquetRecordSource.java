/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 */
package io.tileverse.geotools.parquet;

import io.tileverse.parquet.CloseableIterator;
import io.tileverse.parquet.RangeReaderInputFile;
import io.tileverse.rangereader.RangeReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Set;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;

/**
 * Hadoop-backed record source for performance comparisons only.
 */
final class HadoopGeoParquetRecordSource implements GeoParquetRecordSource {

    private final InputFile inputFile;
    private final MessageType schema;
    private final Map<String, String> keyValueMetadata;

    HadoopGeoParquetRecordSource(RangeReader rangeReader) throws IOException {
        this.inputFile = new RangeReaderInputFile(rangeReader);
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            this.schema = reader.getFooter().getFileMetaData().getSchema();
            this.keyValueMetadata = reader.getFooter().getFileMetaData().getKeyValueMetaData();
        }
    }

    @Override
    public MessageType getSchema() {
        return schema;
    }

    @Override
    public Map<String, String> getKeyValueMetadata() {
        return keyValueMetadata;
    }

    @Override
    public CloseableIterator<GenericRecord> read() throws IOException {
        return openReader(null);
    }

    @Override
    public CloseableIterator<GenericRecord> read(Set<String> columns) throws IOException {
        return openReader(null);
    }

    @Override
    public CloseableIterator<GenericRecord> read(FilterPredicate filter) throws IOException {
        return openReader(filter);
    }

    @Override
    public CloseableIterator<GenericRecord> read(FilterPredicate filter, Set<String> columns) throws IOException {
        return openReader(filter);
    }

    private CloseableIterator<GenericRecord> openReader(FilterPredicate filter) throws IOException {
        ParquetReader.Builder<GenericRecord> builder = AvroParquetReader.<GenericRecord>builder(inputFile);
        if (filter != null) {
            builder = builder.withFilter(FilterCompat.get(filter));
        }
        ParquetReader<GenericRecord> reader = builder.build();
        return new CloseableIterator<>() {
            private GenericRecord next;
            private boolean finished;

            @Override
            public boolean hasNext() {
                if (finished) {
                    return false;
                }
                if (next != null) {
                    return true;
                }
                try {
                    next = reader.read();
                    if (next == null) {
                        finished = true;
                        return false;
                    }
                    return true;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            public GenericRecord next() {
                if (!hasNext()) {
                    throw new java.util.NoSuchElementException();
                }
                GenericRecord current = next;
                next = null;
                return current;
            }

            @Override
            public void close() throws IOException {
                reader.close();
            }
        };
    }
}
