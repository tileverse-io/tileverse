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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.jspecify.annotations.Nullable;

/**
 * High-level, stateless facade for reading a Parquet file's schema, metadata, and records.
 * <p>
 * This class caches file metadata (schema, key-value metadata, record count) from an initial
 * footer read. Each {@link #read()} call opens a fresh {@link ParquetFileReader} internally,
 * allowing multiple concurrent reads from the same {@code ParquetDataset}.
 *
 * <p><strong>Default record type:</strong> Avro {@link GenericRecord}, which handles nested
 * schemas natively. Use {@link #readGroups()} for backward-compatible {@link Group}-based reading.
 *
 * <p><strong>Filtering:</strong> All read methods have overloads accepting a {@link FilterPredicate}
 * for predicate pushdown (row group skipping, page skipping, and record-level filtering).
 *
 * <p><strong>Usage example:</strong></p>
 * <pre>{@code
 * InputFile inputFile = new RangeReaderInputFile(reader);
 * ParquetDataset dataset = ParquetDataset.open(inputFile);
 * FilterPredicate filter = FilterApi.gt(FilterApi.intColumn("id"), 50);
 * try (CloseableIterator<GenericRecord> records = dataset.read(filter, Set.of("id", "name"))) {
 *     while (records.hasNext()) {
 *         GenericRecord record = records.next();
 *         // process record...
 *     }
 * }
 * }</pre>
 */
public class ParquetDataset {

    private final InputFile inputFile;
    private final ParquetReadOptions readOptions;

    private long recordCount = -1L;
    private ParquetMetadata footer;

    private ParquetDataset(InputFile inputFile, ParquetReadOptions readOptions, ParquetMetadata footer) {
        this.inputFile = inputFile;
        this.readOptions = readOptions;
        this.footer = footer;
    }

    /**
     * Opens a Parquet file for reading with default options.
     * <p>
     * Reads the file footer to cache schema and metadata, then closes the reader.
     *
     * @param inputFile the Parquet file to open
     * @return a new stateless {@code ParquetDataset}
     * @throws IOException if the file cannot be opened or its footer cannot be read
     */
    public static ParquetDataset open(InputFile inputFile) throws IOException {
        return open(inputFile, ParquetReadOptions.builder().build());
    }

    /**
     * Opens a Parquet file for reading with the given options.
     * <p>
     * Reads the file footer to cache schema and metadata, then closes the reader.
     *
     * @param inputFile the Parquet file to open
     * @param options   read options (codec factory, filters, etc.)
     * @return a new stateless {@code ParquetDataset}
     * @throws IOException if the file cannot be opened or its footer cannot be read
     */
    public static ParquetDataset open(InputFile inputFile, ParquetReadOptions options) throws IOException {
        Objects.requireNonNull(inputFile, "inputFile");
        Objects.requireNonNull(options, "options");
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile, options)) {
            ParquetMetadata footer = reader.getFooter();
            return new ParquetDataset(inputFile, options, footer);
        }
    }

    /**
     * Returns the file's Parquet schema.
     */
    public MessageType getSchema() {
        return footer.getFileMetaData().getSchema();
    }

    /**
     * Returns the file-level key-value metadata as an unmodifiable map.
     */
    public Map<String, String> getKeyValueMetadata() {
        FileMetaData fileMetaData = footer.getFileMetaData();
        Map<String, String> keyValueMetaData = fileMetaData.getKeyValueMetaData();
        return Collections.unmodifiableMap(keyValueMetaData);
    }

    /**
     * Returns the total number of records in the file.
     */
    public long getRecordCount() {
        if (this.recordCount < 0) {
            this.recordCount = footer.getBlocks().stream()
                    .mapToLong(BlockMetaData::getRowCount)
                    .sum();
        }
        return recordCount;
    }

    // ---- Avro GenericRecord read methods ----

    /**
     * Returns a lazy iterator over all records as Avro {@link GenericRecord}.
     *
     * @return a closeable iterator over all records
     * @throws IOException if the file cannot be opened for reading
     */
    public CloseableIterator<GenericRecord> read() throws IOException {
        return doRead(AvroMaterializerProvider.INSTANCE, null, Set.of());
    }

    /**
     * Returns a lazy iterator over records as Avro {@link GenericRecord} with only the
     * requested columns materialized.
     *
     * @param columns the set of column names to read
     * @return a closeable iterator over projected records
     * @throws IllegalArgumentException if any column name is not found in the schema
     * @throws IOException if the file cannot be opened for reading
     */
    public CloseableIterator<GenericRecord> read(Set<String> columns) throws IOException {
        return doRead(AvroMaterializerProvider.INSTANCE, null, columns);
    }

    /**
     * Returns a lazy iterator over filtered records as Avro {@link GenericRecord}.
     *
     * @param filter the filter predicate for row group, page, and record-level filtering
     * @return a closeable iterator over filtered records
     * @throws IOException if the file cannot be opened for reading
     */
    public CloseableIterator<GenericRecord> read(FilterPredicate filter) throws IOException {
        return doRead(AvroMaterializerProvider.INSTANCE, filter, Set.of());
    }

    /**
     * Returns a lazy iterator over filtered records as Avro {@link GenericRecord} with
     * column projection.
     *
     * @param filter  the filter predicate
     * @param columns the set of column names to read
     * @return a closeable iterator over filtered, projected records
     * @throws IllegalArgumentException if any column name is not found in the schema
     * @throws IOException if the file cannot be opened for reading
     */
    public CloseableIterator<GenericRecord> read(FilterPredicate filter, Set<String> columns) throws IOException {
        return doRead(AvroMaterializerProvider.INSTANCE, filter, columns);
    }

    // ---- Custom materializer read methods ----

    /**
     * Returns a lazy iterator over all records using a custom materializer.
     *
     * @param provider the materializer provider
     * @param <T>      the record type
     * @return a closeable iterator over all records
     * @throws IOException if the file cannot be opened for reading
     */
    public <T> CloseableIterator<T> read(ParquetMaterializerProvider<T> provider) throws IOException {
        return doRead(provider, null, Set.of());
    }

    /**
     * Returns a lazy iterator over records using a custom materializer with column projection.
     *
     * @param provider the materializer provider
     * @param columns  the set of column names to read (empty for all columns)
     * @param <T>      the record type
     * @return a closeable iterator over projected records
     * @throws IllegalArgumentException if any column name is not found in the schema
     * @throws IOException if the file cannot be opened for reading
     */
    public <T> CloseableIterator<T> read(ParquetMaterializerProvider<T> provider, Set<String> columns)
            throws IOException {
        return doRead(provider, null, columns);
    }

    /**
     * Returns a lazy iterator over filtered records using a custom materializer.
     *
     * @param provider the materializer provider
     * @param filter   the filter predicate
     * @param <T>      the record type
     * @return a closeable iterator over filtered records
     * @throws IOException if the file cannot be opened for reading
     */
    public <T> CloseableIterator<T> read(ParquetMaterializerProvider<T> provider, FilterPredicate filter)
            throws IOException {
        return doRead(provider, filter, Set.of());
    }

    /**
     * Returns a lazy iterator over filtered records using a custom materializer with
     * column projection.
     *
     * @param provider the materializer provider
     * @param filter   the filter predicate
     * @param columns  the set of column names to read (empty for all columns)
     * @param <T>      the record type
     * @return a closeable iterator over filtered, projected records
     * @throws IllegalArgumentException if any column name is not found in the schema
     * @throws IOException if the file cannot be opened for reading
     */
    public <T> CloseableIterator<T> read(
            ParquetMaterializerProvider<T> provider, FilterPredicate filter, Set<String> columns) throws IOException {
        return doRead(provider, filter, columns);
    }

    // ---- Group read methods ----

    /**
     * Returns a lazy iterator over all records as {@link Group}.
     *
     * @return a closeable iterator over all records
     * @throws IOException if the file cannot be opened for reading
     */
    public CloseableIterator<Group> readGroups() throws IOException {
        return doRead(GroupMaterializerProvider.INSTANCE, null, Set.of());
    }

    /**
     * Returns a lazy iterator over records as {@link Group} with only the requested
     * columns materialized.
     *
     * @param columns the set of column names to read
     * @return a closeable iterator over projected records
     * @throws IllegalArgumentException if any column name is not found in the schema
     * @throws IOException if the file cannot be opened for reading
     */
    public CloseableIterator<Group> readGroups(Set<String> columns) throws IOException {
        return doRead(GroupMaterializerProvider.INSTANCE, null, columns);
    }

    /**
     * Returns a lazy iterator over filtered records as {@link Group}.
     *
     * @param filter the filter predicate
     * @return a closeable iterator over filtered records
     * @throws IOException if the file cannot be opened for reading
     */
    public CloseableIterator<Group> readGroups(FilterPredicate filter) throws IOException {
        return doRead(GroupMaterializerProvider.INSTANCE, filter, Set.of());
    }

    /**
     * Returns a lazy iterator over filtered records as {@link Group} with column projection.
     *
     * @param filter  the filter predicate
     * @param columns the set of column names to read
     * @return a closeable iterator over filtered, projected records
     * @throws IllegalArgumentException if any column name is not found in the schema
     * @throws IOException if the file cannot be opened for reading
     */
    public CloseableIterator<Group> readGroups(FilterPredicate filter, Set<String> columns) throws IOException {
        return doRead(GroupMaterializerProvider.INSTANCE, filter, columns);
    }

    // ---- Internal ----

    private <T> CloseableIterator<T> doRead(
            ParquetMaterializerProvider<T> provider, @Nullable FilterPredicate filter, Set<String> columns)
            throws IOException {
        Objects.requireNonNull(provider, "provider");
        Objects.requireNonNull(columns, "columns");

        MessageType schema = getSchema();
        MessageType requestedSchema = columns.isEmpty() ? schema : projectSchema(schema, columns);

        ParquetReadOptions options = this.readOptions;
        FilterCompat.Filter filterCompat = FilterCompat.NOOP;
        if (filter != null) {
            filterCompat = FilterCompat.get(filter);
            options = ParquetReadOptions.builder()
                    .copy(readOptions)
                    .withRecordFilter(filterCompat)
                    .build();
        }

        ParquetFileReader reader = ParquetFileReader.open(inputFile, options);
        if (!columns.isEmpty()) {
            reader.setRequestedSchema(requestedSchema);
        }

        Map<String, String> keyValueMetadata = getKeyValueMetadata();
        RecordMaterializer<T> materializer = provider.createMaterializer(schema, requestedSchema, keyValueMetadata);

        return new ParquetRecordIterator<>(reader, materializer, requestedSchema, schema, filterCompat);
    }

    /**
     * Builds a projected {@link MessageType} containing only the specified columns.
     *
     * @param schema  the full file schema
     * @param columns the column names to include
     * @return a new {@code MessageType} with only the requested fields
     * @throws IllegalArgumentException if any column name is not found in the schema
     */
    public static MessageType projectSchema(MessageType schema, Set<String> columns) {
        Objects.requireNonNull(schema, "schema");
        Objects.requireNonNull(columns, "columns");
        List<Type> fields = new ArrayList<>(columns.size());
        for (String column : columns) {
            if (!schema.containsField(column)) {
                throw new IllegalArgumentException(
                        "Column '%s' not found in schema: %s".formatted(column, schema.getFields()));
            }
            fields.add(schema.getType(column));
        }
        return new MessageType(schema.getName(), fields);
    }
}
