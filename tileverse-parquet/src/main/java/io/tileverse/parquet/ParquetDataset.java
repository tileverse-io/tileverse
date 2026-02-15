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
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

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
 * <p><strong>Usage example:</strong></p>
 * <pre>{@code
 * InputFile inputFile = new RangeReaderInputFile(reader);
 * ParquetDataset dataset = ParquetDataset.open(inputFile);
 * MessageType schema = dataset.getSchema();
 * try (CloseableIterator<GenericRecord> records = dataset.read(Set.of("name", "geometry"))) {
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
    private final MessageType schema;
    private final Map<String, String> keyValueMetadata;
    private final long recordCount;

    private ParquetDataset(InputFile inputFile, ParquetReadOptions readOptions, ParquetMetadata footer) {
        this.inputFile = inputFile;
        this.readOptions = readOptions;
        this.schema = footer.getFileMetaData().getSchema();
        this.keyValueMetadata =
                Collections.unmodifiableMap(footer.getFileMetaData().getKeyValueMetaData());
        this.recordCount = footer.getBlocks().stream()
                .mapToLong(block -> block.getRowCount())
                .sum();
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
            return new ParquetDataset(inputFile, options, reader.getFooter());
        }
    }

    /**
     * Returns the file's Parquet schema.
     */
    public MessageType getSchema() {
        return schema;
    }

    /**
     * Returns the file-level key-value metadata as an unmodifiable map.
     */
    public Map<String, String> getKeyValueMetadata() {
        return keyValueMetadata;
    }

    /**
     * Returns the total number of records in the file.
     */
    public long getRecordCount() {
        return recordCount;
    }

    /**
     * Returns a lazy iterator over all records as Avro {@link GenericRecord}.
     *
     * @return a closeable iterator over all records
     * @throws IOException if the file cannot be opened for reading
     */
    public CloseableIterator<GenericRecord> read() throws IOException {
        return read(AvroMaterializerProvider.INSTANCE, Set.of());
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
        return read(AvroMaterializerProvider.INSTANCE, columns);
    }

    /**
     * Returns a lazy iterator over all records using a custom materializer.
     *
     * @param provider the materializer provider
     * @param <T>      the record type
     * @return a closeable iterator over all records
     * @throws IOException if the file cannot be opened for reading
     */
    public <T> CloseableIterator<T> read(ParquetMaterializerProvider<T> provider) throws IOException {
        return read(provider, Set.of());
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
        Objects.requireNonNull(provider, "provider");
        Objects.requireNonNull(columns, "columns");

        MessageType requestedSchema = columns.isEmpty() ? schema : projectSchema(schema, columns);

        ParquetFileReader reader = ParquetFileReader.open(inputFile, readOptions);
        if (!columns.isEmpty()) {
            reader.setRequestedSchema(requestedSchema);
        }

        RecordMaterializer<T> materializer = provider.createMaterializer(schema, requestedSchema, keyValueMetadata);

        return new ParquetRecordIterator<>(reader, materializer, requestedSchema, schema);
    }

    /**
     * Returns a lazy iterator over all records as {@link Group}.
     *
     * @return a closeable iterator over all records
     * @throws IOException if the file cannot be opened for reading
     */
    public CloseableIterator<Group> readGroups() throws IOException {
        return read(GroupMaterializerProvider.INSTANCE, Set.of());
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
        return read(GroupMaterializerProvider.INSTANCE, columns);
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
