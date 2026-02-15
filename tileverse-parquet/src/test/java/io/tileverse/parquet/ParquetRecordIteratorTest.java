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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ParquetRecordIteratorTest {

    @TempDir
    Path tempDir;

    private static final MessageType SCHEMA = MessageTypeParser.parseMessageType(
            """
            message test {
              required int32 id;
              required binary name (UTF8);
            }
            """);

    private ParquetRecordIterator<Group> openGroupIterator(Path file) throws IOException {
        ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file));
        MessageType fileSchema = reader.getFooter().getFileMetaData().getSchema();
        RecordMaterializer<Group> materializer =
                GroupMaterializerProvider.INSTANCE.createMaterializer(fileSchema, fileSchema, Map.of());
        return new ParquetRecordIterator<>(reader, materializer, fileSchema, fileSchema);
    }

    private ParquetRecordIterator<Group> openGroupIterator(Path file, MessageType requestedSchema) throws IOException {
        ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file));
        reader.setRequestedSchema(requestedSchema);
        MessageType fileSchema = reader.getFooter().getFileMetaData().getSchema();
        RecordMaterializer<Group> materializer =
                GroupMaterializerProvider.INSTANCE.createMaterializer(fileSchema, requestedSchema, Map.of());
        return new ParquetRecordIterator<>(reader, materializer, requestedSchema, fileSchema);
    }

    @Test
    void iteratesAllRecords_acrossMultipleRowGroups() throws IOException {
        // Write file with small row group size to force multiple row groups
        Path file = tempDir.resolve("multi-rg.parquet");
        int numRecords = 200;
        writeFileWithSmallRowGroups(file, SCHEMA, numRecords);

        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            // Verify we actually have multiple row groups
            assertThat(reader.getFooter().getBlocks().size()).isGreaterThan(1);
        }

        // Now iterate and verify all records are read
        try (var iterator = openGroupIterator(file)) {
            List<Group> records = new ArrayList<>();
            while (iterator.hasNext()) {
                records.add(iterator.next());
            }
            assertThat(records).hasSize(numRecords);

            // Verify records are in order
            for (int i = 0; i < numRecords; i++) {
                assertThat(records.get(i).getInteger("id", 0)).isEqualTo(i);
            }
        }
    }

    @Test
    void hasNext_returnsFalseAfterLastRecord() throws IOException {
        Path file = tempDir.resolve("small.parquet");
        ParquetDatasetTest.writeTestFile(file, SCHEMA, 3, Map.of());

        try (var iterator = openGroupIterator(file)) {
            iterator.next();
            iterator.next();
            iterator.next();
            assertThat(iterator.hasNext()).isFalse();
        }
    }

    @Test
    void next_throwsNoSuchElementException_whenExhausted() throws IOException {
        Path file = tempDir.resolve("tiny.parquet");
        ParquetDatasetTest.writeTestFile(file, SCHEMA, 1, Map.of());

        try (var iterator = openGroupIterator(file)) {
            iterator.next(); // consume the single record
            assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
        }
    }

    @Test
    void close_isIdempotent() throws IOException {
        Path file = tempDir.resolve("close-test.parquet");
        ParquetDatasetTest.writeTestFile(file, SCHEMA, 5, Map.of());

        var iterator = openGroupIterator(file);
        iterator.close();
        iterator.close(); // should not throw
        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    void projectedSchema_onlyMaterializesRequestedFields() throws IOException {
        MessageType fullSchema = MessageTypeParser.parseMessageType(
                """
                message projected_test {
                  required int32 id;
                  required binary name (UTF8);
                  required double value;
                }
                """);
        Path file = tempDir.resolve("projection.parquet");

        SimpleGroupFactory groupFactory = new SimpleGroupFactory(fullSchema);
        try (var writer = ExampleParquetWriter.builder(new LocalOutputFile(file))
                .withType(fullSchema)
                .build()) {
            for (int i = 0; i < 10; i++) {
                Group g = groupFactory.newGroup();
                g.add("id", i);
                g.add("name", "name_" + i);
                g.add("value", i * 1.5);
                writer.write(g);
            }
        }

        MessageType projected = ParquetDataset.projectSchema(fullSchema, java.util.Set.of("id", "name"));

        try (var iterator = openGroupIterator(file, projected)) {
            List<Group> records = new ArrayList<>();
            while (iterator.hasNext()) {
                records.add(iterator.next());
            }
            assertThat(records).hasSize(10);

            Group first = records.get(0);
            assertThat(first.getInteger("id", 0)).isZero();
            assertThat(first.getString("name", 0)).isEqualTo("name_0");
            // "value" field should not be in the record's schema
            assertThat(first.getType().containsField("value")).isFalse();
        }
    }

    @Test
    void emptyFile_hasNextReturnsFalse() throws IOException {
        Path file = tempDir.resolve("empty.parquet");
        ParquetDatasetTest.writeTestFile(file, SCHEMA, 0, Map.of());

        try (var iterator = openGroupIterator(file)) {
            assertThat(iterator.hasNext()).isFalse();
        }
    }

    private void writeFileWithSmallRowGroups(Path file, MessageType schema, int numRecords) throws IOException {
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        try (var writer = ExampleParquetWriter.builder(new LocalOutputFile(file))
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withRowGroupSize(1024L) // very small row group to force multiple groups
                .build()) {
            for (int i = 0; i < numRecords; i++) {
                Group g = groupFactory.newGroup();
                g.add("id", i);
                g.add("name", "name_" + i);
                writer.write(g);
            }
        }
    }
}
