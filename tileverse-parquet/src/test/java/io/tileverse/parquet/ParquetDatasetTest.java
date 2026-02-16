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
package io.tileverse.parquet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ParquetDatasetTest {

    @TempDir
    Path tempDir;

    private static final MessageType SCHEMA = MessageTypeParser.parseMessageType(
            """
            message test_schema {
              required int32 id;
              required binary name (UTF8);
              required double value;
              required boolean active;
              required int64 timestamp;
            }
            """);

    private static final int NUM_RECORDS = 100;
    private Path testFile;

    @BeforeEach
    void setUp() throws IOException {
        testFile = tempDir.resolve("test.parquet");
        writeTestFile(testFile, SCHEMA, NUM_RECORDS, Map.of("creator", "test", "version", "1.0"));
    }

    private ParquetDataset open(Path file) throws IOException {
        return ParquetDataset.open(new LocalInputFile(file));
    }

    @Test
    void getSchema_returnsCorrectSchema() throws IOException {
        ParquetDataset dataset = open(testFile);
        MessageType schema = dataset.getSchema();
        assertThat(schema.getFieldCount()).isEqualTo(5);
        assertThat(schema.getFields().stream().map(Type::getName))
                .containsExactly("id", "name", "value", "active", "timestamp");
    }

    @Test
    void getKeyValueMetadata_includesFileMetadata() throws IOException {
        ParquetDataset dataset = open(testFile);
        Map<String, String> metadata = dataset.getKeyValueMetadata();
        assertThat(metadata).containsEntry("creator", "test").containsEntry("version", "1.0");
    }

    @Test
    void getRecordCount_returnsCorrectCount() throws IOException {
        ParquetDataset dataset = open(testFile);
        assertThat(dataset.getRecordCount()).isEqualTo(NUM_RECORDS);
    }

    @Test
    void read_returnsAllRecordsAsGenericRecord() throws IOException {
        ParquetDataset dataset = open(testFile);
        List<GenericRecord> records = readAll(dataset.read());
        assertThat(records).hasSize(NUM_RECORDS);

        // Verify first record values
        GenericRecord first = records.get(0);
        assertThat(first.get("id")).isEqualTo(0);
        assertThat(first.get("name").toString()).isEqualTo("name_0");
        assertThat(first.get("value")).isEqualTo(0.0);
        assertThat(first.get("active")).isEqualTo(true);
        assertThat(first.get("timestamp")).isEqualTo(1000L);

        // Verify last record values
        GenericRecord last = records.get(NUM_RECORDS - 1);
        assertThat(last.get("id")).isEqualTo(NUM_RECORDS - 1);
        assertThat(last.get("name").toString()).isEqualTo("name_" + (NUM_RECORDS - 1));
    }

    @Test
    void read_withColumnProjection_returnsOnlyRequestedColumns() throws IOException {
        ParquetDataset dataset = open(testFile);
        List<GenericRecord> records = readAll(dataset.read(Set.of("id", "name")));
        assertThat(records).hasSize(NUM_RECORDS);

        GenericRecord first = records.get(0);
        assertThat(first.get("id")).isEqualTo(0);
        assertThat(first.get("name").toString()).isEqualTo("name_0");

        // Projected-out columns should not be in the schema
        assertThat(first.getSchema().getField("value")).isNull();
        assertThat(first.getSchema().getField("active")).isNull();
        assertThat(first.getSchema().getField("timestamp")).isNull();
    }

    @Test
    void read_emptyColumns_readsAllColumns() throws IOException {
        ParquetDataset dataset = open(testFile);
        List<GenericRecord> records = readAll(dataset.read(Set.of()));
        assertThat(records).hasSize(NUM_RECORDS);

        // All columns should be present
        GenericRecord first = records.get(0);
        assertThat(first.getSchema().getFields()).hasSize(5);
    }

    @Test
    void readGroups_returnsAllRecordsAsGroup() throws IOException {
        ParquetDataset dataset = open(testFile);
        List<Group> records = readAllGroups(dataset.readGroups());
        assertThat(records).hasSize(NUM_RECORDS);

        Group first = records.get(0);
        assertThat(first.getInteger("id", 0)).isZero();
        assertThat(first.getString("name", 0)).isEqualTo("name_0");
        assertThat(first.getDouble("value", 0)).isEqualTo(0.0);
        assertThat(first.getBoolean("active", 0)).isTrue();
        assertThat(first.getLong("timestamp", 0)).isEqualTo(1000L);
    }

    @Test
    void readGroups_withColumnProjection_returnsOnlyRequestedColumns() throws IOException {
        ParquetDataset dataset = open(testFile);
        List<Group> records = readAllGroups(dataset.readGroups(Set.of("id", "name")));
        assertThat(records).hasSize(NUM_RECORDS);

        Group first = records.get(0);
        assertThat(first.getInteger("id", 0)).isZero();
        assertThat(first.getString("name", 0)).isEqualTo("name_0");
        assertThat(first.getType().containsField("value")).isFalse();
        assertThat(first.getType().containsField("active")).isFalse();
    }

    @Test
    void projectSchema_buildsCorrectSubset() {
        MessageType projected = ParquetDataset.projectSchema(SCHEMA, Set.of("id", "value"));
        assertThat(projected.getFieldCount()).isEqualTo(2);
        assertThat(projected.containsField("id")).isTrue();
        assertThat(projected.containsField("value")).isTrue();
        assertThat(projected.containsField("name")).isFalse();
    }

    @Test
    void projectSchema_throwsOnUnknownColumn() {
        Set<String> nonExistent = Set.of("nonexistent");
        assertThatThrownBy(() -> ParquetDataset.projectSchema(SCHEMA, nonExistent))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Column 'nonexistent' not found");
    }

    @Test
    void multipleReads_onSameDataset_workIndependently() throws IOException {
        ParquetDataset dataset = open(testFile);

        assertThat(dataset.getRecordCount()).isEqualTo(NUM_RECORDS);

        List<GenericRecord> records1 = readAll(dataset.read());
        List<GenericRecord> records2 = readAll(dataset.read());
        assertThat(records1).hasSize(NUM_RECORDS);
        assertThat(records2).hasSize(NUM_RECORDS);
    }

    @Test
    void emptyFile_zeroRecords() throws IOException {
        Path emptyFile = tempDir.resolve("empty.parquet");
        writeTestFile(emptyFile, SCHEMA, 0, Map.of());

        ParquetDataset dataset = open(emptyFile);
        assertThat(dataset.getRecordCount()).isZero();
        assertThat(dataset.getSchema().getFieldCount()).isEqualTo(5);
        List<GenericRecord> records = readAll(dataset.read());
        assertThat(records).isEmpty();
    }

    @Test
    void read_nestedSchema_returnsNestedGenericRecords() throws IOException {
        MessageType nestedSchema = MessageTypeParser.parseMessageType(
                """
                message overture_building {
                  required binary id (UTF8);
                  required group names {
                    required binary primary (UTF8);
                    optional binary common (UTF8);
                  }
                  required group sources {
                    required binary dataset (UTF8);
                    optional binary record_id (UTF8);
                  }
                }
                """);
        Path nestedFile = tempDir.resolve("nested.parquet");

        SimpleGroupFactory groupFactory = new SimpleGroupFactory(nestedSchema);
        try (var writer = ExampleParquetWriter.builder(new LocalOutputFile(nestedFile))
                .withType(nestedSchema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {
            for (int i = 0; i < 10; i++) {
                Group group = groupFactory.newGroup();
                group.add("id", "building_" + i);
                Group names = group.addGroup("names");
                names.add("primary", "Building " + i);
                names.add("common", "Bldg " + i);
                Group sources = group.addGroup("sources");
                sources.add("dataset", "overture");
                sources.add("record_id", "rec_" + i);
                writer.write(group);
            }
        }

        ParquetDataset dataset = open(nestedFile);
        List<GenericRecord> records = readAll(dataset.read());
        assertThat(records).hasSize(10);

        GenericRecord first = records.get(0);
        assertThat(first.get("id").toString()).isEqualTo("building_0");

        GenericRecord names = (GenericRecord) first.get("names");
        assertThat(names.get("primary").toString()).isEqualTo("Building 0");
        assertThat(names.get("common").toString()).isEqualTo("Bldg 0");

        GenericRecord sources = (GenericRecord) first.get("sources");
        assertThat(sources.get("dataset").toString()).isEqualTo("overture");
        assertThat(sources.get("record_id").toString()).isEqualTo("rec_0");
    }

    @Test
    void read_nestedSchema_withProjection() throws IOException {
        MessageType nestedSchema = MessageTypeParser.parseMessageType(
                """
                message nested_projection {
                  required binary id (UTF8);
                  required group info {
                    required binary label (UTF8);
                  }
                  required double value;
                }
                """);
        Path nestedFile = tempDir.resolve("nested-proj.parquet");

        SimpleGroupFactory groupFactory = new SimpleGroupFactory(nestedSchema);
        try (var writer = ExampleParquetWriter.builder(new LocalOutputFile(nestedFile))
                .withType(nestedSchema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {
            for (int i = 0; i < 5; i++) {
                Group group = groupFactory.newGroup();
                group.add("id", "item_" + i);
                group.addGroup("info").add("label", "Label " + i);
                group.add("value", i * 2.5);
                writer.write(group);
            }
        }

        ParquetDataset dataset = open(nestedFile);
        List<GenericRecord> records = readAll(dataset.read(Set.of("id", "info")));
        assertThat(records).hasSize(5);

        GenericRecord first = records.get(0);
        assertThat(first.get("id").toString()).isEqualTo("item_0");
        GenericRecord info = (GenericRecord) first.get("info");
        assertThat(info.get("label").toString()).isEqualTo("Label 0");
        assertThat(first.getSchema().getField("value")).isNull();
    }

    @Test
    void read_withFilter_returnsOnlyMatchingRecords() throws IOException {
        ParquetDataset dataset = open(testFile);
        // id > 50 should match ids 51..99 = 49 records
        FilterPredicate filter = FilterApi.gt(FilterApi.intColumn("id"), 50);
        List<GenericRecord> records = readAll(dataset.read(filter));
        assertThat(records).hasSize(49);
        assertThat(records).allSatisfy(r -> assertThat((int) r.get("id")).isGreaterThan(50));
    }

    @Test
    void read_withFilterAndProjection_combinesCorrectly() throws IOException {
        ParquetDataset dataset = open(testFile);
        FilterPredicate filter = FilterApi.ltEq(FilterApi.intColumn("id"), 5);
        List<GenericRecord> records = readAll(dataset.read(filter, Set.of("id", "name")));
        // id <= 5 -> ids 0,1,2,3,4,5 = 6 records
        assertThat(records).hasSize(6);
        GenericRecord first = records.get(0);
        assertThat(first.get("id")).isEqualTo(0);
        assertThat(first.get("name").toString()).isEqualTo("name_0");
        assertThat(first.getSchema().getField("value")).isNull();
    }

    @Test
    void readGroups_withFilter() throws IOException {
        ParquetDataset dataset = open(testFile);
        FilterPredicate filter = FilterApi.gt(FilterApi.intColumn("id"), 95);
        List<Group> records = readAllGroups(dataset.readGroups(filter));
        // id > 95 -> ids 96,97,98,99 = 4 records
        assertThat(records).hasSize(4);
        assertThat(records.get(0).getInteger("id", 0)).isEqualTo(96);
    }

    @Test
    void read_withFilter_emptyResult() throws IOException {
        ParquetDataset dataset = open(testFile);
        // id > 999 matches nothing
        FilterPredicate filter = FilterApi.gt(FilterApi.intColumn("id"), 999);
        List<GenericRecord> records = readAll(dataset.read(filter));
        assertThat(records).isEmpty();
    }

    @Test
    void read_withFilter_compoundPredicate() throws IOException {
        ParquetDataset dataset = open(testFile);
        // id >= 10 AND id < 20 -> ids 10..19 = 10 records
        FilterPredicate filter = FilterApi.and(
                FilterApi.gtEq(FilterApi.intColumn("id"), 10), FilterApi.lt(FilterApi.intColumn("id"), 20));
        List<GenericRecord> records = readAll(dataset.read(filter));
        assertThat(records).hasSize(10);
        assertThat(records).allSatisfy(r -> {
            int id = (int) r.get("id");
            assertThat(id).isBetween(10, 19);
        });
    }

    private static <T> List<T> readAll(CloseableIterator<T> iterator) throws IOException {
        try (iterator) {
            List<T> result = new ArrayList<>();
            while (iterator.hasNext()) {
                result.add(iterator.next());
            }
            return result;
        }
    }

    private static List<Group> readAllGroups(CloseableIterator<Group> iterator) throws IOException {
        try (iterator) {
            List<Group> result = new ArrayList<>();
            while (iterator.hasNext()) {
                result.add(iterator.next());
            }
            return result;
        }
    }

    static void writeTestFile(Path path, MessageType schema, int numRecords, Map<String, String> metadata)
            throws IOException {
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        try (var writer = ExampleParquetWriter.builder(new LocalOutputFile(path))
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withExtraMetaData(metadata)
                .build()) {
            for (int i = 0; i < numRecords; i++) {
                Group group = groupFactory.newGroup();
                if (schema.containsField("id")) group.add("id", i);
                if (schema.containsField("name")) group.add("name", "name_" + i);
                if (schema.containsField("value")) group.add("value", i * 1.0);
                if (schema.containsField("active")) group.add("active", i % 2 == 0);
                if (schema.containsField("timestamp")) group.add("timestamp", 1000L + i);
                writer.write(group);
            }
        }
    }
}
