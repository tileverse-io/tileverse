/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
 */
package io.tileverse.parquet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tileverse.parquet.reader.CoreParquetFooter;
import io.tileverse.parquet.reader.CoreParquetFooterReader;
import io.tileverse.parquet.reader.CoreParquetReadOptions;
import io.tileverse.parquet.reader.CoreParquetRowGroupReader;
import io.tileverse.parquet.reader.ParquetRowGroupReader;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ParquetRecordIteratorTest {

    private Path sampleFile;

    @BeforeEach
    void setUp() throws Exception {
        sampleFile = resourcePath("geoparquet/sample-geoparquet.parquet");
    }

    private ParquetRecordIterator<org.apache.parquet.example.data.Group> openGroupIterator(Path file)
            throws IOException {
        LocalInputFile inputFile = new LocalInputFile(file);
        CoreParquetFooter footer = CoreParquetFooterReader.read(inputFile);
        MessageType fileSchema = footer.schema();
        RecordMaterializer<org.apache.parquet.example.data.Group> materializer =
                GroupMaterializerProvider.INSTANCE.createMaterializer(fileSchema, fileSchema, Map.of());
        ParquetRowGroupReader reader =
                new CoreParquetRowGroupReader(inputFile, footer, CoreParquetReadOptions.defaults(), fileSchema);
        return new ParquetRecordIterator<>(reader, materializer, fileSchema, fileSchema);
    }

    @Test
    void iteratesAllRecords() throws IOException {
        try (var iterator = openGroupIterator(sampleFile)) {
            List<org.apache.parquet.example.data.Group> records = new ArrayList<>();
            while (iterator.hasNext()) {
                records.add(iterator.next());
            }
            assertThat(records).hasSize(3);
            assertThat(records.get(0).getString("id", 0)).isEqualTo("fid-1");
        }
    }

    @Test
    void hasNext_returnsFalseAfterLastRecord() throws IOException {
        try (var iterator = openGroupIterator(sampleFile)) {
            iterator.next();
            iterator.next();
            iterator.next();
            assertThat(iterator.hasNext()).isFalse();
        }
    }

    @Test
    void next_throwsNoSuchElementException_whenExhausted() throws IOException {
        try (var iterator = openGroupIterator(sampleFile)) {
            iterator.next();
            iterator.next();
            iterator.next();
            assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
        }
    }

    @Test
    void close_isIdempotent() throws IOException {
        var iterator = openGroupIterator(sampleFile);
        iterator.close();
        iterator.close();
        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    void filter_skipsNonMatchingRecords() throws IOException {
        FilterPredicate pred =
                FilterApi.eq(FilterApi.binaryColumn("id"), org.apache.parquet.io.api.Binary.fromString("fid-3"));
        FilterCompat.Filter filter = FilterCompat.get(pred);

        LocalInputFile inputFile = new LocalInputFile(sampleFile);
        CoreParquetFooter footer = CoreParquetFooterReader.read(inputFile);
        CoreParquetReadOptions options =
                CoreParquetReadOptions.builder().withRecordFilter(filter).build();
        try (ParquetRowGroupReader reader =
                new CoreParquetRowGroupReader(inputFile, footer, options, footer.schema())) {
            MessageType fileSchema = footer.schema();
            RecordMaterializer<org.apache.parquet.example.data.Group> materializer =
                    GroupMaterializerProvider.INSTANCE.createMaterializer(fileSchema, fileSchema, Map.of());
            try (var iterator = new ParquetRecordIterator<>(reader, materializer, fileSchema, fileSchema, filter)) {
                List<org.apache.parquet.example.data.Group> records = new ArrayList<>();
                while (iterator.hasNext()) {
                    records.add(iterator.next());
                }
                assertThat(records).hasSize(1);
                assertThat(records.get(0).getString("id", 0)).isEqualTo("fid-3");
            }
        }
    }

    private static Path resourcePath(String resource) throws Exception {
        URL url = ParquetRecordIteratorTest.class.getClassLoader().getResource(resource);
        if (url == null) {
            throw new IllegalStateException("Missing resource: " + resource);
        }
        return Path.of(url.toURI());
    }
}
