/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 */
package io.tileverse.parquet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ParquetDatasetTest {

    private Path sampleFile;

    @BeforeEach
    void setUp() throws Exception {
        sampleFile = resourcePath("geoparquet/sample-geoparquet.parquet");
    }

    private ParquetDataset open(Path file) throws IOException {
        return ParquetDataset.open(new LocalInputFile(file));
    }

    @Test
    void getSchema_returnsExpectedTopLevelFields() throws IOException {
        ParquetDataset dataset = open(sampleFile);
        MessageType schema = dataset.getSchema();

        assertThat(schema.getFieldCount()).isGreaterThanOrEqualTo(3);
        assertThat(schema.getFields().stream().map(Type::getName)).contains("id", "geometry");
    }

    @Test
    void getKeyValueMetadata_containsGeoMetadata() throws IOException {
        ParquetDataset dataset = open(sampleFile);
        Map<String, String> metadata = dataset.getKeyValueMetadata();

        assertThat(metadata).containsKey("geo");
        assertThat(metadata.get("geo")).contains("primary_column");
    }

    @Test
    void getRecordCount_returnsExpectedCount() throws IOException {
        ParquetDataset dataset = open(sampleFile);
        assertThat(dataset.getRecordCount()).isEqualTo(3);
    }

    @Test
    void read_returnsAllRecordsAsGenericRecord() throws IOException {
        ParquetDataset dataset = open(sampleFile);
        List<GenericRecord> records = readAll(dataset.read());

        assertThat(records).hasSize(3);
        assertThat(records.get(0).get("id").toString()).isEqualTo("fid-1");
    }

    @Test
    void read_withColumnProjection_returnsOnlyRequestedColumns() throws IOException {
        ParquetDataset dataset = open(sampleFile);
        List<GenericRecord> records = readAll(dataset.read(Set.of("id")));

        assertThat(records).hasSize(3);
        GenericRecord first = records.get(0);
        assertThat(first.get("id").toString()).isEqualTo("fid-1");
        assertThat(first.getSchema().getField("geometry")).isNull();
    }

    @Test
    void readGroups_returnsAllRecordsAsGroup() throws IOException {
        ParquetDataset dataset = open(sampleFile);
        List<Group> records = readAllGroups(dataset.readGroups());

        assertThat(records).hasSize(3);
        assertThat(records.get(0).getString("id", 0)).isEqualTo("fid-1");
    }

    @Test
    void read_withFilter_returnsOnlyMatchingRecords() throws IOException {
        ParquetDataset dataset = open(sampleFile);
        FilterPredicate filter =
                FilterApi.eq(FilterApi.binaryColumn("id"), org.apache.parquet.io.api.Binary.fromString("fid-2"));

        List<GenericRecord> records = readAll(dataset.read(filter));

        assertThat(records).hasSize(1);
        assertThat(records.get(0).get("id").toString()).isEqualTo("fid-2");
    }

    @Test
    void read_withFilterAndProjection_combinesCorrectly() throws IOException {
        ParquetDataset dataset = open(sampleFile);
        FilterPredicate filter =
                FilterApi.eq(FilterApi.binaryColumn("id"), org.apache.parquet.io.api.Binary.fromString("fid-3"));

        List<GenericRecord> records = readAll(dataset.read(filter, Set.of("id")));

        assertThat(records).hasSize(1);
        assertThat(records.get(0).get("id").toString()).isEqualTo("fid-3");
        assertThat(records.get(0).getSchema().getField("geometry")).isNull();
    }

    @Test
    void read_withFilter_emptyResult() throws IOException {
        ParquetDataset dataset = open(sampleFile);
        FilterPredicate filter =
                FilterApi.eq(FilterApi.binaryColumn("id"), org.apache.parquet.io.api.Binary.fromString("missing"));

        List<GenericRecord> records = readAll(dataset.read(filter));
        assertThat(records).isEmpty();
    }

    @Test
    void projectSchema_throwsOnUnknownColumn() throws IOException {
        ParquetDataset dataset = open(sampleFile);
        assertThatThrownBy(() -> ParquetDataset.projectSchema(dataset.getSchema(), Set.of("nonexistent")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Column 'nonexistent' not found");
    }

    @Test
    void read_withFilter_prunesRowGroupsForNoMatch() throws IOException {
        CountingInputFile countingInput = new CountingInputFile(new LocalInputFile(sampleFile));
        ParquetDataset dataset = ParquetDataset.open(countingInput);

        countingInput.reset();
        FilterPredicate noMatch =
                FilterApi.eq(FilterApi.binaryColumn("id"), org.apache.parquet.io.api.Binary.fromString("missing"));
        List<GenericRecord> filtered = readAll(dataset.read(noMatch));

        assertThat(filtered).isEmpty();
        assertThat(countingInput.bytesRead()).isEqualTo(0L);
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

    private static Path resourcePath(String resource) throws Exception {
        URL url = ParquetDatasetTest.class.getClassLoader().getResource(resource);
        if (url == null) {
            throw new IllegalStateException("Missing resource: " + resource);
        }
        return Path.of(url.toURI());
    }

    private static final class CountingInputFile implements InputFile {
        private final InputFile delegate;
        private final AtomicLong bytesRead = new AtomicLong();

        private CountingInputFile(InputFile delegate) {
            this.delegate = delegate;
        }

        @Override
        public long getLength() throws IOException {
            return delegate.getLength();
        }

        @Override
        public SeekableInputStream newStream() throws IOException {
            return new CountingSeekableInputStream(delegate.newStream(), bytesRead);
        }

        long bytesRead() {
            return bytesRead.get();
        }

        void reset() {
            bytesRead.set(0L);
        }
    }

    private static final class CountingSeekableInputStream extends SeekableInputStream {
        private final SeekableInputStream delegate;
        private final AtomicLong bytesRead;

        private CountingSeekableInputStream(SeekableInputStream delegate, AtomicLong bytesRead) {
            this.delegate = delegate;
            this.bytesRead = bytesRead;
        }

        @Override
        public long getPos() throws IOException {
            return delegate.getPos();
        }

        @Override
        public void seek(long newPos) throws IOException {
            delegate.seek(newPos);
        }

        @Override
        public void readFully(byte[] bytes) throws IOException {
            delegate.readFully(bytes);
            bytesRead.addAndGet(bytes.length);
        }

        @Override
        public void readFully(byte[] bytes, int start, int len) throws IOException {
            delegate.readFully(bytes, start, len);
            bytesRead.addAndGet(len);
        }

        @Override
        public int read(java.nio.ByteBuffer buf) throws IOException {
            int n = delegate.read(buf);
            if (n > 0) {
                bytesRead.addAndGet(n);
            }
            return n;
        }

        @Override
        public void readFully(java.nio.ByteBuffer buf) throws IOException {
            int remaining = buf.remaining();
            delegate.readFully(buf);
            bytesRead.addAndGet(remaining);
        }

        @Override
        public int read() throws IOException {
            int n = delegate.read();
            if (n >= 0) {
                bytesRead.incrementAndGet();
            }
            return n;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int n = delegate.read(b, off, len);
            if (n > 0) {
                bytesRead.addAndGet(n);
            }
            return n;
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
