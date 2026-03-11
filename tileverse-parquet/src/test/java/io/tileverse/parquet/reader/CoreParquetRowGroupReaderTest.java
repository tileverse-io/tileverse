/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 */
package io.tileverse.parquet.reader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.luben.zstd.Zstd;
import io.tileverse.parquet.CloseableIterator;
import io.tileverse.parquet.ParquetDataset;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.format.Type;
import org.apache.parquet.format.Util;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CoreParquetRowGroupReaderTest {

    @TempDir
    Path tempDir;

    private Path sampleFile;

    @BeforeEach
    void setUp() throws Exception {
        sampleFile = resourcePath("geoparquet/sample-geoparquet.parquet");
    }

    @Test
    void read_fixtureFile() throws Exception {
        ParquetDataset dataset = ParquetDataset.open(new LocalInputFile(sampleFile));
        List<GenericRecord> records = readAll(dataset.read());
        assertThat(records).hasSize(3);
    }

    @Test
    void read_withFilter_onFixtureFile() throws Exception {
        FilterPredicate filter =
                FilterApi.eq(FilterApi.binaryColumn("id"), org.apache.parquet.io.api.Binary.fromString("fid-2"));
        ParquetDataset dataset = ParquetDataset.open(new LocalInputFile(sampleFile));
        List<GenericRecord> records = readAll(dataset.read(filter));

        assertThat(records).hasSize(1);
        assertThat(records.get(0).get("id").toString()).isEqualTo("fid-2");
    }

    @Test
    void decompress_throwsForUnsupportedCodec() {
        assertThatThrownBy(() -> CompressionUtil.decompress(CompressionCodec.LZO, new byte[] {1, 2, 3}, 3))
                .isInstanceOf(java.io.IOException.class)
                .hasMessage("Unsupported compression codec in core reader: LZO");
    }

    @Test
    void decompress_supportsZstd() throws Exception {
        byte[] original = "tileverse-zstd".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] compressed = Zstd.compress(original);

        byte[] out = CompressionUtil.decompress(CompressionCodec.ZSTD, compressed, original.length);
        assertThat(out).isEqualTo(original);
    }

    @Test
    void readNextFilteredRowGroup_skipsStatsDroppedGroupAndReadsNext() throws Exception {
        MessageType schema = MessageTypeParser.parseMessageType("message test { optional int32 id; }");
        byte[] chunkBytes = dataPageChunk(new byte[] {1, 2, 3, 4});
        Path file = writeTempFile(chunkBytes);

        CoreColumnChunkMeta dropped = new CoreColumnChunkMeta(
                "id",
                List.of("id"),
                Type.INT32,
                CompressionCodec.UNCOMPRESSED,
                1,
                chunkBytes.length,
                0,
                0,
                -1,
                statsWithNullCount(0),
                null,
                null,
                null,
                null);
        CoreColumnChunkMeta kept = new CoreColumnChunkMeta(
                "id",
                List.of("id"),
                Type.INT32,
                CompressionCodec.UNCOMPRESSED,
                1,
                chunkBytes.length,
                0,
                0,
                -1,
                statsWithNullCount(1),
                null,
                null,
                null,
                null);

        CoreParquetFooter footer = new CoreParquetFooter(
                schema,
                java.util.Map.of(),
                2,
                List.of(new CoreRowGroupMeta(1, List.of(dropped)), new CoreRowGroupMeta(1, List.of(kept))));
        CoreParquetReadOptions options = CoreParquetReadOptions.builder()
                .useStatsFilter(true)
                .useDictionaryFilter(false)
                .withRecordFilter(FilterCompat.get(FilterApi.eq(FilterApi.intColumn("id"), null)))
                .build();
        CoreParquetRowGroupReader reader =
                new CoreParquetRowGroupReader(new LocalInputFile(file), footer, options, schema);

        PageReadStore store = reader.readNextFilteredRowGroup();
        assertThat(store).isNotNull();
        assertThat(store.getRowCount()).isEqualTo(1);
        assertThat(reader.readNextFilteredRowGroup()).isNull();
    }

    @Test
    void readNextFilteredRowGroup_usesDictionaryPruningFromDictionaryPage() throws Exception {
        MessageType schema = MessageTypeParser.parseMessageType("message test { optional int32 id; }");
        byte[] dictChunk = dictionaryOnlyChunk(7);

        byte[] fileBytes = new byte[dictChunk.length + 1];
        System.arraycopy(dictChunk, 0, fileBytes, 1, dictChunk.length);
        Path file = writeTempFile(fileBytes);

        CoreColumnChunkMeta column = new CoreColumnChunkMeta(
                "id",
                List.of("id"),
                Type.INT32,
                CompressionCodec.UNCOMPRESSED,
                1,
                dictChunk.length,
                1,
                1 + dictChunk.length,
                1,
                statsWithNullCount(0),
                null,
                null,
                null,
                null);
        CoreParquetFooter footer =
                new CoreParquetFooter(schema, java.util.Map.of(), 1, List.of(new CoreRowGroupMeta(1, List.of(column))));

        CoreParquetReadOptions dropByEq = CoreParquetReadOptions.builder()
                .useStatsFilter(false)
                .useDictionaryFilter(true)
                .withRecordFilter(FilterCompat.get(FilterApi.eq(FilterApi.intColumn("id"), 99)))
                .build();
        CoreParquetRowGroupReader readerDrop =
                new CoreParquetRowGroupReader(new LocalInputFile(file), footer, dropByEq, schema);
        assertThat(readerDrop.readNextFilteredRowGroup()).isNull();

        CoreParquetReadOptions keepByEq = CoreParquetReadOptions.builder()
                .useStatsFilter(false)
                .useDictionaryFilter(true)
                .withRecordFilter(FilterCompat.get(FilterApi.eq(FilterApi.intColumn("id"), 7)))
                .build();
        CoreParquetRowGroupReader readerKeep =
                new CoreParquetRowGroupReader(new LocalInputFile(file), footer, keepByEq, schema);
        assertThat(readerKeep.readNextFilteredRowGroup()).isNotNull();

        CoreParquetReadOptions dropByNotEq = CoreParquetReadOptions.builder()
                .useStatsFilter(false)
                .useDictionaryFilter(true)
                .withRecordFilter(FilterCompat.get(FilterApi.notEq(FilterApi.intColumn("id"), 7)))
                .build();
        CoreParquetRowGroupReader readerDropNotEq =
                new CoreParquetRowGroupReader(new LocalInputFile(file), footer, dropByNotEq, schema);
        assertThat(readerDropNotEq.readNextFilteredRowGroup()).isNull();
    }

    @Test
    void readColumnChunk_throwsForInvalidChunkSize() throws Exception {
        MessageType schema = MessageTypeParser.parseMessageType("message test { optional int32 id; }");
        Path file = writeTempFile(new byte[] {1, 2, 3});
        CoreParquetRowGroupReader reader = new CoreParquetRowGroupReader(
                new LocalInputFile(file),
                new CoreParquetFooter(schema, java.util.Map.of(), 0, List.of()),
                CoreParquetReadOptions.defaults(),
                schema);

        CoreColumnChunkMeta invalid = new CoreColumnChunkMeta(
                "id",
                List.of("id"),
                Type.INT32,
                CompressionCodec.UNCOMPRESSED,
                1,
                (long) Integer.MAX_VALUE + 1,
                0,
                0,
                -1,
                null,
                null,
                null,
                null,
                null);

        ColumnDescriptor descriptor = schema.getColumns().get(0);

        try (SeekableInputStream in = new LocalInputFile(file).newStream()) {
            assertThatThrownBy(() -> reader.readColumnChunk(in, invalid, descriptor))
                    .isInstanceOf(IOException.class)
                    .hasMessage("Unsupported column chunk size: 2147483648");
        }
    }

    @Test
    void readColumnChunk_throwsForMalformedPages() throws Exception {
        MessageType schema = MessageTypeParser.parseMessageType("message test { optional int32 id; }");
        ColumnDescriptor descriptor = schema.getColumns().get(0);

        byte[] truncatedPayloadChunk = truncatedPayloadChunk();
        Path truncatedFile = writeTempFile(truncatedPayloadChunk);
        CoreParquetRowGroupReader truncatedReader = new CoreParquetRowGroupReader(
                new LocalInputFile(truncatedFile),
                new CoreParquetFooter(schema, java.util.Map.of(), 0, List.of()),
                CoreParquetReadOptions.defaults(),
                schema);
        CoreColumnChunkMeta truncatedMeta = new CoreColumnChunkMeta(
                "id",
                List.of("id"),
                Type.INT32,
                CompressionCodec.UNCOMPRESSED,
                1,
                truncatedPayloadChunk.length,
                0,
                0,
                -1,
                null,
                null,
                null,
                null,
                null);
        try (SeekableInputStream in = new LocalInputFile(truncatedFile).newStream()) {
            assertThatThrownBy(() -> truncatedReader.readColumnChunk(in, truncatedMeta, descriptor))
                    .isInstanceOf(IOException.class)
                    .hasMessage("Unexpected EOF while reading page payload");
        }

        byte[] invalidV2Chunk = invalidDataPageV2Chunk();
        Path invalidV2File = writeTempFile(invalidV2Chunk);
        CoreParquetRowGroupReader invalidV2Reader = new CoreParquetRowGroupReader(
                new LocalInputFile(invalidV2File),
                new CoreParquetFooter(schema, java.util.Map.of(), 0, List.of()),
                CoreParquetReadOptions.defaults(),
                schema);
        CoreColumnChunkMeta invalidV2Meta = new CoreColumnChunkMeta(
                "id",
                List.of("id"),
                Type.INT32,
                CompressionCodec.UNCOMPRESSED,
                1,
                invalidV2Chunk.length,
                0,
                0,
                -1,
                null,
                null,
                null,
                null,
                null);
        try (SeekableInputStream in = new LocalInputFile(invalidV2File).newStream()) {
            assertThatThrownBy(() -> invalidV2Reader.readColumnChunk(in, invalidV2Meta, descriptor))
                    .isInstanceOf(IOException.class)
                    .hasMessage("Invalid DATA_PAGE_V2 level lengths");
        }
    }

    @Test
    void setRequestedSchema_rejectsNull() throws Exception {
        MessageType schema = MessageTypeParser.parseMessageType("message test { optional int32 id; }");
        CoreParquetRowGroupReader reader = new CoreParquetRowGroupReader(
                new LocalInputFile(sampleFile),
                new CoreParquetFooter(schema, java.util.Map.of(), 0, List.of()),
                CoreParquetReadOptions.defaults(),
                schema);
        assertThatThrownBy(() -> reader.setRequestedSchema(null)).isInstanceOf(NullPointerException.class);
    }

    private Path writeTempFile(byte[] bytes) throws IOException {
        Path file = tempDir.resolve("chunk-" + System.nanoTime() + ".bin");
        Files.write(file, bytes);
        return file;
    }

    private static byte[] dataPageChunk(byte[] payload) throws IOException {
        DataPageHeader dataHeader = new DataPageHeader(1, Encoding.PLAIN, Encoding.RLE, Encoding.RLE);
        PageHeader pageHeader = new PageHeader(PageType.DATA_PAGE, payload.length, payload.length);
        pageHeader.setData_page_header(dataHeader);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Util.writePageHeader(pageHeader, out);
        out.write(payload);
        return out.toByteArray();
    }

    private static byte[] dictionaryOnlyChunk(int value) throws IOException {
        byte[] payload = ByteBuffer.allocate(4)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putInt(value)
                .array();
        DictionaryPageHeader dictionaryHeader = new DictionaryPageHeader(1, Encoding.PLAIN);
        PageHeader pageHeader = new PageHeader(PageType.DICTIONARY_PAGE, payload.length, payload.length);
        pageHeader.setDictionary_page_header(dictionaryHeader);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Util.writePageHeader(pageHeader, out);
        out.write(payload);
        return out.toByteArray();
    }

    private static byte[] truncatedPayloadChunk() throws IOException {
        byte[] payload = new byte[] {1, 2};
        DataPageHeader dataHeader = new DataPageHeader(1, Encoding.PLAIN, Encoding.RLE, Encoding.RLE);
        PageHeader pageHeader = new PageHeader(PageType.DATA_PAGE, 4, 4);
        pageHeader.setData_page_header(dataHeader);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Util.writePageHeader(pageHeader, out);
        out.write(payload);
        return out.toByteArray();
    }

    private static byte[] invalidDataPageV2Chunk() throws IOException {
        byte[] payload = new byte[] {1, 2, 3};
        DataPageHeaderV2 dataHeader = new DataPageHeaderV2(1, 0, 1, Encoding.PLAIN, 2, 2);
        dataHeader.setIs_compressed(false);
        PageHeader pageHeader = new PageHeader(PageType.DATA_PAGE_V2, payload.length, payload.length);
        pageHeader.setData_page_header_v2(dataHeader);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Util.writePageHeader(pageHeader, out);
        out.write(payload);
        return out.toByteArray();
    }

    private static Statistics statsWithNullCount(long nullCount) {
        Statistics stats = new Statistics();
        stats.setNull_count(nullCount);
        return stats;
    }

    private static <T> List<T> readAll(CloseableIterator<T> iterator) throws Exception {
        try (iterator) {
            List<T> result = new ArrayList<>();
            while (iterator.hasNext()) {
                result.add(iterator.next());
            }
            return result;
        }
    }

    private static Path resourcePath(String resource) throws Exception {
        URL url = CoreParquetRowGroupReaderTest.class.getClassLoader().getResource(resource);
        if (url == null) {
            throw new IllegalStateException("Missing resource: " + resource);
        }
        return Path.of(url.toURI());
    }
}
