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
package io.tileverse.parquet.reader;

import com.github.luben.zstd.Zstd;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Type;
import org.apache.parquet.format.Util;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.xerial.snappy.Snappy;

public final class CoreParquetRowGroupReader implements ParquetRowGroupReader {
    private final InputFile inputFile;
    private final CoreParquetReadOptions options;
    private final List<CoreRowGroupMeta> rowGroups;
    private final MessageType fileSchema;
    private final FilterPredicate filterPredicate;
    private MessageType requestedSchema;
    private int nextRowGroupIndex;

    public CoreParquetRowGroupReader(
            InputFile inputFile, CoreParquetFooter footer, CoreParquetReadOptions options, MessageType fileSchema) {
        this.inputFile = Objects.requireNonNull(inputFile, "inputFile");
        this.options = Objects.requireNonNull(options, "options");
        this.fileSchema = Objects.requireNonNull(fileSchema, "fileSchema");
        this.requestedSchema = fileSchema;
        this.rowGroups = footer.rowGroups();
        FilterCompat.Filter recordFilter = options.getRecordFilter();
        this.filterPredicate = recordFilter instanceof FilterCompat.FilterPredicateCompat predicateCompat
                ? predicateCompat.getFilterPredicate()
                : null;
    }

    @Override
    public void setRequestedSchema(MessageType requestedSchema) {
        this.requestedSchema = Objects.requireNonNull(requestedSchema, "requestedSchema");
    }

    @Override
    public PageReadStore readNextRowGroup() throws IOException {
        if (nextRowGroupIndex >= rowGroups.size()) {
            return null;
        }
        CoreRowGroupMeta rowGroup = rowGroups.get(nextRowGroupIndex++);
        return readRowGroup(rowGroup);
    }

    @Override
    public PageReadStore readNextFilteredRowGroup() throws IOException {
        while (nextRowGroupIndex < rowGroups.size()) {
            CoreRowGroupMeta rowGroup = rowGroups.get(nextRowGroupIndex++);
            if (canDropByStats(rowGroup)) {
                continue;
            }
            if (canDropByDictionary(rowGroup)) {
                continue;
            }
            // Column-index pushdown intentionally omitted in hard-decoupled core path.
            return readRowGroup(rowGroup);
        }
        return null;
    }

    @Override
    public void close() {}

    private PageReadStore readRowGroup(CoreRowGroupMeta rowGroup) throws IOException {
        Map<ColumnDescriptor, PageReader> pageReaders = new HashMap<>();
        Map<String, CoreColumnChunkMeta> columnByPath = new HashMap<>();
        for (CoreColumnChunkMeta column : rowGroup.columns()) {
            columnByPath.put(column.path(), column);
        }

        for (ColumnDescriptor descriptor : requestedSchema.getColumns()) {
            String path = String.join(".", descriptor.getPath());
            CoreColumnChunkMeta columnMeta = columnByPath.get(path);
            if (columnMeta == null) {
                continue;
            }
            CoreColumnChunk chunk = readColumnChunk(columnMeta, descriptor);
            pageReaders.put(descriptor, new CorePageReader(chunk.valueCount, chunk.pages, chunk.dictionaryPage));
        }

        return new CorePageReadStore(rowGroup.rowCount(), pageReaders);
    }

    private CoreColumnChunk readColumnChunk(CoreColumnChunkMeta columnMeta, ColumnDescriptor descriptor)
            throws IOException {
        long start = columnMeta.startOffset();
        long totalSize = columnMeta.totalCompressedSize();
        if (totalSize < 0 || totalSize > Integer.MAX_VALUE) {
            throw new IOException("Unsupported column chunk size: " + totalSize);
        }

        byte[] chunkBytes = readRange(start, (int) totalSize);
        PrimitiveType primitiveType = descriptor.getPrimitiveType();

        DictionaryPage dictionaryPage = null;
        List<DataPage> dataPages = new ArrayList<>();
        ByteArrayInputStream in = new ByteArrayInputStream(chunkBytes);

        while (in.available() > 0) {
            PageHeader header = Util.readPageHeader(in);
            int payloadSize = header.getCompressed_page_size();
            byte[] payload = in.readNBytes(payloadSize);
            if (payload.length != payloadSize) {
                throw new IOException("Unexpected EOF while reading page payload");
            }

            if (header.getType() == PageType.DICTIONARY_PAGE) {
                DictionaryPageHeader dictionaryHeader = header.getDictionary_page_header();
                byte[] decompressed = decompress(columnMeta.codec(), payload, header.getUncompressed_page_size());
                dictionaryPage = new DictionaryPage(
                        org.apache.parquet.bytes.BytesInput.from(decompressed),
                        dictionaryHeader.getNum_values(),
                        toEncoding(dictionaryHeader.getEncoding()));
                continue;
            }

            if (header.getType() == PageType.DATA_PAGE) {
                DataPageHeader dataHeader = header.getData_page_header();
                byte[] decompressed = decompress(columnMeta.codec(), payload, header.getUncompressed_page_size());
                Statistics<?> stats = Statistics.noopStats(primitiveType);
                DataPageV1 page = new DataPageV1(
                        org.apache.parquet.bytes.BytesInput.from(decompressed),
                        dataHeader.getNum_values(),
                        header.getUncompressed_page_size(),
                        stats,
                        toEncoding(dataHeader.getRepetition_level_encoding()),
                        toEncoding(dataHeader.getDefinition_level_encoding()),
                        toEncoding(dataHeader.getEncoding()));
                dataPages.add(page);
                continue;
            }

            if (header.getType() == PageType.DATA_PAGE_V2) {
                DataPageHeaderV2 dataHeader = header.getData_page_header_v2();
                int repLen = dataHeader.getRepetition_levels_byte_length();
                int defLen = dataHeader.getDefinition_levels_byte_length();
                if (repLen + defLen > payload.length) {
                    throw new IOException("Invalid DATA_PAGE_V2 level lengths");
                }

                org.apache.parquet.bytes.BytesInput repetitionLevels =
                        org.apache.parquet.bytes.BytesInput.from(payload, 0, repLen);
                org.apache.parquet.bytes.BytesInput definitionLevels =
                        org.apache.parquet.bytes.BytesInput.from(payload, repLen, defLen);

                int dataOffset = repLen + defLen;
                byte[] dataPayload = Arrays.copyOfRange(payload, dataOffset, payload.length);
                byte[] decodedData = dataHeader.isIs_compressed()
                        ? decompress(
                                columnMeta.codec(), dataPayload, header.getUncompressed_page_size() - repLen - defLen)
                        : dataPayload;

                Statistics<?> stats = Statistics.noopStats(primitiveType);
                DataPageV2 page = DataPageV2.uncompressed(
                        dataHeader.getNum_rows(),
                        dataHeader.getNum_nulls(),
                        dataHeader.getNum_values(),
                        repetitionLevels,
                        definitionLevels,
                        toEncoding(dataHeader.getEncoding()),
                        org.apache.parquet.bytes.BytesInput.from(decodedData),
                        stats);
                dataPages.add(page);
            }
        }

        return new CoreColumnChunk(columnMeta.valueCount(), dataPages, dictionaryPage);
    }

    private static byte[] decompress(CompressionCodec codec, byte[] payload, int uncompressedSize) throws IOException {
        return switch (codec) {
            case UNCOMPRESSED -> payload;
            case SNAPPY -> Snappy.uncompress(payload);
            case GZIP -> readAll(new GZIPInputStream(new ByteArrayInputStream(payload)));
            case ZSTD -> decompressZstd(payload, uncompressedSize);
            default -> throw new IOException("Unsupported compression codec in core reader: " + codec);
        };
    }

    private static byte[] decompressZstd(byte[] payload, int uncompressedSize) throws IOException {
        byte[] out = Zstd.decompress(payload, uncompressedSize);
        if (out.length != uncompressedSize) {
            throw new IOException("Unexpected ZSTD decompressed size: " + out.length + " expected " + uncompressedSize);
        }
        return out;
    }

    private static byte[] readAll(GZIPInputStream in) throws IOException {
        try (in;
                java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream()) {
            byte[] buf = new byte[8192];
            int n;
            while ((n = in.read(buf)) >= 0) {
                out.write(buf, 0, n);
            }
            return out.toByteArray();
        }
    }

    private static Encoding toEncoding(org.apache.parquet.format.Encoding encoding) {
        return Encoding.valueOf(encoding.name());
    }

    private boolean canDropByStats(CoreRowGroupMeta rowGroup) {
        if (filterPredicate == null || !options.useStatsFilter()) {
            return false;
        }
        Map<String, CoreColumnChunkMeta> byPath = new HashMap<>();
        for (CoreColumnChunkMeta column : rowGroup.columns()) {
            byPath.put(column.path(), column);
        }
        return filterPredicate.accept(new StatsPruningVisitor(byPath));
    }

    private boolean canDropByDictionary(CoreRowGroupMeta rowGroup) {
        if (filterPredicate == null || !options.useDictionaryFilter()) {
            return false;
        }
        Map<String, CoreColumnChunkMeta> byPath = new HashMap<>();
        for (CoreColumnChunkMeta column : rowGroup.columns()) {
            byPath.put(column.path(), column);
        }
        try {
            return filterPredicate.accept(new DictionaryPruningVisitor(byPath));
        } catch (RuntimeException e) {
            return false;
        }
    }

    private DictionaryPage readDictionaryPage(CoreColumnChunkMeta columnMeta) throws IOException {
        if (!columnMeta.hasDictionaryPage()) {
            return null;
        }
        long dictionaryOffset = columnMeta.dictionaryPageOffset();
        long firstDataPageOffset = columnMeta.firstDataPageOffset();
        long chunkStart = columnMeta.startOffset();
        long chunkEnd = chunkStart + columnMeta.totalCompressedSize();

        if (dictionaryOffset <= 0) {
            dictionaryOffset = chunkStart;
        }
        long scanEnd = firstDataPageOffset > dictionaryOffset ? firstDataPageOffset : chunkEnd;
        long scanLength = scanEnd - dictionaryOffset;
        if (scanLength <= 0 || scanLength > Integer.MAX_VALUE) {
            return null;
        }

        byte[] bytes = readRange(dictionaryOffset, (int) scanLength);
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        PageHeader header = Util.readPageHeader(in);
        if (header.getType() != PageType.DICTIONARY_PAGE || !header.isSetDictionary_page_header()) {
            return null;
        }
        int payloadSize = header.getCompressed_page_size();
        byte[] payload = in.readNBytes(payloadSize);
        if (payload.length != payloadSize) {
            throw new IOException("Unexpected EOF while reading dictionary page payload");
        }

        DictionaryPageHeader dictionaryHeader = header.getDictionary_page_header();
        byte[] decompressed = decompress(columnMeta.codec(), payload, header.getUncompressed_page_size());
        return new DictionaryPage(
                org.apache.parquet.bytes.BytesInput.from(decompressed),
                dictionaryHeader.getNum_values(),
                toEncoding(dictionaryHeader.getEncoding()));
    }

    private Set<Object> decodeDictionaryValues(CoreColumnChunkMeta columnMeta) throws IOException {
        ColumnDescriptor descriptor = fileSchema.getColumnDescription(columnMeta.pathArray());
        DictionaryPage dictionaryPage = readDictionaryPage(columnMeta);
        if (dictionaryPage == null) {
            return Set.of();
        }
        Dictionary dictionary = dictionaryPage.getEncoding().initDictionary(descriptor, dictionaryPage);
        Set<Object> values = new HashSet<>();
        for (int id = 0; id <= dictionary.getMaxId(); id++) {
            values.add(decodeDictionaryValue(descriptor.getPrimitiveType(), dictionary, id));
        }
        return values;
    }

    private static Object decodeDictionaryValue(PrimitiveType primitiveType, Dictionary dictionary, int id) {
        return switch (primitiveType.getPrimitiveTypeName()) {
            case BOOLEAN -> dictionary.decodeToBoolean(id);
            case INT32 -> dictionary.decodeToInt(id);
            case INT64 -> dictionary.decodeToLong(id);
            case FLOAT -> dictionary.decodeToFloat(id);
            case DOUBLE -> dictionary.decodeToDouble(id);
            case BINARY, FIXED_LEN_BYTE_ARRAY, INT96 -> dictionary.decodeToBinary(id);
        };
    }

    private byte[] readRange(long offset, int length) throws IOException {
        byte[] bytes = new byte[length];
        try (SeekableInputStream in = inputFile.newStream()) {
            in.seek(offset);
            in.readFully(bytes);
        }
        return bytes;
    }

    private static final class StatsPruningVisitor implements FilterPredicate.Visitor<Boolean> {
        private final Map<String, CoreColumnChunkMeta> columns;

        StatsPruningVisitor(Map<String, CoreColumnChunkMeta> columns) {
            this.columns = columns;
        }

        @Override
        public <T extends Comparable<T>> Boolean visit(Operators.Eq<T> eq) {
            CoreColumnChunkMeta column = column(eq.getColumn());
            if (column == null) {
                return false;
            }
            T value = eq.getValue();
            if (value == null) {
                Long nullCount = nullCount(column);
                return nullCount != null && nullCount == 0L;
            }
            Comparable<?> min = minValue(column);
            Comparable<?> max = maxValue(column);
            if (min == null || max == null) {
                return false;
            }
            return compare(min, value) > 0 || compare(max, value) < 0;
        }

        @Override
        public <T extends Comparable<T>> Boolean visit(Operators.NotEq<T> notEq) {
            CoreColumnChunkMeta column = column(notEq.getColumn());
            if (column == null) {
                return false;
            }
            T value = notEq.getValue();
            if (value == null) {
                return false;
            }
            Comparable<?> min = minValue(column);
            Comparable<?> max = maxValue(column);
            Long nullCount = nullCount(column);
            if (min == null || max == null || nullCount == null) {
                return false;
            }
            return compare(min, value) == 0 && compare(max, value) == 0 && nullCount == 0L;
        }

        @Override
        public <T extends Comparable<T>> Boolean visit(Operators.Lt<T> lt) {
            CoreColumnChunkMeta column = column(lt.getColumn());
            if (column == null || lt.getValue() == null) {
                return false;
            }
            Comparable<?> min = minValue(column);
            return min != null && compare(min, lt.getValue()) >= 0;
        }

        @Override
        public <T extends Comparable<T>> Boolean visit(Operators.LtEq<T> ltEq) {
            CoreColumnChunkMeta column = column(ltEq.getColumn());
            if (column == null || ltEq.getValue() == null) {
                return false;
            }
            Comparable<?> min = minValue(column);
            return min != null && compare(min, ltEq.getValue()) > 0;
        }

        @Override
        public <T extends Comparable<T>> Boolean visit(Operators.Gt<T> gt) {
            CoreColumnChunkMeta column = column(gt.getColumn());
            if (column == null || gt.getValue() == null) {
                return false;
            }
            Comparable<?> max = maxValue(column);
            return max != null && compare(max, gt.getValue()) <= 0;
        }

        @Override
        public <T extends Comparable<T>> Boolean visit(Operators.GtEq<T> gtEq) {
            CoreColumnChunkMeta column = column(gtEq.getColumn());
            if (column == null || gtEq.getValue() == null) {
                return false;
            }
            Comparable<?> max = maxValue(column);
            return max != null && compare(max, gtEq.getValue()) < 0;
        }

        @Override
        public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Boolean visit(
                Operators.UserDefined<T, U> udp) {
            return false;
        }

        @Override
        public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Boolean visit(
                Operators.LogicalNotUserDefined<T, U> udp) {
            return false;
        }

        @Override
        public Boolean visit(Operators.And and) {
            return and.getLeft().accept(this) || and.getRight().accept(this);
        }

        @Override
        public Boolean visit(Operators.Or or) {
            return or.getLeft().accept(this) && or.getRight().accept(this);
        }

        @Override
        public Boolean visit(Operators.Not not) {
            return false;
        }

        private CoreColumnChunkMeta column(Operators.Column<?> column) {
            return columns.get(column.getColumnPath().toDotString());
        }

        private Long nullCount(CoreColumnChunkMeta column) {
            org.apache.parquet.format.Statistics s = column.statistics();
            return s != null && s.isSetNull_count() ? s.getNull_count() : null;
        }

        private Comparable<?> minValue(CoreColumnChunkMeta column) {
            return decodeBound(column, true);
        }

        private Comparable<?> maxValue(CoreColumnChunkMeta column) {
            return decodeBound(column, false);
        }

        private Comparable<?> decodeBound(CoreColumnChunkMeta column, boolean min) {
            org.apache.parquet.format.Statistics stats = column.statistics();
            if (stats == null) {
                return null;
            }
            byte[] bytes = min
                    ? (stats.isSetMin_value() ? stats.getMin_value() : stats.isSetMin() ? stats.getMin() : null)
                    : (stats.isSetMax_value() ? stats.getMax_value() : stats.isSetMax() ? stats.getMax() : null);
            if (bytes == null) {
                return null;
            }
            return decodeStatValue(column.type(), bytes);
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        private static int compare(Comparable left, Comparable right) {
            return left.compareTo(right);
        }

        private static Comparable<?> decodeStatValue(Type type, byte[] bytes) {
            ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
            return switch (type) {
                case BOOLEAN -> bytes.length == 0 ? Boolean.FALSE : bytes[0] != 0;
                case INT32 -> bb.getInt();
                case INT64 -> bb.getLong();
                case FLOAT -> bb.getFloat();
                case DOUBLE -> bb.getDouble();
                case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> Binary.fromConstantByteArray(bytes);
            };
        }
    }

    private final class DictionaryPruningVisitor implements FilterPredicate.Visitor<Boolean> {
        private final Map<String, CoreColumnChunkMeta> columns;

        DictionaryPruningVisitor(Map<String, CoreColumnChunkMeta> columns) {
            this.columns = columns;
        }

        @Override
        public <T extends Comparable<T>> Boolean visit(Operators.Eq<T> eq) {
            CoreColumnChunkMeta column = column(eq.getColumn());
            if (column == null) {
                return false;
            }
            T value = eq.getValue();
            if (value == null) {
                Long nullCount = nullCount(column);
                return nullCount != null && nullCount == 0L;
            }
            Set<Object> dictionary = dictionaryValues(column);
            if (dictionary.isEmpty()) {
                return false;
            }
            return !dictionary.contains(value);
        }

        @Override
        public <T extends Comparable<T>> Boolean visit(Operators.NotEq<T> notEq) {
            CoreColumnChunkMeta column = column(notEq.getColumn());
            if (column == null || notEq.getValue() == null) {
                return false;
            }
            Set<Object> dictionary = dictionaryValues(column);
            Long nullCount = nullCount(column);
            if (dictionary.size() == 1 && nullCount != null && nullCount == 0L) {
                return dictionary.contains(notEq.getValue());
            }
            return false;
        }

        @Override
        public <T extends Comparable<T>> Boolean visit(Operators.Lt<T> lt) {
            return false;
        }

        @Override
        public <T extends Comparable<T>> Boolean visit(Operators.LtEq<T> ltEq) {
            return false;
        }

        @Override
        public <T extends Comparable<T>> Boolean visit(Operators.Gt<T> gt) {
            return false;
        }

        @Override
        public <T extends Comparable<T>> Boolean visit(Operators.GtEq<T> gtEq) {
            return false;
        }

        @Override
        public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Boolean visit(
                Operators.UserDefined<T, U> udp) {
            return false;
        }

        @Override
        public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Boolean visit(
                Operators.LogicalNotUserDefined<T, U> udp) {
            return false;
        }

        @Override
        public Boolean visit(Operators.And and) {
            return and.getLeft().accept(this) || and.getRight().accept(this);
        }

        @Override
        public Boolean visit(Operators.Or or) {
            return andConservative(or);
        }

        private Boolean andConservative(Operators.Or or) {
            return or.getLeft().accept(this) && or.getRight().accept(this);
        }

        @Override
        public Boolean visit(Operators.Not not) {
            return false;
        }

        private CoreColumnChunkMeta column(Operators.Column<?> column) {
            return columns.get(column.getColumnPath().toDotString());
        }

        private Long nullCount(CoreColumnChunkMeta column) {
            org.apache.parquet.format.Statistics s = column.statistics();
            return s != null && s.isSetNull_count() ? s.getNull_count() : null;
        }

        private Set<Object> dictionaryValues(CoreColumnChunkMeta column) {
            try {
                return decodeDictionaryValues(column);
            } catch (IOException | RuntimeException e) {
                return Set.of();
            }
        }
    }

    private record CoreColumnChunk(long valueCount, List<DataPage> pages, DictionaryPage dictionaryPage) {}

    private static final class CorePageReader implements PageReader {
        private final long totalValueCount;
        private final Queue<DataPage> pages;
        private final DictionaryPage dictionaryPage;

        CorePageReader(long totalValueCount, List<DataPage> pages, DictionaryPage dictionaryPage) {
            this.totalValueCount = totalValueCount;
            this.pages = new ArrayDeque<>(pages);
            this.dictionaryPage = dictionaryPage;
        }

        @Override
        public DictionaryPage readDictionaryPage() {
            return dictionaryPage;
        }

        @Override
        public long getTotalValueCount() {
            return totalValueCount;
        }

        @Override
        public DataPage readPage() {
            return pages.poll();
        }
    }

    private static final class CorePageReadStore implements PageReadStore {
        private final long rowCount;
        private final Map<ColumnDescriptor, PageReader> readers;

        CorePageReadStore(long rowCount, Map<ColumnDescriptor, PageReader> readers) {
            this.rowCount = rowCount;
            this.readers = readers;
        }

        @Override
        public PageReader getPageReader(ColumnDescriptor descriptor) {
            return readers.get(descriptor);
        }

        @Override
        public long getRowCount() {
            return rowCount;
        }
    }
}
