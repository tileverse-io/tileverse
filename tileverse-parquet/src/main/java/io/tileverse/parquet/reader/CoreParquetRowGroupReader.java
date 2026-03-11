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
import java.util.Optional;
import java.util.PrimitiveIterator;
import java.util.Queue;
import java.util.Set;
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
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Type;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexFilter;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

/**
 * Reads and decompresses column chunks from a Parquet file, implementing four-tier filter pushdown:
 *
 * <ol>
 *   <li><strong>Statistics-based pruning</strong> — skips entire row groups whose column min/max
 *       statistics prove no records can match the filter.</li>
 *   <li><strong>Dictionary-based pruning</strong> — loads dictionary pages and eliminates row groups
 *       where the filter value is absent from the dictionary.</li>
 *   <li><strong>Column-index pushdown (page-level)</strong> — reads page-level {@code ColumnIndex}
 *       and {@code OffsetIndex} structures to compute matching {@code RowRanges}, then reads only
 *       the pages that may contain matching records.</li>
 *   <li><strong>Record-level filtering</strong> — applies the filter predicate during record
 *       assembly, skipping individual non-matching records.</li>
 * </ol>
 *
 * <p>Each {@link #readRowGroup} call opens a single {@code SeekableInputStream} that is reused
 * across all column chunks in that row group. Dictionary pages read during the pruning phase are
 * cached and reused during the column-read phase, avoiding redundant I/O.
 */
public final class CoreParquetRowGroupReader implements ParquetRowGroupReader {
    private final InputFile inputFile;
    private final CoreParquetReadOptions options;
    private final List<CoreRowGroupMeta> rowGroups;
    private final MessageType fileSchema;
    private final FilterPredicate filterPredicate;
    private final Map<String, DictionaryPage> dictionaryCache = new HashMap<>();
    private MessageType requestedSchema;
    private int nextRowGroupIndex;

    /**
     * Creates a row group reader for the given Parquet file.
     *
     * @param inputFile  the Parquet file to read from
     * @param footer     the parsed file footer
     * @param options    read options controlling filter pushdown behavior
     * @param fileSchema the full file schema (used for column descriptors)
     */
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
            dictionaryCache.clear();
            CoreRowGroupMeta rowGroup = rowGroups.get(nextRowGroupIndex++);
            if (canDropByStats(rowGroup)) {
                continue;
            }
            if (canDropByDictionary(rowGroup)) {
                continue;
            }
            if (filterPredicate != null && options.useColumnIndexFilter()) {
                Set<ColumnPath> paths = getRequestedColumnPaths();
                ColumnIndexStore columnIndexStore = CoreColumnIndexStore.create(inputFile, rowGroup, fileSchema, paths);
                RowRanges rowRanges = ColumnIndexFilter.calculateRowRanges(
                        options.getRecordFilter(), columnIndexStore, paths, rowGroup.rowCount());
                if (rowRanges.rowCount() == 0) {
                    continue;
                }
                if (rowRanges.rowCount() < rowGroup.rowCount()) {
                    return readFilteredRowGroup(rowGroup, rowRanges, columnIndexStore);
                }
            }
            return readRowGroup(rowGroup);
        }
        return null;
    }

    @Override
    public void close() {}

    // ---- Row group reading ----

    private PageReadStore readRowGroup(CoreRowGroupMeta rowGroup) throws IOException {
        Map<ColumnDescriptor, PageReader> pageReaders = new HashMap<>();
        Map<String, CoreColumnChunkMeta> columnByPath = buildColumnByPathMap(rowGroup);

        try (SeekableInputStream in = inputFile.newStream()) {
            for (ColumnDescriptor descriptor : requestedSchema.getColumns()) {
                String path = String.join(".", descriptor.getPath());
                CoreColumnChunkMeta columnMeta = columnByPath.get(path);
                if (columnMeta == null) {
                    continue;
                }
                CoreColumnChunk chunk = readColumnChunk(in, columnMeta, descriptor);
                pageReaders.put(descriptor, new CorePageReader(chunk.valueCount, chunk.pages, chunk.dictionaryPage));
            }
        }

        return new CorePageReadStore(rowGroup.rowCount(), pageReaders);
    }

    private PageReadStore readFilteredRowGroup(
            CoreRowGroupMeta rowGroup, RowRanges rowRanges, ColumnIndexStore columnIndexStore) throws IOException {
        Map<ColumnDescriptor, PageReader> pageReaders = new HashMap<>();
        Map<String, CoreColumnChunkMeta> columnByPath = buildColumnByPathMap(rowGroup);

        try (SeekableInputStream in = inputFile.newStream()) {
            for (ColumnDescriptor descriptor : requestedSchema.getColumns()) {
                String path = String.join(".", descriptor.getPath());
                CoreColumnChunkMeta columnMeta = columnByPath.get(path);
                if (columnMeta == null) {
                    continue;
                }

                ColumnPath columnPath = ColumnPath.get(descriptor.getPath());
                OffsetIndex offsetIndex;
                try {
                    offsetIndex = columnIndexStore.getOffsetIndex(columnPath);
                } catch (ColumnIndexStore.MissingOffsetIndexException e) {
                    CoreColumnChunk chunk = readColumnChunk(in, columnMeta, descriptor);
                    pageReaders.put(
                            descriptor, new CorePageReader(chunk.valueCount, chunk.pages, chunk.dictionaryPage));
                    continue;
                }

                OffsetIndex filteredOffsetIndex = filterOffsetIndex(offsetIndex, rowRanges, rowGroup.rowCount());
                if (filteredOffsetIndex.getPageCount() == 0) {
                    continue;
                }

                CoreColumnChunk chunk = readFilteredColumnChunk(in, columnMeta, descriptor, filteredOffsetIndex);
                pageReaders.put(descriptor, new CorePageReader(chunk.valueCount, chunk.pages, chunk.dictionaryPage));
            }
        }

        return new CorePageReadStore(rowRanges, pageReaders);
    }

    // ---- Column chunk reading ----

    CoreColumnChunk readColumnChunk(SeekableInputStream in, CoreColumnChunkMeta columnMeta, ColumnDescriptor descriptor)
            throws IOException {
        long start = columnMeta.startOffset();
        long totalSize = columnMeta.totalCompressedSize();
        if (totalSize < 0 || totalSize > Integer.MAX_VALUE) {
            throw new IOException("Unsupported column chunk size: " + totalSize);
        }

        byte[] chunkBytes = readRange(in, start, (int) totalSize);
        PrimitiveType primitiveType = descriptor.getPrimitiveType();

        DictionaryPage dictionaryPage = dictionaryCache.get(columnMeta.path());
        List<DataPage> dataPages = new ArrayList<>();
        ByteArrayInputStream bis = new ByteArrayInputStream(chunkBytes);

        while (bis.available() > 0) {
            PageHeader header = Util.readPageHeader(bis);
            int payloadSize = header.getCompressed_page_size();
            byte[] payload = bis.readNBytes(payloadSize);
            if (payload.length != payloadSize) {
                throw new IOException("Unexpected EOF while reading page payload");
            }

            if (header.getType() == PageType.DICTIONARY_PAGE) {
                if (dictionaryPage == null) {
                    DictionaryPageHeader dictionaryHeader = header.getDictionary_page_header();
                    byte[] decompressed =
                            CompressionUtil.decompress(columnMeta.codec(), payload, header.getUncompressed_page_size());
                    dictionaryPage = new DictionaryPage(
                            org.apache.parquet.bytes.BytesInput.from(decompressed),
                            dictionaryHeader.getNum_values(),
                            toEncoding(dictionaryHeader.getEncoding()));
                }
                continue;
            }

            if (header.getType() == PageType.DATA_PAGE) {
                dataPages.add(parseDataPageV1(header, payload, columnMeta, primitiveType));
                continue;
            }

            if (header.getType() == PageType.DATA_PAGE_V2) {
                dataPages.add(parseDataPageV2(header, payload, columnMeta, primitiveType));
            }
        }

        return new CoreColumnChunk(columnMeta.valueCount(), dataPages, dictionaryPage);
    }

    private CoreColumnChunk readFilteredColumnChunk(
            SeekableInputStream in,
            CoreColumnChunkMeta columnMeta,
            ColumnDescriptor descriptor,
            OffsetIndex filteredOffsetIndex)
            throws IOException {
        PrimitiveType primitiveType = descriptor.getPrimitiveType();

        DictionaryPage dictionaryPage = dictionaryCache.get(columnMeta.path());
        if (dictionaryPage == null && columnMeta.hasDictionaryPage()) {
            dictionaryPage = readDictionaryPage(columnMeta);
        }

        List<DataPage> dataPages = new ArrayList<>();
        long valueCount = 0;

        for (int i = 0; i < filteredOffsetIndex.getPageCount(); i++) {
            long pageOffset = filteredOffsetIndex.getOffset(i);
            int pageSize = filteredOffsetIndex.getCompressedPageSize(i);

            byte[] pageBytes = readRange(in, pageOffset, pageSize);
            ByteArrayInputStream pageStream = new ByteArrayInputStream(pageBytes);
            PageHeader header = Util.readPageHeader(pageStream);

            int payloadSize = header.getCompressed_page_size();
            byte[] payload = pageStream.readNBytes(payloadSize);
            if (payload.length != payloadSize) {
                throw new IOException("Unexpected EOF while reading page payload");
            }

            if (header.getType() == PageType.DATA_PAGE) {
                DataPage page = parseDataPageV1(header, payload, columnMeta, primitiveType);
                dataPages.add(page);
                valueCount += page.getValueCount();
            } else if (header.getType() == PageType.DATA_PAGE_V2) {
                DataPage page = parseDataPageV2(header, payload, columnMeta, primitiveType);
                dataPages.add(page);
                valueCount += page.getValueCount();
            }
        }

        return new CoreColumnChunk(valueCount, dataPages, dictionaryPage);
    }

    // ---- Page parsing ----

    private static DataPageV1 parseDataPageV1(
            PageHeader header, byte[] payload, CoreColumnChunkMeta columnMeta, PrimitiveType primitiveType)
            throws IOException {
        DataPageHeader dataHeader = header.getData_page_header();
        byte[] decompressed =
                CompressionUtil.decompress(columnMeta.codec(), payload, header.getUncompressed_page_size());
        Statistics<?> stats = Statistics.noopStats(primitiveType);
        return new DataPageV1(
                org.apache.parquet.bytes.BytesInput.from(decompressed),
                dataHeader.getNum_values(),
                header.getUncompressed_page_size(),
                stats,
                toEncoding(dataHeader.getRepetition_level_encoding()),
                toEncoding(dataHeader.getDefinition_level_encoding()),
                toEncoding(dataHeader.getEncoding()));
    }

    private static DataPageV2 parseDataPageV2(
            PageHeader header, byte[] payload, CoreColumnChunkMeta columnMeta, PrimitiveType primitiveType)
            throws IOException {
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
                ? CompressionUtil.decompress(
                        columnMeta.codec(), dataPayload, header.getUncompressed_page_size() - repLen - defLen)
                : dataPayload;

        Statistics<?> stats = Statistics.noopStats(primitiveType);
        return DataPageV2.uncompressed(
                dataHeader.getNum_rows(),
                dataHeader.getNum_nulls(),
                dataHeader.getNum_values(),
                repetitionLevels,
                definitionLevels,
                toEncoding(dataHeader.getEncoding()),
                org.apache.parquet.bytes.BytesInput.from(decodedData),
                stats);
    }

    private static Encoding toEncoding(org.apache.parquet.format.Encoding encoding) {
        return Encoding.valueOf(encoding.name());
    }

    // ---- Row group pruning ----

    private boolean canDropByStats(CoreRowGroupMeta rowGroup) {
        if (filterPredicate == null || !options.useStatsFilter()) {
            return false;
        }
        Map<String, CoreColumnChunkMeta> byPath = buildColumnByPathMap(rowGroup);
        return filterPredicate.accept(new StatsPruningVisitor(byPath));
    }

    boolean canDropByDictionary(CoreRowGroupMeta rowGroup) {
        if (filterPredicate == null || !options.useDictionaryFilter()) {
            return false;
        }
        Map<String, CoreColumnChunkMeta> byPath = buildColumnByPathMap(rowGroup);
        try {
            return filterPredicate.accept(new DictionaryPruningVisitor(byPath));
        } catch (RuntimeException e) {
            return false;
        }
    }

    // ---- Dictionary reading ----

    private DictionaryPage readDictionaryPage(CoreColumnChunkMeta columnMeta) throws IOException {
        DictionaryPage cached = dictionaryCache.get(columnMeta.path());
        if (cached != null) {
            return cached;
        }
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
        byte[] decompressed =
                CompressionUtil.decompress(columnMeta.codec(), payload, header.getUncompressed_page_size());
        DictionaryPage result = new DictionaryPage(
                org.apache.parquet.bytes.BytesInput.from(decompressed),
                dictionaryHeader.getNum_values(),
                toEncoding(dictionaryHeader.getEncoding()));
        dictionaryCache.put(columnMeta.path(), result);
        return result;
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

    // ---- Column-index filtering ----

    private Set<ColumnPath> getRequestedColumnPaths() {
        Set<ColumnPath> paths = new HashSet<>();
        for (ColumnDescriptor descriptor : requestedSchema.getColumns()) {
            paths.add(ColumnPath.get(descriptor.getPath()));
        }
        return paths;
    }

    private static OffsetIndex filterOffsetIndex(OffsetIndex offsetIndex, RowRanges rowRanges, long totalRowCount) {
        List<Integer> indexMap = new ArrayList<>();
        for (int i = 0; i < offsetIndex.getPageCount(); i++) {
            long from = offsetIndex.getFirstRowIndex(i);
            long to = offsetIndex.getLastRowIndex(i, totalRowCount);
            if (rowRanges.isOverlapping(from, to)) {
                indexMap.add(i);
            }
        }
        return new FilteredOffsetIndex(
                offsetIndex, indexMap.stream().mapToInt(Integer::intValue).toArray());
    }

    // ---- I/O ----

    private byte[] readRange(long offset, int length) throws IOException {
        byte[] bytes = new byte[length];
        try (SeekableInputStream in = inputFile.newStream()) {
            in.seek(offset);
            in.readFully(bytes);
        }
        return bytes;
    }

    private static byte[] readRange(SeekableInputStream in, long offset, int length) throws IOException {
        byte[] bytes = new byte[length];
        in.seek(offset);
        in.readFully(bytes);
        return bytes;
    }

    // ---- Helpers ----

    private static Map<String, CoreColumnChunkMeta> buildColumnByPathMap(CoreRowGroupMeta rowGroup) {
        Map<String, CoreColumnChunkMeta> byPath = new HashMap<>();
        for (CoreColumnChunkMeta column : rowGroup.columns()) {
            byPath.put(column.path(), column);
        }
        return byPath;
    }

    // ---- Inner classes ----

    private static final class FilteredOffsetIndex implements OffsetIndex {
        private final OffsetIndex delegate;
        private final int[] indexMap;

        FilteredOffsetIndex(OffsetIndex delegate, int[] indexMap) {
            this.delegate = delegate;
            this.indexMap = indexMap;
        }

        @Override
        public int getPageCount() {
            return indexMap.length;
        }

        @Override
        public long getOffset(int pageIndex) {
            return delegate.getOffset(indexMap[pageIndex]);
        }

        @Override
        public int getCompressedPageSize(int pageIndex) {
            return delegate.getCompressedPageSize(indexMap[pageIndex]);
        }

        @Override
        public long getFirstRowIndex(int pageIndex) {
            return delegate.getFirstRowIndex(indexMap[pageIndex]);
        }

        @Override
        public int getPageOrdinal(int pageIndex) {
            return indexMap[pageIndex];
        }

        @Override
        public long getLastRowIndex(int pageIndex, long totalRowCount) {
            int nextIndex = indexMap[pageIndex] + 1;
            return (nextIndex >= delegate.getPageCount() ? totalRowCount : delegate.getFirstRowIndex(nextIndex)) - 1;
        }
    }

    /** Visitor that evaluates whether a row group can be dropped based on column statistics. */
    static final class StatsPruningVisitor implements FilterPredicate.Visitor<Boolean> {
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
        private final RowRanges rowRanges;

        CorePageReadStore(long rowCount, Map<ColumnDescriptor, PageReader> readers) {
            this.rowCount = rowCount;
            this.readers = readers;
            this.rowRanges = null;
        }

        CorePageReadStore(RowRanges rowRanges, Map<ColumnDescriptor, PageReader> readers) {
            this.rowCount = rowRanges.rowCount();
            this.readers = readers;
            this.rowRanges = rowRanges;
        }

        @Override
        public PageReader getPageReader(ColumnDescriptor descriptor) {
            return readers.get(descriptor);
        }

        @Override
        public long getRowCount() {
            return rowCount;
        }

        @Override
        public Optional<PrimitiveIterator.OfLong> getRowIndexes() {
            return rowRanges == null ? Optional.empty() : Optional.of(rowRanges.iterator());
        }
    }
}
