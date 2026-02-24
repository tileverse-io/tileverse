/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 */
package io.tileverse.parquet.reader;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.EdgeInterpolationAlgorithm;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CoreParquetVisitorsTest {

    @TempDir
    Path tempDir;

    @Test
    void footerReader_privateEnumMappings_coverAllConvertedTypesAndUnits() throws Exception {
        Method fromConverted =
                CoreParquetFooterReader.class.getDeclaredMethod("fromConvertedType", SchemaElement.class);
        fromConverted.setAccessible(true);

        for (ConvertedType type : ConvertedType.values()) {
            SchemaElement se = new SchemaElement("f");
            se.setConverted_type(type);
            if (type == ConvertedType.DECIMAL) {
                se.setScale(2);
                se.setPrecision(10);
            }
            Object result = fromConverted.invoke(null, se);
            if (type == ConvertedType.MAP_KEY_VALUE) {
                assertThat(result).isNull();
            } else {
                assertThat(result).isNotNull();
            }
        }

        Method fromTimeUnit = CoreParquetFooterReader.class.getDeclaredMethod("fromTimeUnit", TimeUnit.class);
        fromTimeUnit.setAccessible(true);
        assertThat(fromTimeUnit.invoke(null, new Object[] {null}).toString()).isEqualTo("MILLIS");

        TimeUnit millis = new TimeUnit();
        millis.setMILLIS(new org.apache.parquet.format.MilliSeconds());
        assertThat(fromTimeUnit.invoke(null, millis).toString()).isEqualTo("MILLIS");

        TimeUnit micros = new TimeUnit();
        micros.setMICROS(new org.apache.parquet.format.MicroSeconds());
        assertThat(fromTimeUnit.invoke(null, micros).toString()).isEqualTo("MICROS");

        TimeUnit nanos = new TimeUnit();
        nanos.setNANOS(new org.apache.parquet.format.NanoSeconds());
        assertThat(fromTimeUnit.invoke(null, nanos).toString()).isEqualTo("NANOS");

        Method fromEdge = CoreParquetFooterReader.class.getDeclaredMethod(
                "fromEdgeInterpolation", org.apache.parquet.format.EdgeInterpolationAlgorithm.class);
        fromEdge.setAccessible(true);
        for (EdgeInterpolationAlgorithm alg : EdgeInterpolationAlgorithm.values()) {
            assertThat(fromEdge.invoke(null, alg)).isNotNull();
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    void statsPruningVisitor_handlesComparisonAndLogicalOperators() throws Exception {
        Class<?> visitorClass =
                Class.forName("io.tileverse.parquet.reader.CoreParquetRowGroupReader$StatsPruningVisitor");
        Constructor<?> ctor = visitorClass.getDeclaredConstructor(Map.class);
        ctor.setAccessible(true);

        CoreColumnChunkMeta id = new CoreColumnChunkMeta(
                "id",
                java.util.List.of("id"),
                org.apache.parquet.format.Type.INT32,
                org.apache.parquet.format.CompressionCodec.UNCOMPRESSED,
                100,
                100,
                0,
                0,
                -1,
                intStats(10, 20, 0),
                null,
                null,
                null,
                null);

        FilterPredicate.Visitor<Boolean> visitor =
                (FilterPredicate.Visitor<Boolean>) ctor.newInstance(Map.of("id", id));

        assertThat(FilterApi.eq(FilterApi.intColumn("id"), 5).accept(visitor)).isTrue();
        assertThat(FilterApi.eq(FilterApi.intColumn("id"), 15).accept(visitor)).isFalse();
        assertThat(FilterApi.notEq(FilterApi.intColumn("id"), 15).accept(visitor))
                .isFalse();
        assertThat(FilterApi.lt(FilterApi.intColumn("id"), 5).accept(visitor)).isTrue();
        assertThat(FilterApi.ltEq(FilterApi.intColumn("id"), 9).accept(visitor)).isTrue();
        assertThat(FilterApi.gt(FilterApi.intColumn("id"), 25).accept(visitor)).isTrue();
        assertThat(FilterApi.gtEq(FilterApi.intColumn("id"), 21).accept(visitor))
                .isTrue();

        FilterPredicate andFilter =
                FilterApi.and(FilterApi.eq(FilterApi.intColumn("id"), 15), FilterApi.gt(FilterApi.intColumn("id"), 25));
        assertThat(andFilter.accept(visitor)).isTrue();

        FilterPredicate orFilter =
                FilterApi.or(FilterApi.eq(FilterApi.intColumn("id"), 5), FilterApi.eq(FilterApi.intColumn("id"), 15));
        assertThat(orFilter.accept(visitor)).isFalse();

        assertThat(FilterApi.not(FilterApi.eq(FilterApi.intColumn("id"), 5)).accept(visitor))
                .isFalse();

        CoreColumnChunkMeta onlyNulls = new CoreColumnChunkMeta(
                "id",
                java.util.List.of("id"),
                org.apache.parquet.format.Type.INT32,
                org.apache.parquet.format.CompressionCodec.UNCOMPRESSED,
                100,
                100,
                0,
                0,
                -1,
                nullOnlyStats(100),
                null,
                null,
                null,
                null);
        FilterPredicate.Visitor<Boolean> nullVisitor =
                (FilterPredicate.Visitor<Boolean>) ctor.newInstance(Map.of("id", onlyNulls));
        assertThat(FilterApi.eq(FilterApi.intColumn("id"), 5).accept(nullVisitor))
                .isFalse();
        assertThat(FilterApi.eq(FilterApi.intColumn("id"), null).accept(nullVisitor))
                .isFalse();

        CoreColumnChunkMeta constant = new CoreColumnChunkMeta(
                "id",
                java.util.List.of("id"),
                org.apache.parquet.format.Type.INT32,
                org.apache.parquet.format.CompressionCodec.UNCOMPRESSED,
                100,
                100,
                0,
                0,
                -1,
                intStats(10, 10, 0),
                null,
                null,
                null,
                null);
        FilterPredicate.Visitor<Boolean> constantVisitor =
                (FilterPredicate.Visitor<Boolean>) ctor.newInstance(Map.of("id", constant));
        assertThat(FilterApi.notEq(FilterApi.intColumn("id"), 10).accept(constantVisitor))
                .isTrue();
        assertThat(FilterApi.eq(FilterApi.intColumn("id"), null).accept(constantVisitor))
                .isTrue();
    }

    @SuppressWarnings("unchecked")
    @Test
    void statsPruningVisitor_coversTypeDecodingAndNullBranches() throws Exception {
        Class<?> visitorClass =
                Class.forName("io.tileverse.parquet.reader.CoreParquetRowGroupReader$StatsPruningVisitor");
        Constructor<?> ctor = visitorClass.getDeclaredConstructor(Map.class);
        ctor.setAccessible(true);

        CoreColumnChunkMeta boolCol = new CoreColumnChunkMeta(
                "b",
                java.util.List.of("b"),
                org.apache.parquet.format.Type.BOOLEAN,
                CompressionCodec.UNCOMPRESSED,
                10,
                10,
                0,
                0,
                -1,
                stats(new byte[] {0}, new byte[] {0}, 0),
                null,
                null,
                null,
                null);
        CoreColumnChunkMeta longCol = new CoreColumnChunkMeta(
                "l",
                java.util.List.of("l"),
                org.apache.parquet.format.Type.INT64,
                CompressionCodec.UNCOMPRESSED,
                10,
                10,
                0,
                0,
                -1,
                stats(longToBytes(100), longToBytes(200), 0),
                null,
                null,
                null,
                null);
        CoreColumnChunkMeta floatCol = new CoreColumnChunkMeta(
                "f",
                java.util.List.of("f"),
                org.apache.parquet.format.Type.FLOAT,
                CompressionCodec.UNCOMPRESSED,
                10,
                10,
                0,
                0,
                -1,
                stats(floatToBytes(1.0f), floatToBytes(2.0f), 0),
                null,
                null,
                null,
                null);
        CoreColumnChunkMeta doubleCol = new CoreColumnChunkMeta(
                "d",
                java.util.List.of("d"),
                org.apache.parquet.format.Type.DOUBLE,
                CompressionCodec.UNCOMPRESSED,
                10,
                10,
                0,
                0,
                -1,
                stats(doubleToBytes(10.0d), doubleToBytes(20.0d), 0),
                null,
                null,
                null,
                null);
        CoreColumnChunkMeta binCol = new CoreColumnChunkMeta(
                "s",
                java.util.List.of("s"),
                org.apache.parquet.format.Type.BYTE_ARRAY,
                CompressionCodec.UNCOMPRESSED,
                10,
                10,
                0,
                0,
                -1,
                stats(
                        "a".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                        "c".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                        0),
                null,
                null,
                null,
                null);
        CoreColumnChunkMeta withNoStats = new CoreColumnChunkMeta(
                "x",
                java.util.List.of("x"),
                org.apache.parquet.format.Type.INT32,
                CompressionCodec.UNCOMPRESSED,
                10,
                10,
                0,
                0,
                -1,
                null,
                null,
                null,
                null,
                null);

        FilterPredicate.Visitor<Boolean> visitor = (FilterPredicate.Visitor<Boolean>) ctor.newInstance(Map.of(
                "b", boolCol,
                "l", longCol,
                "f", floatCol,
                "d", doubleCol,
                "s", binCol,
                "x", withNoStats));

        assertThat(FilterApi.eq(FilterApi.booleanColumn("b"), true).accept(visitor))
                .isTrue();
        assertThat(FilterApi.lt(FilterApi.longColumn("l"), 50L).accept(visitor)).isTrue();
        assertThat(FilterApi.gt(FilterApi.floatColumn("f"), 3.0f).accept(visitor))
                .isTrue();
        assertThat(FilterApi.gtEq(FilterApi.doubleColumn("d"), 21.0d).accept(visitor))
                .isTrue();
        assertThat(FilterApi.eq(FilterApi.binaryColumn("s"), org.apache.parquet.io.api.Binary.fromString("z"))
                        .accept(visitor))
                .isTrue();

        assertThat(FilterApi.eq(FilterApi.intColumn("missing"), 1).accept(visitor))
                .isFalse();
        assertThat(FilterApi.eq(FilterApi.intColumn("x"), 1).accept(visitor)).isFalse();
        assertThat(FilterApi.lt(FilterApi.intColumn("x"), 1).accept(visitor)).isFalse();
        assertThat(FilterApi.gt(FilterApi.intColumn("x"), 1).accept(visitor)).isFalse();
    }

    private static org.apache.parquet.format.Statistics intStats(int min, int max, long nullCount) {
        org.apache.parquet.format.Statistics s = new org.apache.parquet.format.Statistics();
        s.setMin(intToBytes(min));
        s.setMax(intToBytes(max));
        s.setNull_count(nullCount);
        return s;
    }

    private static org.apache.parquet.format.Statistics nullOnlyStats(long nullCount) {
        org.apache.parquet.format.Statistics s = new org.apache.parquet.format.Statistics();
        s.setNull_count(nullCount);
        return s;
    }

    private static byte[] intToBytes(int v) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(v).array();
    }

    private static org.apache.parquet.format.Statistics stats(byte[] min, byte[] max, long nullCount) {
        org.apache.parquet.format.Statistics s = new org.apache.parquet.format.Statistics();
        s.setMin(min);
        s.setMax(max);
        s.setNull_count(nullCount);
        return s;
    }

    private static byte[] longToBytes(long v) {
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(v).array();
    }

    private static byte[] floatToBytes(float v) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putFloat(v).array();
    }

    private static byte[] doubleToBytes(double v) {
        return ByteBuffer.allocate(8)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putDouble(v)
                .array();
    }

    @Test
    void dictionaryPruningVisitor_handlesDefensivePaths() throws Exception {
        CoreColumnChunkMeta idNoDictionary = new CoreColumnChunkMeta(
                "id",
                java.util.List.of("id"),
                org.apache.parquet.format.Type.INT32,
                CompressionCodec.UNCOMPRESSED,
                10,
                0,
                0,
                0,
                -1,
                intStats(10, 20, 0),
                null,
                null,
                null,
                null);
        CoreColumnChunkMeta idWithNulls = new CoreColumnChunkMeta(
                "id",
                java.util.List.of("id"),
                org.apache.parquet.format.Type.INT32,
                CompressionCodec.UNCOMPRESSED,
                10,
                0,
                0,
                0,
                -1,
                intStats(10, 20, 3),
                null,
                null,
                null,
                null);

        assertThat(canDropByDictionary(FilterApi.eq(FilterApi.intColumn("missing"), 1), idNoDictionary))
                .isFalse();
        assertThat(canDropByDictionary(FilterApi.eq(FilterApi.intColumn("id"), null), idNoDictionary))
                .isTrue();
        assertThat(canDropByDictionary(FilterApi.eq(FilterApi.intColumn("id"), null), idWithNulls))
                .isFalse();
        assertThat(canDropByDictionary(FilterApi.eq(FilterApi.intColumn("id"), 15), idNoDictionary))
                .isFalse();

        assertThat(canDropByDictionary(FilterApi.notEq(FilterApi.intColumn("id"), null), idNoDictionary))
                .isFalse();
        assertThat(canDropByDictionary(FilterApi.lt(FilterApi.intColumn("id"), 15), idNoDictionary))
                .isFalse();
        assertThat(canDropByDictionary(FilterApi.ltEq(FilterApi.intColumn("id"), 15), idNoDictionary))
                .isFalse();
        assertThat(canDropByDictionary(FilterApi.gt(FilterApi.intColumn("id"), 15), idNoDictionary))
                .isFalse();
        assertThat(canDropByDictionary(FilterApi.gtEq(FilterApi.intColumn("id"), 15), idNoDictionary))
                .isFalse();

        assertThat(canDropByDictionary(
                        FilterApi.and(
                                FilterApi.eq(FilterApi.intColumn("id"), 1), FilterApi.eq(FilterApi.intColumn("id"), 2)),
                        idNoDictionary))
                .isFalse();
        assertThat(canDropByDictionary(
                        FilterApi.or(
                                FilterApi.eq(FilterApi.intColumn("id"), 1), FilterApi.eq(FilterApi.intColumn("id"), 2)),
                        idNoDictionary))
                .isFalse();
        assertThat(canDropByDictionary(FilterApi.not(FilterApi.eq(FilterApi.intColumn("id"), 1)), idNoDictionary))
                .isFalse();
    }

    private boolean canDropByDictionary(FilterPredicate predicate, CoreColumnChunkMeta columnMeta) throws Exception {
        Path fakeFile = tempDir.resolve("empty.parquet");
        if (!Files.exists(fakeFile)) {
            Files.write(fakeFile, new byte[] {0});
        }
        org.apache.parquet.schema.MessageType schema =
                org.apache.parquet.schema.MessageTypeParser.parseMessageType("message t { required int32 id; }");
        CoreParquetFooter footer = new CoreParquetFooter(schema, Map.of(), 10, java.util.List.of());
        CoreParquetReadOptions options = CoreParquetReadOptions.builder()
                .useStatsFilter(false)
                .useDictionaryFilter(true)
                .withRecordFilter(FilterCompat.get(predicate))
                .build();
        CoreParquetRowGroupReader reader = new CoreParquetRowGroupReader(
                new org.apache.parquet.io.LocalInputFile(fakeFile), footer, options, schema);
        Method method =
                CoreParquetRowGroupReader.class.getDeclaredMethod("canDropByDictionary", CoreRowGroupMeta.class);
        method.setAccessible(true);
        return (Boolean) method.invoke(reader, new CoreRowGroupMeta(10, java.util.List.of(columnMeta)));
    }
}
