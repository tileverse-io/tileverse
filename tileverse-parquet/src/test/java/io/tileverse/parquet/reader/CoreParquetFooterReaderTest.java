/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 */
package io.tileverse.parquet.reader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.EdgeInterpolationAlgorithm;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.LogicalType;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.TimeUnit;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CoreParquetFooterReaderTest {

    @TempDir
    Path tempDir;

    @Test
    void read_rejectsTooSmallFile() throws IOException {
        Path file = tempDir.resolve("tiny.parquet");
        Files.write(file, new byte[] {1, 2, 3});

        assertThatThrownBy(() -> CoreParquetFooterReader.read(new LocalInputFile(file)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("too small");
    }

    @Test
    void read_rejectsMissingMagic() throws IOException {
        Path file = tempDir.resolve("bad-magic.parquet");
        byte[] bytes = new byte[12];
        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).putInt(4);
        System.arraycopy("NOPE".getBytes(StandardCharsets.US_ASCII), 0, bytes, 4, 4);
        Files.write(file, bytes);

        assertThatThrownBy(() -> CoreParquetFooterReader.read(new LocalInputFile(file)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("missing PAR1");
    }

    @Test
    void read_rejectsInvalidFooterLength() throws IOException {
        Path file = tempDir.resolve("bad-footer-len.parquet");
        byte[] bytes = new byte[16];
        ByteBuffer.wrap(bytes, 8, 8).order(ByteOrder.LITTLE_ENDIAN).putInt(10_000);
        System.arraycopy("PAR1".getBytes(StandardCharsets.US_ASCII), 0, bytes, 12, 4);
        Files.write(file, bytes);

        assertThatThrownBy(() -> CoreParquetFooterReader.read(new LocalInputFile(file)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("bad footer length");
    }

    @Test
    void read_parsesValidFooterAndRowGroups() throws Exception {
        java.net.URL url =
                CoreParquetFooterReaderTest.class.getClassLoader().getResource("geoparquet/sample-geoparquet.parquet");
        Path file = Path.of(java.util.Objects.requireNonNull(url, "fixture").toURI());

        CoreParquetFooter footer = CoreParquetFooterReader.read(new LocalInputFile(file));

        assertThat(footer.schema().getFieldCount()).isGreaterThanOrEqualTo(3);
        assertThat(footer.keyValueMetadata()).containsKey("geo");
        assertThat(footer.recordCount()).isEqualTo(3);
        assertThat(footer.rowGroups()).isNotEmpty();
        assertThat(footer.rowGroups().get(0).columns()).isNotEmpty();
    }

    @Test
    void privateLogicalConversionMethods_coverBranches() throws Exception {
        Method fromLogicalType = CoreParquetFooterReader.class.getDeclaredMethod("fromLogicalType", LogicalType.class);
        fromLogicalType.setAccessible(true);

        LogicalType stringLt = new LogicalType();
        stringLt.setSTRING(new org.apache.parquet.format.StringType());
        assertThat(((LogicalTypeAnnotation) fromLogicalType.invoke(null, stringLt)).toString())
                .contains("STRING");

        LogicalType timeLt = new LogicalType();
        org.apache.parquet.format.TimeType time = new org.apache.parquet.format.TimeType();
        time.setIsAdjustedToUTC(true);
        TimeUnit unit = new TimeUnit();
        unit.setMICROS(new org.apache.parquet.format.MicroSeconds());
        time.setUnit(unit);
        timeLt.setTIME(time);
        assertThat(((LogicalTypeAnnotation) fromLogicalType.invoke(null, timeLt)).toString())
                .contains("TIME");

        LogicalType geographyLt = new LogicalType();
        org.apache.parquet.format.GeographyType geography = new org.apache.parquet.format.GeographyType();
        geography.setCrs("EPSG:4326");
        geography.setAlgorithm(EdgeInterpolationAlgorithm.SPHERICAL);
        geographyLt.setGEOGRAPHY(geography);
        assertThat(((LogicalTypeAnnotation) fromLogicalType.invoke(null, geographyLt)).toString())
                .contains("GEOGRAPHY");

        Method fromConverted =
                CoreParquetFooterReader.class.getDeclaredMethod("fromConvertedType", SchemaElement.class);
        fromConverted.setAccessible(true);

        SchemaElement utf8 = new SchemaElement("name");
        utf8.setConverted_type(ConvertedType.UTF8);
        assertThat(((LogicalTypeAnnotation) fromConverted.invoke(null, utf8)).toString())
                .contains("STRING");

        SchemaElement decimal = new SchemaElement("d");
        decimal.setConverted_type(ConvertedType.DECIMAL);
        decimal.setScale(2);
        decimal.setPrecision(10);
        assertThat(((LogicalTypeAnnotation) fromConverted.invoke(null, decimal)).toString())
                .contains("DECIMAL");

        SchemaElement mapKeyValue = new SchemaElement("mkv");
        mapKeyValue.setConverted_type(ConvertedType.MAP_KEY_VALUE);
        assertThat(fromConverted.invoke(null, mapKeyValue)).isNull();
    }

    @Test
    void privateFromLogicalType_coversAllLogicalVariants() throws Exception {
        Method fromLogicalType = CoreParquetFooterReader.class.getDeclaredMethod("fromLogicalType", LogicalType.class);
        fromLogicalType.setAccessible(true);

        assertThat(fromLogicalType.invoke(null, logical("STRING", new org.apache.parquet.format.StringType())))
                .isNotNull();
        assertThat(fromLogicalType.invoke(null, logical("MAP", new org.apache.parquet.format.MapType())))
                .isNotNull();
        assertThat(fromLogicalType.invoke(null, logical("LIST", new org.apache.parquet.format.ListType())))
                .isNotNull();
        assertThat(fromLogicalType.invoke(null, logical("ENUM", new org.apache.parquet.format.EnumType())))
                .isNotNull();

        org.apache.parquet.format.DecimalType decimal = new org.apache.parquet.format.DecimalType();
        decimal.setScale(2);
        decimal.setPrecision(9);
        assertThat(fromLogicalType.invoke(null, logical("DECIMAL", decimal))).isNotNull();
        assertThat(fromLogicalType.invoke(null, logical("DATE", new org.apache.parquet.format.DateType())))
                .isNotNull();

        org.apache.parquet.format.TimeType time = new org.apache.parquet.format.TimeType();
        time.setIsAdjustedToUTC(true);
        TimeUnit micros = new TimeUnit();
        micros.setMICROS(new org.apache.parquet.format.MicroSeconds());
        time.setUnit(micros);
        assertThat(fromLogicalType.invoke(null, logical("TIME", time))).isNotNull();

        org.apache.parquet.format.TimestampType ts = new org.apache.parquet.format.TimestampType();
        ts.setIsAdjustedToUTC(false);
        TimeUnit nanos = new TimeUnit();
        nanos.setNANOS(new org.apache.parquet.format.NanoSeconds());
        ts.setUnit(nanos);
        assertThat(fromLogicalType.invoke(null, logical("TIMESTAMP", ts))).isNotNull();

        org.apache.parquet.format.IntType intType = new org.apache.parquet.format.IntType();
        intType.setBitWidth((byte) 32);
        intType.setIsSigned(true);
        assertThat(fromLogicalType.invoke(null, logical("INTEGER", intType))).isNotNull();

        assertThat(fromLogicalType.invoke(null, logical("UNKNOWN", new org.apache.parquet.format.NullType())))
                .isNotNull();
        assertThat(fromLogicalType.invoke(null, logical("JSON", new org.apache.parquet.format.JsonType())))
                .isNotNull();
        assertThat(fromLogicalType.invoke(null, logical("BSON", new org.apache.parquet.format.BsonType())))
                .isNotNull();
        assertThat(fromLogicalType.invoke(null, logical("UUID", new org.apache.parquet.format.UUIDType())))
                .isNotNull();
        assertThat(fromLogicalType.invoke(null, logical("FLOAT16", new org.apache.parquet.format.Float16Type())))
                .isNotNull();

        org.apache.parquet.format.VariantType variant = new org.apache.parquet.format.VariantType();
        variant.setSpecification_version((byte) 1);
        assertThat(fromLogicalType.invoke(null, logical("VARIANT", variant))).isNotNull();

        org.apache.parquet.format.GeometryType geom = new org.apache.parquet.format.GeometryType();
        geom.setCrs("EPSG:4326");
        assertThat(fromLogicalType.invoke(null, logical("GEOMETRY", geom))).isNotNull();

        org.apache.parquet.format.GeographyType geog = new org.apache.parquet.format.GeographyType();
        geog.setCrs("EPSG:4326");
        geog.setAlgorithm(EdgeInterpolationAlgorithm.KARNEY);
        assertThat(fromLogicalType.invoke(null, logical("GEOGRAPHY", geog))).isNotNull();
    }

    private static LogicalType logical(String setterName, Object value) throws Exception {
        LogicalType logicalType = new LogicalType();
        Method setter = LogicalType.class.getMethod("set" + setterName, value.getClass());
        setter.invoke(logicalType, value);
        return logicalType;
    }

    @Test
    void privateSchemaAndMetadataHelpers_coverInvalidAndEdgeCases() throws Exception {
        Method readRecordCount = CoreParquetFooterReader.class.getDeclaredMethod("readRecordCount", List.class);
        readRecordCount.setAccessible(true);
        assertThat((Long) readRecordCount.invoke(null, new Object[] {null})).isZero();
        assertThat((Long) readRecordCount.invoke(null, List.of())).isZero();

        Method readKvs = CoreParquetFooterReader.class.getDeclaredMethod("readKeyValueMetadata", List.class);
        readKvs.setAccessible(true);
        assertThat((Map<?, ?>) readKvs.invoke(null, new Object[] {null})).isEmpty();
        org.apache.parquet.format.KeyValue noKey = new org.apache.parquet.format.KeyValue();
        org.apache.parquet.format.KeyValue nullValue = new org.apache.parquet.format.KeyValue("k");
        @SuppressWarnings("unchecked")
        Map<String, String> kvMap = (Map<String, String>) readKvs.invoke(null, List.of(noKey, nullValue));
        assertThat(kvMap).containsEntry("k", null);

        Method fromSchema = CoreParquetFooterReader.class.getDeclaredMethod("fromParquetSchema", List.class);
        fromSchema.setAccessible(true);
        assertThatThrownBy(() -> fromSchema.invoke(null, List.of()))
                .isInstanceOf(InvocationTargetException.class)
                .satisfies(e -> {
                    Throwable cause = e.getCause();
                    assertThat(cause).isInstanceOf(IllegalArgumentException.class);
                    assertThat(cause).hasMessageContaining("empty");
                });

        SchemaElement rootNoChildren = new SchemaElement("root");
        assertThatThrownBy(() -> fromSchema.invoke(null, List.of(rootNoChildren)))
                .isInstanceOf(InvocationTargetException.class)
                .satisfies(e -> {
                    Throwable cause = e.getCause();
                    assertThat(cause).isInstanceOf(IllegalArgumentException.class);
                    assertThat(cause).hasMessageContaining("no children");
                });

        SchemaElement root = new SchemaElement("root");
        root.setNum_children(1);
        SchemaElement unnamed = new SchemaElement();
        unnamed.setType(org.apache.parquet.format.Type.INT32);
        unnamed.setRepetition_type(FieldRepetitionType.REQUIRED);
        assertThatThrownBy(() -> fromSchema.invoke(null, List.of(root, unnamed)))
                .isInstanceOf(InvocationTargetException.class)
                .satisfies(e -> {
                    Throwable cause = e.getCause();
                    assertThat(cause).isInstanceOf(IllegalArgumentException.class);
                    assertThat(cause).hasMessageContaining("unnamed field");
                });

        SchemaElement root2 = new SchemaElement("root");
        root2.setNum_children(1);
        SchemaElement field = new SchemaElement("id");
        field.setType(org.apache.parquet.format.Type.INT32);
        field.setRepetition_type(FieldRepetitionType.REQUIRED);
        SchemaElement trailing = new SchemaElement("extra");
        trailing.setType(org.apache.parquet.format.Type.INT32);
        trailing.setRepetition_type(FieldRepetitionType.REQUIRED);
        assertThatThrownBy(() -> fromSchema.invoke(null, List.of(root2, field, trailing)))
                .isInstanceOf(InvocationTargetException.class)
                .satisfies(e -> {
                    Throwable cause = e.getCause();
                    assertThat(cause).isInstanceOf(IllegalArgumentException.class);
                    assertThat(cause).hasMessageContaining("trailing schema elements");
                });

        SchemaElement root3 = new SchemaElement("root");
        root3.setNum_children(1);
        SchemaElement groupWithoutChildren = new SchemaElement("grp");
        groupWithoutChildren.setRepetition_type(FieldRepetitionType.REQUIRED);
        assertThatThrownBy(() -> fromSchema.invoke(null, List.of(root3, groupWithoutChildren)))
                .isInstanceOf(InvocationTargetException.class)
                .satisfies(e -> {
                    Throwable cause = e.getCause();
                    assertThat(cause).isInstanceOf(IllegalArgumentException.class);
                    assertThat(cause).hasMessageContaining("has no children");
                });

        SchemaElement validRoot = new SchemaElement("schema");
        validRoot.setNum_children(1);
        SchemaElement validId = new SchemaElement("id");
        validId.setType(org.apache.parquet.format.Type.INT32);
        validId.setRepetition_type(FieldRepetitionType.REQUIRED);
        MessageType parsed = (MessageType) fromSchema.invoke(null, List.of(validRoot, validId));
        assertThat(parsed.getFieldCount()).isEqualTo(1);

        Method fromLogicalType = CoreParquetFooterReader.class.getDeclaredMethod("fromLogicalType", LogicalType.class);
        fromLogicalType.setAccessible(true);
        assertThat(fromLogicalType.invoke(null, new Object[] {null})).isNull();
        assertThat(fromLogicalType.invoke(null, new LogicalType())).isNull();
    }
}
