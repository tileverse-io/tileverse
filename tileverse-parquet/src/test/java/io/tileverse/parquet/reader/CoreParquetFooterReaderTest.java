/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 */
package io.tileverse.parquet.reader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
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
    void logicalConversionMethods_coverBranches() {
        LogicalType stringLt = new LogicalType();
        stringLt.setSTRING(new org.apache.parquet.format.StringType());
        assertThat(CoreParquetFooterReader.fromLogicalType(stringLt).toString()).contains("STRING");

        LogicalType timeLt = new LogicalType();
        org.apache.parquet.format.TimeType time = new org.apache.parquet.format.TimeType();
        time.setIsAdjustedToUTC(true);
        TimeUnit unit = new TimeUnit();
        unit.setMICROS(new org.apache.parquet.format.MicroSeconds());
        time.setUnit(unit);
        timeLt.setTIME(time);
        assertThat(CoreParquetFooterReader.fromLogicalType(timeLt).toString()).contains("TIME");

        LogicalType geographyLt = new LogicalType();
        org.apache.parquet.format.GeographyType geography = new org.apache.parquet.format.GeographyType();
        geography.setCrs("EPSG:4326");
        geography.setAlgorithm(EdgeInterpolationAlgorithm.SPHERICAL);
        geographyLt.setGEOGRAPHY(geography);
        assertThat(CoreParquetFooterReader.fromLogicalType(geographyLt).toString())
                .contains("GEOGRAPHY");

        SchemaElement utf8 = new SchemaElement("name");
        utf8.setConverted_type(ConvertedType.UTF8);
        assertThat(CoreParquetFooterReader.fromConvertedType(utf8).toString()).contains("STRING");

        SchemaElement decimal = new SchemaElement("d");
        decimal.setConverted_type(ConvertedType.DECIMAL);
        decimal.setScale(2);
        decimal.setPrecision(10);
        assertThat(CoreParquetFooterReader.fromConvertedType(decimal).toString())
                .contains("DECIMAL");

        SchemaElement mapKeyValue = new SchemaElement("mkv");
        mapKeyValue.setConverted_type(ConvertedType.MAP_KEY_VALUE);
        assertThat(CoreParquetFooterReader.fromConvertedType(mapKeyValue)).isNull();
    }

    @Test
    void fromLogicalType_coversAllLogicalVariants() {
        LogicalType lt;

        lt = new LogicalType();
        lt.setSTRING(new org.apache.parquet.format.StringType());
        assertThat(CoreParquetFooterReader.fromLogicalType(lt)).isNotNull();

        lt = new LogicalType();
        lt.setMAP(new org.apache.parquet.format.MapType());
        assertThat(CoreParquetFooterReader.fromLogicalType(lt)).isNotNull();

        lt = new LogicalType();
        lt.setLIST(new org.apache.parquet.format.ListType());
        assertThat(CoreParquetFooterReader.fromLogicalType(lt)).isNotNull();

        lt = new LogicalType();
        lt.setENUM(new org.apache.parquet.format.EnumType());
        assertThat(CoreParquetFooterReader.fromLogicalType(lt)).isNotNull();

        org.apache.parquet.format.DecimalType decimalType = new org.apache.parquet.format.DecimalType();
        decimalType.setScale(2);
        decimalType.setPrecision(9);
        lt = new LogicalType();
        lt.setDECIMAL(decimalType);
        assertThat(CoreParquetFooterReader.fromLogicalType(lt)).isNotNull();

        lt = new LogicalType();
        lt.setDATE(new org.apache.parquet.format.DateType());
        assertThat(CoreParquetFooterReader.fromLogicalType(lt)).isNotNull();

        org.apache.parquet.format.TimeType time = new org.apache.parquet.format.TimeType();
        time.setIsAdjustedToUTC(true);
        TimeUnit micros = new TimeUnit();
        micros.setMICROS(new org.apache.parquet.format.MicroSeconds());
        time.setUnit(micros);
        lt = new LogicalType();
        lt.setTIME(time);
        assertThat(CoreParquetFooterReader.fromLogicalType(lt)).isNotNull();

        org.apache.parquet.format.TimestampType ts = new org.apache.parquet.format.TimestampType();
        ts.setIsAdjustedToUTC(false);
        TimeUnit nanos = new TimeUnit();
        nanos.setNANOS(new org.apache.parquet.format.NanoSeconds());
        ts.setUnit(nanos);
        lt = new LogicalType();
        lt.setTIMESTAMP(ts);
        assertThat(CoreParquetFooterReader.fromLogicalType(lt)).isNotNull();

        org.apache.parquet.format.IntType intType = new org.apache.parquet.format.IntType();
        intType.setBitWidth((byte) 32);
        intType.setIsSigned(true);
        lt = new LogicalType();
        lt.setINTEGER(intType);
        assertThat(CoreParquetFooterReader.fromLogicalType(lt)).isNotNull();

        lt = new LogicalType();
        lt.setUNKNOWN(new org.apache.parquet.format.NullType());
        assertThat(CoreParquetFooterReader.fromLogicalType(lt)).isNotNull();

        lt = new LogicalType();
        lt.setJSON(new org.apache.parquet.format.JsonType());
        assertThat(CoreParquetFooterReader.fromLogicalType(lt)).isNotNull();

        lt = new LogicalType();
        lt.setBSON(new org.apache.parquet.format.BsonType());
        assertThat(CoreParquetFooterReader.fromLogicalType(lt)).isNotNull();

        lt = new LogicalType();
        lt.setUUID(new org.apache.parquet.format.UUIDType());
        assertThat(CoreParquetFooterReader.fromLogicalType(lt)).isNotNull();

        lt = new LogicalType();
        lt.setFLOAT16(new org.apache.parquet.format.Float16Type());
        assertThat(CoreParquetFooterReader.fromLogicalType(lt)).isNotNull();

        org.apache.parquet.format.VariantType variant = new org.apache.parquet.format.VariantType();
        variant.setSpecification_version((byte) 1);
        lt = new LogicalType();
        lt.setVARIANT(variant);
        assertThat(CoreParquetFooterReader.fromLogicalType(lt)).isNotNull();

        org.apache.parquet.format.GeometryType geom = new org.apache.parquet.format.GeometryType();
        geom.setCrs("EPSG:4326");
        lt = new LogicalType();
        lt.setGEOMETRY(geom);
        assertThat(CoreParquetFooterReader.fromLogicalType(lt)).isNotNull();

        org.apache.parquet.format.GeographyType geog = new org.apache.parquet.format.GeographyType();
        geog.setCrs("EPSG:4326");
        geog.setAlgorithm(EdgeInterpolationAlgorithm.KARNEY);
        lt = new LogicalType();
        lt.setGEOGRAPHY(geog);
        assertThat(CoreParquetFooterReader.fromLogicalType(lt)).isNotNull();
    }

    @Test
    void schemaAndMetadataHelpers_coverInvalidAndEdgeCases() {
        assertThat(CoreParquetFooterReader.readRecordCount(null)).isZero();
        assertThat(CoreParquetFooterReader.readRecordCount(List.of())).isZero();

        assertThat(CoreParquetFooterReader.readKeyValueMetadata(null)).isEmpty();
        org.apache.parquet.format.KeyValue noKey = new org.apache.parquet.format.KeyValue();
        org.apache.parquet.format.KeyValue nullValue = new org.apache.parquet.format.KeyValue("k");
        Map<String, String> kvMap = CoreParquetFooterReader.readKeyValueMetadata(List.of(noKey, nullValue));
        assertThat(kvMap).containsEntry("k", null);

        assertThatThrownBy(() -> CoreParquetFooterReader.fromParquetSchema(List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty");

        SchemaElement rootNoChildren = new SchemaElement("root");
        assertThatThrownBy(() -> CoreParquetFooterReader.fromParquetSchema(List.of(rootNoChildren)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no children");

        SchemaElement root = new SchemaElement("root");
        root.setNum_children(1);
        SchemaElement unnamed = new SchemaElement();
        unnamed.setType(org.apache.parquet.format.Type.INT32);
        unnamed.setRepetition_type(FieldRepetitionType.REQUIRED);
        assertThatThrownBy(() -> CoreParquetFooterReader.fromParquetSchema(List.of(root, unnamed)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unnamed field");

        SchemaElement root2 = new SchemaElement("root");
        root2.setNum_children(1);
        SchemaElement field = new SchemaElement("id");
        field.setType(org.apache.parquet.format.Type.INT32);
        field.setRepetition_type(FieldRepetitionType.REQUIRED);
        SchemaElement trailing = new SchemaElement("extra");
        trailing.setType(org.apache.parquet.format.Type.INT32);
        trailing.setRepetition_type(FieldRepetitionType.REQUIRED);
        assertThatThrownBy(() -> CoreParquetFooterReader.fromParquetSchema(List.of(root2, field, trailing)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("trailing schema elements");

        SchemaElement root3 = new SchemaElement("root");
        root3.setNum_children(1);
        SchemaElement groupWithoutChildren = new SchemaElement("grp");
        groupWithoutChildren.setRepetition_type(FieldRepetitionType.REQUIRED);
        assertThatThrownBy(() -> CoreParquetFooterReader.fromParquetSchema(List.of(root3, groupWithoutChildren)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("has no children");

        SchemaElement validRoot = new SchemaElement("schema");
        validRoot.setNum_children(1);
        SchemaElement validId = new SchemaElement("id");
        validId.setType(org.apache.parquet.format.Type.INT32);
        validId.setRepetition_type(FieldRepetitionType.REQUIRED);
        MessageType parsed = CoreParquetFooterReader.fromParquetSchema(List.of(validRoot, validId));
        assertThat(parsed.getFieldCount()).isEqualTo(1);

        assertThat(CoreParquetFooterReader.fromLogicalType(null)).isNull();
        assertThat(CoreParquetFooterReader.fromLogicalType(new LogicalType())).isNull();
    }
}
