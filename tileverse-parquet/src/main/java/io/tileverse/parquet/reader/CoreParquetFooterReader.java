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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.parquet.column.schema.EdgeInterpolationAlgorithm;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.LogicalType;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.TimeUnit;
import org.apache.parquet.format.Util;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

public final class CoreParquetFooterReader {
    private static final int TAIL_SIZE = 8;
    private static final String PARQUET_MAGIC = "PAR1";

    private CoreParquetFooterReader() {}

    public static CoreParquetFooter read(InputFile inputFile) throws IOException {
        Objects.requireNonNull(inputFile, "inputFile");
        long fileLength = inputFile.getLength();
        if (fileLength < TAIL_SIZE) {
            throw new IOException("Invalid Parquet file: too small (%d bytes)".formatted(fileLength));
        }

        byte[] footerBytes;
        try (SeekableInputStream in = inputFile.newStream()) {
            byte[] tail = new byte[TAIL_SIZE];
            in.seek(fileLength - TAIL_SIZE);
            in.readFully(tail);

            ByteBuffer bb = ByteBuffer.wrap(tail).order(ByteOrder.LITTLE_ENDIAN);
            int footerLen = bb.getInt();
            byte[] magic = new byte[4];
            bb.get(magic);
            String magicText = new String(magic, StandardCharsets.US_ASCII);
            if (!PARQUET_MAGIC.equals(magicText)) {
                throw new IOException("Invalid Parquet file: missing PAR1 footer magic");
            }
            if (footerLen < 0 || footerLen > fileLength - TAIL_SIZE) {
                throw new IOException("Invalid Parquet file: bad footer length %d".formatted(footerLen));
            }

            footerBytes = new byte[footerLen];
            in.seek(fileLength - TAIL_SIZE - footerLen);
            in.readFully(footerBytes);
        }

        FileMetaData fileMetaData = Util.readFileMetaData(new ByteArrayInputStream(footerBytes));
        MessageType schema = fromParquetSchema(fileMetaData.getSchema());
        Map<String, String> keyValueMetadata = readKeyValueMetadata(fileMetaData.getKey_value_metadata());
        long recordCount = readRecordCount(fileMetaData.getRow_groups());
        List<CoreRowGroupMeta> rowGroups = readRowGroups(fileMetaData.getRow_groups());

        return new CoreParquetFooter(schema, keyValueMetadata, recordCount, rowGroups);
    }

    private static long readRecordCount(List<RowGroup> rowGroups) {
        if (rowGroups == null || rowGroups.isEmpty()) {
            return 0L;
        }
        long total = 0L;
        for (RowGroup rowGroup : rowGroups) {
            total += rowGroup.getNum_rows();
        }
        return total;
    }

    private static Map<String, String> readKeyValueMetadata(List<KeyValue> keyValues) {
        Map<String, String> map = new LinkedHashMap<>();
        if (keyValues == null) {
            return map;
        }
        for (KeyValue kv : keyValues) {
            if (kv != null && kv.isSetKey()) {
                map.put(kv.getKey(), kv.isSetValue() ? kv.getValue() : null);
            }
        }
        return map;
    }

    private static List<CoreRowGroupMeta> readRowGroups(List<RowGroup> rowGroups) {
        if (rowGroups == null || rowGroups.isEmpty()) {
            return List.of();
        }
        List<CoreRowGroupMeta> groups = new ArrayList<>(rowGroups.size());
        for (RowGroup rowGroup : rowGroups) {
            List<CoreColumnChunkMeta> columns = new ArrayList<>();
            if (rowGroup.isSetColumns()) {
                for (ColumnChunk chunk : rowGroup.getColumns()) {
                    if (!chunk.isSetMeta_data()) {
                        continue;
                    }
                    ColumnMetaData meta = chunk.getMeta_data();
                    List<String> pathSegments = meta.getPath_in_schema();
                    String path = pathSegments.stream().collect(Collectors.joining("."));
                    long dataOffset = meta.getData_page_offset();
                    long dictionaryOffset = meta.isSetDictionary_page_offset() ? meta.getDictionary_page_offset() : -1L;
                    long startOffset = dictionaryOffset > 0 ? Math.min(dictionaryOffset, dataOffset) : dataOffset;
                    if (chunk.isSetFile_offset()
                            && chunk.getFile_offset() > 0
                            && chunk.getFile_offset() < startOffset) {
                        startOffset = chunk.getFile_offset();
                    }

                    Long columnIndexOffset = chunk.isSetColumn_index_offset() ? chunk.getColumn_index_offset() : null;
                    Integer columnIndexLength =
                            chunk.isSetColumn_index_length() ? chunk.getColumn_index_length() : null;
                    Long offsetIndexOffset = chunk.isSetOffset_index_offset() ? chunk.getOffset_index_offset() : null;
                    Integer offsetIndexLength =
                            chunk.isSetOffset_index_length() ? chunk.getOffset_index_length() : null;

                    CompressionCodec codec = meta.isSetCodec() ? meta.getCodec() : CompressionCodec.UNCOMPRESSED;

                    columns.add(new CoreColumnChunkMeta(
                            path,
                            pathSegments,
                            meta.getType(),
                            codec,
                            meta.getNum_values(),
                            meta.getTotal_compressed_size(),
                            startOffset,
                            dataOffset,
                            dictionaryOffset,
                            meta.isSetStatistics() ? meta.getStatistics() : null,
                            columnIndexOffset,
                            columnIndexLength,
                            offsetIndexOffset,
                            offsetIndexLength));
                }
            }
            groups.add(new CoreRowGroupMeta(rowGroup.getNum_rows(), columns));
        }
        return groups;
    }

    private static MessageType fromParquetSchema(List<SchemaElement> schema) {
        if (schema == null || schema.isEmpty()) {
            throw new IllegalArgumentException("Invalid Parquet schema: empty");
        }

        SchemaElement root = schema.get(0);
        if (!root.isSetNum_children()) {
            throw new IllegalArgumentException("Invalid Parquet schema: root element has no children");
        }
        int[] index = {1};
        List<Type> fields = new ArrayList<>(root.getNum_children());
        for (int i = 0; i < root.getNum_children(); i++) {
            fields.add(parseType(schema, index));
        }

        if (index[0] != schema.size()) {
            throw new IllegalArgumentException("Invalid Parquet schema: trailing schema elements");
        }
        String rootName = root.isSetName() ? root.getName() : "schema";
        return new MessageType(rootName, fields);
    }

    private static Type parseType(List<SchemaElement> schema, int[] index) {
        if (index[0] >= schema.size()) {
            throw new IllegalArgumentException("Invalid Parquet schema: unexpected end");
        }
        SchemaElement element = schema.get(index[0]++);
        if (!element.isSetName()) {
            throw new IllegalArgumentException("Invalid Parquet schema: unnamed field");
        }

        Type.Repetition repetition = element.isSetRepetition_type()
                ? fromRepetition(element.getRepetition_type())
                : Type.Repetition.REQUIRED;
        LogicalTypeAnnotation logicalType = getLogicalTypeAnnotation(element);

        Type type;
        if (element.isSetType()) {
            PrimitiveTypeName primitive = fromPrimitive(element.getType());
            Types.PrimitiveBuilder<org.apache.parquet.schema.PrimitiveType> builder =
                    Types.primitive(primitive, repetition);
            if (logicalType != null) {
                builder = builder.as(logicalType);
            }
            if (element.isSetType_length()) {
                builder = builder.length(element.getType_length());
            }
            if (element.isSetPrecision()) {
                builder = builder.precision(element.getPrecision());
            }
            if (element.isSetScale()) {
                builder = builder.scale(element.getScale());
            }
            if (element.isSetField_id()) {
                builder = builder.id(element.getField_id());
            }
            type = builder.named(element.getName());
        } else {
            if (!element.isSetNum_children()) {
                throw new IllegalArgumentException(
                        "Invalid Parquet schema: group field '%s' has no children".formatted(element.getName()));
            }
            List<Type> children = new ArrayList<>(element.getNum_children());
            for (int i = 0; i < element.getNum_children(); i++) {
                children.add(parseType(schema, index));
            }

            Types.GroupBuilder<org.apache.parquet.schema.GroupType> builder = Types.buildGroup(repetition);
            if (logicalType != null) {
                builder = builder.as(logicalType);
            }
            if (element.isSetField_id()) {
                builder = builder.id(element.getField_id());
            }
            type = builder.addFields(children.toArray(Type[]::new)).named(element.getName());
        }

        return type;
    }

    private static Type.Repetition fromRepetition(org.apache.parquet.format.FieldRepetitionType repetition) {
        return switch (repetition) {
            case REQUIRED -> Type.Repetition.REQUIRED;
            case OPTIONAL -> Type.Repetition.OPTIONAL;
            case REPEATED -> Type.Repetition.REPEATED;
        };
    }

    private static PrimitiveTypeName fromPrimitive(org.apache.parquet.format.Type type) {
        return switch (type) {
            case BOOLEAN -> PrimitiveTypeName.BOOLEAN;
            case INT32 -> PrimitiveTypeName.INT32;
            case INT64 -> PrimitiveTypeName.INT64;
            case INT96 -> PrimitiveTypeName.INT96;
            case FLOAT -> PrimitiveTypeName.FLOAT;
            case DOUBLE -> PrimitiveTypeName.DOUBLE;
            case BYTE_ARRAY -> PrimitiveTypeName.BINARY;
            case FIXED_LEN_BYTE_ARRAY -> PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
        };
    }

    private static LogicalTypeAnnotation getLogicalTypeAnnotation(SchemaElement element) {
        if (element.isSetLogicalType()) {
            return fromLogicalType(element.getLogicalType());
        }
        if (element.isSetConverted_type()) {
            return fromConvertedType(element);
        }
        return null;
    }

    private static LogicalTypeAnnotation fromLogicalType(LogicalType logicalType) {
        if (logicalType == null || !logicalType.isSet()) {
            return null;
        }
        return switch (logicalType.getSetField()) {
            case STRING -> LogicalTypeAnnotation.stringType();
            case MAP -> LogicalTypeAnnotation.mapType();
            case LIST -> LogicalTypeAnnotation.listType();
            case ENUM -> LogicalTypeAnnotation.enumType();
            case DECIMAL ->
                LogicalTypeAnnotation.decimalType(
                        logicalType.getDECIMAL().getScale(),
                        logicalType.getDECIMAL().getPrecision());
            case DATE -> LogicalTypeAnnotation.dateType();
            case TIME ->
                LogicalTypeAnnotation.timeType(
                        logicalType.getTIME().isIsAdjustedToUTC(),
                        fromTimeUnit(logicalType.getTIME().getUnit()));
            case TIMESTAMP ->
                LogicalTypeAnnotation.timestampType(
                        logicalType.getTIMESTAMP().isIsAdjustedToUTC(),
                        fromTimeUnit(logicalType.getTIMESTAMP().getUnit()));
            case INTEGER ->
                LogicalTypeAnnotation.intType(
                        logicalType.getINTEGER().getBitWidth(),
                        logicalType.getINTEGER().isIsSigned());
            case UNKNOWN -> LogicalTypeAnnotation.unknownType();
            case JSON -> LogicalTypeAnnotation.jsonType();
            case BSON -> LogicalTypeAnnotation.bsonType();
            case UUID -> LogicalTypeAnnotation.uuidType();
            case FLOAT16 -> LogicalTypeAnnotation.float16Type();
            case VARIANT ->
                LogicalTypeAnnotation.variantType(
                        logicalType.getVARIANT().isSetSpecification_version()
                                ? logicalType.getVARIANT().getSpecification_version()
                                : (byte) 1);
            case GEOMETRY ->
                logicalType.getGEOMETRY().isSetCrs()
                        ? LogicalTypeAnnotation.geometryType(
                                logicalType.getGEOMETRY().getCrs())
                        : LogicalTypeAnnotation.geometryType(LogicalTypeAnnotation.DEFAULT_CRS);
            case GEOGRAPHY -> {
                String crs = logicalType.getGEOGRAPHY().isSetCrs()
                        ? logicalType.getGEOGRAPHY().getCrs()
                        : LogicalTypeAnnotation.DEFAULT_CRS;
                EdgeInterpolationAlgorithm algorithm =
                        logicalType.getGEOGRAPHY().isSetAlgorithm()
                                ? fromEdgeInterpolation(
                                        logicalType.getGEOGRAPHY().getAlgorithm())
                                : LogicalTypeAnnotation.DEFAULT_ALGO;
                yield LogicalTypeAnnotation.geographyType(crs, algorithm);
            }
        };
    }

    private static LogicalTypeAnnotation fromConvertedType(SchemaElement element) {
        ConvertedType convertedType = element.getConverted_type();
        return switch (convertedType) {
            case UTF8 -> LogicalTypeAnnotation.stringType();
            case MAP -> LogicalTypeAnnotation.mapType();
            case MAP_KEY_VALUE -> null;
            case LIST -> LogicalTypeAnnotation.listType();
            case ENUM -> LogicalTypeAnnotation.enumType();
            case DECIMAL -> LogicalTypeAnnotation.decimalType(element.getScale(), element.getPrecision());
            case DATE -> LogicalTypeAnnotation.dateType();
            case TIME_MILLIS -> LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MILLIS);
            case TIME_MICROS -> LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MICROS);
            case TIMESTAMP_MILLIS -> LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS);
            case TIMESTAMP_MICROS -> LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS);
            case UINT_8 -> LogicalTypeAnnotation.intType(8, false);
            case UINT_16 -> LogicalTypeAnnotation.intType(16, false);
            case UINT_32 -> LogicalTypeAnnotation.intType(32, false);
            case UINT_64 -> LogicalTypeAnnotation.intType(64, false);
            case INT_8 -> LogicalTypeAnnotation.intType(8, true);
            case INT_16 -> LogicalTypeAnnotation.intType(16, true);
            case INT_32 -> LogicalTypeAnnotation.intType(32, true);
            case INT_64 -> LogicalTypeAnnotation.intType(64, true);
            case JSON -> LogicalTypeAnnotation.jsonType();
            case BSON -> LogicalTypeAnnotation.bsonType();
            case INTERVAL -> LogicalTypeAnnotation.intervalType();
        };
    }

    private static LogicalTypeAnnotation.TimeUnit fromTimeUnit(TimeUnit timeUnit) {
        if (timeUnit == null || !timeUnit.isSet()) {
            return LogicalTypeAnnotation.TimeUnit.MILLIS;
        }
        return switch (timeUnit.getSetField()) {
            case MILLIS -> LogicalTypeAnnotation.TimeUnit.MILLIS;
            case MICROS -> LogicalTypeAnnotation.TimeUnit.MICROS;
            case NANOS -> LogicalTypeAnnotation.TimeUnit.NANOS;
        };
    }

    private static EdgeInterpolationAlgorithm fromEdgeInterpolation(
            org.apache.parquet.format.EdgeInterpolationAlgorithm algorithm) {
        return EdgeInterpolationAlgorithm.findByValue(algorithm.getValue());
    }
}
