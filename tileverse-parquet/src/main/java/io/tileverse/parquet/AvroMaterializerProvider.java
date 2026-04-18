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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.VariantLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.variant.ImmutableMetadata;
import org.apache.parquet.variant.Variant;
import org.apache.parquet.variant.VariantBuilder;
import org.apache.parquet.variant.VariantConverters;

/**
 * A {@link ParquetMaterializerProvider} that produces Avro {@link GenericRecord} instances
 * through a local converter pipeline, without delegating to parquet-avro's
 * {@code AvroReadSupport}.
 *
 * <h2>Why not use {@code AvroReadSupport}?</h2>
 *
 * <p>Parquet-avro's {@code AvroReadSupport} directly imports {@code org.apache.hadoop.conf.Configuration}
 * and {@code org.apache.hadoop.util.ReflectionUtils}, which means {@code hadoop-common} must be on
 * the classpath at runtime. Although parquet-avro declares {@code hadoop-common} with
 * {@code <scope>provided</scope>}, any code that actually instantiates {@code AvroReadSupport}
 * will fail with {@link NoClassDefFoundError} unless hadoop-common is present.
 *
 * <p>{@code hadoop-common} brings 47 compile-scoped transitive dependencies (Guava, Protobuf, Jetty,
 * Jersey, commons-*, Curator, etc.) — a 100 MB+ dependency tree that conflicts with most modern
 * application stacks and defeats the goal of a Hadoop-free Parquet reader.
 *
 * <p>This class reimplements Avro materialization using only:
 * <ul>
 *   <li>Avro APIs ({@link org.apache.avro.Schema}, {@link GenericData.Record})</li>
 *   <li>Parquet column APIs ({@link GroupConverter}, {@link PrimitiveConverter},
 *       {@link RecordMaterializer})</li>
 *   <li>{@link AvroSchemaConverter} from parquet-avro (which itself has no Hadoop imports)</li>
 * </ul>
 *
 * <p>The result is a fully functional Avro record materializer that supports nested records,
 * arrays (LIST), maps (MAP), all primitive types, and nullable unions — with zero Hadoop
 * classes on the classpath.
 */
class AvroMaterializerProvider implements ParquetMaterializerProvider<GenericRecord> {

    /** Singleton instance. */
    static final AvroMaterializerProvider INSTANCE = new AvroMaterializerProvider();

    private AvroMaterializerProvider() {}

    @Override
    public RecordMaterializer<GenericRecord> createMaterializer(
            MessageType fileSchema, MessageType requestedSchema, Map<String, String> fileMetadata) {
        Schema avroSchema = new AvroSchemaConverter().convert(requestedSchema);
        return new SimpleAvroRecordMaterializer(requestedSchema, avroSchema);
    }

    /**
     * Materializer that assembles Avro {@link GenericRecord}s from Parquet column values.
     */
    private static final class SimpleAvroRecordMaterializer extends RecordMaterializer<GenericRecord> {
        private final RecordGroupConverter rootConverter;

        SimpleAvroRecordMaterializer(MessageType requestedSchema, Schema avroSchema) {
            this.rootConverter = new RecordGroupConverter(requestedSchema, avroSchema, null);
        }

        @Override
        public GenericRecord getCurrentRecord() {
            return rootConverter.currentRecord;
        }

        @Override
        public GroupConverter getRootConverter() {
            return rootConverter;
        }
    }

    /**
     * Callback that receives a converted column value.
     */
    @FunctionalInterface
    interface ValueConsumer {
        void accept(Object value);
    }

    /**
     * Converter for Parquet group types that assembles Avro {@link GenericData.Record}s.
     */
    private static final class RecordGroupConverter extends GroupConverter {
        private final Schema schema;
        private final ValueConsumer valueConsumer;
        private final Converter[] converters;
        private GenericData.Record currentRecord;

        RecordGroupConverter(GroupType parquetGroupType, Schema schema, ValueConsumer valueConsumer) {
            this.schema = unwrapNullable(schema);
            this.valueConsumer = valueConsumer;
            this.converters = new Converter[parquetGroupType.getFieldCount()];

            for (int i = 0; i < parquetGroupType.getFieldCount(); i++) {
                Type parquetField = parquetGroupType.getType(i);
                Schema fieldSchema = this.schema.getFields().get(i).schema();
                int fieldIndex = i;
                converters[i] =
                        buildFieldConverter(parquetField, fieldSchema, value -> currentRecord.put(fieldIndex, value));
            }
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            return converters[fieldIndex];
        }

        @Override
        public void start() {
            currentRecord = new GenericData.Record(schema);
        }

        @Override
        public void end() {
            if (valueConsumer != null) {
                valueConsumer.accept(currentRecord);
            }
        }
    }

    private static Converter buildFieldConverter(Type parquetField, Schema avroFieldSchema, ValueConsumer consumer) {
        if (!parquetField.isPrimitive()
                && parquetField.asGroupType().getLogicalTypeAnnotation() instanceof VariantLogicalTypeAnnotation) {
            return new AvroVariantGroupConverter(parquetField.asGroupType(), consumer);
        }

        if (avroFieldSchema.getType() == Schema.Type.UNION && !parquetField.isPrimitive()) {
            long nonNullCount = avroFieldSchema.getTypes().stream()
                    .filter(s -> s.getType() != Schema.Type.NULL)
                    .count();
            if (nonNullCount > 1) {
                return new UnionGroupConverter(parquetField.asGroupType(), avroFieldSchema, consumer);
            }
        }

        Schema schema = unwrapNullable(avroFieldSchema);

        if (schema.getType() == Schema.Type.ARRAY) {
            return new ArrayFieldConverter(parquetField, schema, consumer);
        }
        if (schema.getType() == Schema.Type.MAP) {
            return new MapFieldConverter(parquetField, schema, consumer);
        }

        if (parquetField.isPrimitive()) {
            return new PrimitiveValueConverter(parquetField.asPrimitiveType(), schema, consumer);
        }

        if (parquetField.isRepetition(Type.Repetition.REPEATED)) {
            return new RepeatedGroupFieldConverter(parquetField.asGroupType(), schema, consumer);
        }

        return new RecordGroupConverter(parquetField.asGroupType(), schema, consumer);
    }

    /**
     * Converter for Parquet LIST-annotated fields, collecting elements into a {@link java.util.List}.
     */
    static final class ArrayFieldConverter extends GroupConverter {
        private final List<Object> elements = new ArrayList<>();
        private final ValueConsumer parentConsumer;
        private final Converter repeatedConverter;

        ArrayFieldConverter(Type parquetField, Schema arraySchema, ValueConsumer parentConsumer) {
            this.parentConsumer = parentConsumer;
            Schema elementSchema = unwrapNullable(arraySchema.getElementType());

            if (parquetField.isPrimitive()) {
                this.repeatedConverter =
                        new PrimitiveValueConverter(parquetField.asPrimitiveType(), elementSchema, elements::add);
            } else {
                GroupType groupType = parquetField.asGroupType();
                Type repeatedType = findListRepeatedType(groupType);
                if (repeatedType == null) {
                    this.repeatedConverter = buildFieldConverter(groupType, elementSchema, elements::add);
                } else if (repeatedType.isPrimitive()) {
                    this.repeatedConverter =
                            new PrimitiveValueConverter(repeatedType.asPrimitiveType(), elementSchema, elements::add);
                } else {
                    this.repeatedConverter =
                            new ListElementGroupConverter(repeatedType.asGroupType(), elementSchema, elements::add);
                }
            }
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            if (fieldIndex != 0) {
                throw new IllegalArgumentException("Unsupported LIST field index: " + fieldIndex);
            }
            return repeatedConverter;
        }

        @Override
        public void start() {
            elements.clear();
        }

        @Override
        public void end() {
            parentConsumer.accept(new ArrayList<>(elements));
        }
    }

    /**
     * Converter for the repeated element group inside a Parquet LIST structure.
     */
    static final class ListElementGroupConverter extends GroupConverter {
        private final GroupType elementGroupType;
        private final Schema elementSchema;
        private final ValueConsumer listConsumer;
        private final Converter[] converters;
        private Object singleValue;
        private GenericData.Record recordValue;

        ListElementGroupConverter(GroupType repeatedGroupType, Schema elementSchema, ValueConsumer listConsumer) {
            this.elementGroupType = repeatedGroupType;
            this.elementSchema = unwrapNullable(elementSchema);
            this.listConsumer = listConsumer;
            this.converters = new Converter[repeatedGroupType.getFieldCount()];

            if (repeatedGroupType.getFieldCount() == 1 && this.elementSchema.getType() != Schema.Type.RECORD) {
                Type only = repeatedGroupType.getType(0);
                converters[0] = buildFieldConverter(only, this.elementSchema, value -> singleValue = value);
            } else {
                for (int i = 0; i < repeatedGroupType.getFieldCount(); i++) {
                    Type parquetField = repeatedGroupType.getType(i);
                    Schema fieldSchema = this.elementSchema.getFields().get(i).schema();
                    int fieldIndex = i;
                    converters[i] =
                            buildFieldConverter(parquetField, fieldSchema, value -> recordValue.put(fieldIndex, value));
                }
            }
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            return converters[fieldIndex];
        }

        @Override
        public void start() {
            singleValue = null;
            if (elementGroupType.getFieldCount() == 1 && elementSchema.getType() != Schema.Type.RECORD) {
                recordValue = null;
            } else {
                recordValue = new GenericData.Record(elementSchema);
            }
        }

        @Override
        public void end() {
            if (recordValue != null) {
                listConsumer.accept(recordValue);
            } else {
                listConsumer.accept(singleValue);
            }
        }
    }

    /**
     * Converter for bare repeated group fields (not annotated as LIST), collecting records into a
     * {@link java.util.List}.
     */
    static final class RepeatedGroupFieldConverter extends GroupConverter {
        private final GroupType groupType;
        private final Schema schema;
        private final ValueConsumer parentConsumer;
        private final Converter[] converters;
        private final List<Object> values = new ArrayList<>();
        private GenericData.Record currentRecord;

        RepeatedGroupFieldConverter(GroupType groupType, Schema schema, ValueConsumer parentConsumer) {
            this.groupType = groupType;
            this.schema = unwrapNullable(schema);
            this.parentConsumer = parentConsumer;
            this.converters = new Converter[groupType.getFieldCount()];
            for (int i = 0; i < groupType.getFieldCount(); i++) {
                Type field = groupType.getType(i);
                Schema fieldSchema = this.schema.getFields().get(i).schema();
                int fieldIndex = i;
                converters[i] = buildFieldConverter(field, fieldSchema, value -> currentRecord.put(fieldIndex, value));
            }
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            return converters[fieldIndex];
        }

        @Override
        public void start() {
            if (currentRecord == null && values.isEmpty()) {
                values.clear();
            }
            currentRecord = new GenericData.Record(schema);
        }

        @Override
        public void end() {
            values.add(currentRecord);
            parentConsumer.accept(new ArrayList<>(values));
        }
    }

    /**
     * Converter for Parquet MAP-annotated fields, collecting key-value pairs into a {@link java.util.Map}.
     */
    static final class MapFieldConverter extends GroupConverter {
        private final Map<String, Object> map = new LinkedHashMap<>();
        private final ValueConsumer parentConsumer;
        private final Converter keyValueConverter;

        MapFieldConverter(Type parquetField, Schema mapSchema, ValueConsumer parentConsumer) {
            this.parentConsumer = parentConsumer;
            GroupType mapGroup = parquetField.asGroupType();
            Type repeatedKeyValue = findMapRepeatedType(mapGroup);
            if (repeatedKeyValue == null || repeatedKeyValue.isPrimitive()) {
                throw new IllegalArgumentException("Unsupported MAP encoding for field: " + parquetField.getName());
            }
            Schema valueSchema = unwrapNullable(mapSchema.getValueType());
            this.keyValueConverter =
                    new MapKeyValueGroupConverter(repeatedKeyValue.asGroupType(), valueSchema, map::put);
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            if (fieldIndex != 0) {
                throw new IllegalArgumentException("Unsupported MAP field index: " + fieldIndex);
            }
            return keyValueConverter;
        }

        @Override
        public void start() {
            map.clear();
        }

        @Override
        public void end() {
            parentConsumer.accept(new LinkedHashMap<>(map));
        }
    }

    /**
     * Converter for the repeated key-value group inside a Parquet MAP structure.
     */
    static final class MapKeyValueGroupConverter extends GroupConverter {
        private final GroupType groupType;
        private final Converter[] converters;
        private final MapEntryConsumer entryConsumer;
        private String currentKey;
        private Object currentValue;

        /**
         * Callback that receives a decoded map key-value pair.
         */
        @FunctionalInterface
        interface MapEntryConsumer {
            void put(String key, Object value);
        }

        MapKeyValueGroupConverter(GroupType groupType, Schema valueSchema, MapEntryConsumer entryConsumer) {
            this.groupType = groupType;
            this.entryConsumer = entryConsumer;
            this.converters = new Converter[groupType.getFieldCount()];

            if (groupType.getFieldCount() < 2) {
                throw new IllegalArgumentException("Invalid MAP key_value group: " + groupType);
            }

            Type keyType = groupType.getType(0);
            Type valueType = groupType.getType(1);

            converters[0] = buildFieldConverter(keyType, Schema.create(Schema.Type.STRING), value -> {
                currentKey = value == null ? null : value.toString();
            });
            converters[1] = buildFieldConverter(valueType, valueSchema, value -> currentValue = value);
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            return converters[fieldIndex];
        }

        @Override
        public void start() {
            currentKey = null;
            currentValue = null;
        }

        @Override
        public void end() {
            if (currentKey != null) {
                entryConsumer.put(currentKey, currentValue);
            }
        }
    }

    /**
     * Converter for Parquet primitive values, translating to the corresponding Avro type.
     *
     * <p>Handles logical type conversions (DECIMAL, UUID, ENUM) and dictionary optimization
     * for binary-backed types.
     */
    static final class PrimitiveValueConverter extends PrimitiveConverter {

        private enum ConversionMode {
            DEFAULT,
            DECIMAL_INT,
            DECIMAL_LONG,
            DECIMAL_BINARY,
            UUID,
            ENUM
        }

        private final PrimitiveType primitiveType;
        private final Schema schema;
        private final ValueConsumer consumer;
        private final ConversionMode mode;
        private final int decimalScale;
        private Object[] dict;

        PrimitiveValueConverter(PrimitiveType primitiveType, Schema schema, ValueConsumer consumer) {
            this.primitiveType = primitiveType;
            this.schema = unwrapNullable(schema);
            this.consumer = consumer;

            LogicalTypeAnnotation annotation = primitiveType.getLogicalTypeAnnotation();
            if (annotation instanceof DecimalLogicalTypeAnnotation decimal) {
                this.decimalScale = decimal.getScale();
                this.mode = switch (primitiveType.getPrimitiveTypeName()) {
                    case INT32 -> ConversionMode.DECIMAL_INT;
                    case INT64 -> ConversionMode.DECIMAL_LONG;
                    default -> ConversionMode.DECIMAL_BINARY;
                };
            } else if (annotation instanceof UUIDLogicalTypeAnnotation) {
                this.decimalScale = 0;
                this.mode = ConversionMode.UUID;
            } else if (this.schema.getType() == Schema.Type.ENUM) {
                this.decimalScale = 0;
                this.mode = ConversionMode.ENUM;
            } else {
                this.decimalScale = 0;
                this.mode = ConversionMode.DEFAULT;
            }
        }

        @Override
        public void addInt(int value) {
            if (mode == ConversionMode.DECIMAL_INT) {
                consumer.accept(new BigDecimal(BigInteger.valueOf(value), decimalScale));
            } else {
                consumer.accept(value);
            }
        }

        @Override
        public void addLong(long value) {
            if (mode == ConversionMode.DECIMAL_LONG) {
                consumer.accept(new BigDecimal(BigInteger.valueOf(value), decimalScale));
            } else {
                consumer.accept(value);
            }
        }

        @Override
        public void addBoolean(boolean value) {
            consumer.accept(value);
        }

        @Override
        public void addFloat(float value) {
            consumer.accept(value);
        }

        @Override
        public void addDouble(double value) {
            consumer.accept(value);
        }

        @Override
        public void addBinary(Binary value) {
            consumer.accept(convertBinary(value));
        }

        @Override
        public boolean hasDictionarySupport() {
            PrimitiveTypeName typeName = primitiveType.getPrimitiveTypeName();
            return typeName == PrimitiveTypeName.BINARY || typeName == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
        }

        @Override
        public void setDictionary(Dictionary dictionary) {
            dict = new Object[dictionary.getMaxId() + 1];
            for (int i = 0; i <= dictionary.getMaxId(); i++) {
                dict[i] = convertBinary(dictionary.decodeToBinary(i));
            }
        }

        @Override
        public void addValueFromDictionary(int dictionaryId) {
            Object value = dict[dictionaryId];
            consumer.accept(value instanceof ByteBuffer bb ? bb.duplicate() : value);
        }

        private Object convertBinary(Binary value) {
            if (mode == ConversionMode.DECIMAL_BINARY) {
                return new BigDecimal(new BigInteger(value.getBytes()), decimalScale);
            }
            if (mode == ConversionMode.UUID) {
                return primitiveType.stringifier().stringify(value);
            }
            if (mode == ConversionMode.ENUM) {
                return new GenericData.EnumSymbol(schema, value.toStringUsingUTF8());
            }
            Schema.Type avroType = schema.getType();
            if (avroType == Schema.Type.STRING) {
                return value.toStringUsingUTF8();
            }
            if (avroType == Schema.Type.FIXED) {
                return new GenericData.Fixed(schema, value.getBytes());
            }

            LogicalTypeAnnotation annotation = primitiveType.getLogicalTypeAnnotation();
            if (annotation != null && annotation.toString().contains("STRING")) {
                return value.toStringUsingUTF8();
            }
            return ByteBuffer.wrap(value.getBytes());
        }
    }

    static Type findListRepeatedType(GroupType listGroup) {
        LogicalTypeAnnotation annotation = listGroup.getLogicalTypeAnnotation();
        if (!(annotation instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation)
                && !"LIST".equalsIgnoreCase(String.valueOf(annotation))) {
            if (listGroup.getFieldCount() == 1 && listGroup.getType(0).isRepetition(Type.Repetition.REPEATED)) {
                return listGroup.getType(0);
            }
            return null;
        }

        if (listGroup.getFieldCount() == 0) {
            return null;
        }
        Type first = listGroup.getType(0);
        if (first.isRepetition(Type.Repetition.REPEATED)) {
            return first;
        }
        return first;
    }

    static Type findMapRepeatedType(GroupType mapGroup) {
        LogicalTypeAnnotation annotation = mapGroup.getLogicalTypeAnnotation();
        if (!(annotation instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation)
                && !(annotation instanceof LogicalTypeAnnotation.MapKeyValueTypeAnnotation)
                && !"MAP".equalsIgnoreCase(String.valueOf(annotation))) {
            for (Type child : mapGroup.getFields()) {
                if (child.isRepetition(Type.Repetition.REPEATED)) {
                    return child;
                }
            }
            return null;
        }

        if (mapGroup.getFieldCount() == 0) {
            return null;
        }
        for (Type child : mapGroup.getFields()) {
            if (child.isRepetition(Type.Repetition.REPEATED)) {
                return child;
            }
        }
        return mapGroup.getType(0);
    }

    static Schema unwrapNullable(Schema schema) {
        if (schema.getType() != Schema.Type.UNION) {
            return schema;
        }
        for (Schema branch : schema.getTypes()) {
            if (branch.getType() != Schema.Type.NULL) {
                return branch;
            }
        }
        return Schema.create(Schema.Type.NULL);
    }

    /**
     * Converter for Parquet groups that represent Avro unions with more than one non-null branch.
     *
     * <p>Parquet-avro encodes a multi-branch union as a group with one optional child per non-null
     * branch; at most one child is populated per record. This converter wires one sub-converter per
     * Parquet child (paired with its matching Avro branch) and emits whichever value arrives to the
     * parent consumer.
     */
    static final class UnionGroupConverter extends GroupConverter {
        private final Converter[] memberConverters;
        private final ValueConsumer parentConsumer;
        private Object memberValue;

        UnionGroupConverter(GroupType parquetGroup, Schema unionSchema, ValueConsumer consumer) {
            this.parentConsumer = consumer;
            this.memberConverters = new Converter[parquetGroup.getFieldCount()];
            int parquetIndex = 0;
            for (Schema branch : unionSchema.getTypes()) {
                if (branch.getType() == Schema.Type.NULL) {
                    continue;
                }
                if (parquetIndex >= parquetGroup.getFieldCount()) {
                    break;
                }
                Type memberType = parquetGroup.getType(parquetIndex);
                memberConverters[parquetIndex] = buildFieldConverter(memberType, branch, value -> memberValue = value);
                parquetIndex++;
            }
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            return memberConverters[fieldIndex];
        }

        @Override
        public void start() {
            memberValue = null;
        }

        @Override
        public void end() {
            parentConsumer.accept(memberValue);
        }
    }

    /**
     * Converter for Parquet groups carrying the {@link VariantLogicalTypeAnnotation}.
     *
     * <p>Delegates the low-level Variant decoding to {@link VariantConverters} and, on {@code end()},
     * materializes the resulting {@link Variant} as a Java value tree composed of
     * {@link java.util.LinkedHashMap}, {@link java.util.ArrayList}, boxed primitives,
     * {@link BigDecimal}, {@link LocalDate}, {@link LocalTime}, {@link Instant},
     * {@link LocalDateTime}, {@link java.util.UUID}, {@code byte[]}, or {@code null}.
     *
     * <p>Going through {@code Map}/{@code List} rather than exposing the {@code Variant} object
     * keeps downstream consumers (e.g. the GeoTools datastore) free of any parquet-variant type.
     */
    static final class AvroVariantGroupConverter extends GroupConverter
            implements VariantConverters.ParentConverter<VariantBuilder> {
        private final GroupConverter delegate;
        private final ValueConsumer consumer;
        private VariantBuilder builder;
        private ImmutableMetadata metadata;

        AvroVariantGroupConverter(GroupType variantGroup, ValueConsumer consumer) {
            this.consumer = consumer;
            this.delegate = VariantConverters.newVariantConverter(variantGroup, this::setMetadata, this);
        }

        private void setMetadata(ByteBuffer metadataBuffer) {
            if (metadata == null || metadata.getEncodedBuffer() != metadataBuffer) {
                this.metadata = new ImmutableMetadata(metadataBuffer);
            }
            this.builder = new VariantBuilder(metadata);
        }

        @Override
        public void build(Consumer<VariantBuilder> buildConsumer) {
            buildConsumer.accept(builder);
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            return delegate.getConverter(fieldIndex);
        }

        @Override
        public void start() {
            delegate.start();
        }

        @Override
        public void end() {
            delegate.end();
            if (metadata == null || builder == null) {
                consumer.accept(null);
                return;
            }
            builder.appendNullIfEmpty();
            Variant variant = new Variant(builder.encodedValue(), metadata.getEncodedBuffer());
            consumer.accept(toJavaValue(variant));
            this.builder = null;
        }

        static Object toJavaValue(Variant v) {
            return switch (v.getType()) {
                case NULL -> null;
                case BOOLEAN -> v.getBoolean();
                case BYTE -> (int) v.getByte();
                case SHORT -> (int) v.getShort();
                case INT -> v.getInt();
                case LONG -> v.getLong();
                case FLOAT -> v.getFloat();
                case DOUBLE -> v.getDouble();
                case DECIMAL4, DECIMAL8, DECIMAL16 -> v.getDecimal();
                case STRING -> v.getString();
                case DATE -> LocalDate.ofEpochDay(v.getInt());
                case TIMESTAMP_TZ -> Instant.ofEpochSecond(0, v.getLong() * 1_000L);
                case TIMESTAMP_NTZ ->
                    Instant.ofEpochSecond(0, v.getLong() * 1_000L)
                            .atOffset(ZoneOffset.UTC)
                            .toLocalDateTime();
                case TIMESTAMP_NANOS_TZ -> Instant.ofEpochSecond(0, v.getLong());
                case TIMESTAMP_NANOS_NTZ ->
                    Instant.ofEpochSecond(0, v.getLong())
                            .atOffset(ZoneOffset.UTC)
                            .toLocalDateTime();
                case TIME -> LocalTime.ofNanoOfDay(v.getLong() * 1_000L);
                case UUID -> v.getUUID();
                case BINARY -> {
                    ByteBuffer bb = v.getBinary();
                    byte[] bytes = new byte[bb.remaining()];
                    bb.duplicate().get(bytes);
                    yield bytes;
                }
                case ARRAY -> {
                    int n = v.numArrayElements();
                    List<Object> list = new ArrayList<>(n);
                    for (int i = 0; i < n; i++) {
                        list.add(toJavaValue(v.getElementAtIndex(i)));
                    }
                    yield list;
                }
                case OBJECT -> {
                    int n = v.numObjectElements();
                    Map<String, Object> map = new LinkedHashMap<>(n);
                    for (int i = 0; i < n; i++) {
                        Variant.ObjectField field = v.getFieldAtIndex(i);
                        map.put(field.key, toJavaValue(field.value));
                    }
                    yield map;
                }
            };
        }
    }
}
