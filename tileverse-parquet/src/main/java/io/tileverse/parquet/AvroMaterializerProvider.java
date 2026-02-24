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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/**
 * A {@link ParquetMaterializerProvider} that produces Avro {@link GenericRecord} instances
 * through a local converter pipeline, without delegating to parquet-avro read support.
 */
class AvroMaterializerProvider implements ParquetMaterializerProvider<GenericRecord> {

    static final AvroMaterializerProvider INSTANCE = new AvroMaterializerProvider();

    private AvroMaterializerProvider() {}

    @Override
    public RecordMaterializer<GenericRecord> createMaterializer(
            MessageType fileSchema, MessageType requestedSchema, Map<String, String> fileMetadata) {
        Schema avroSchema = new AvroSchemaConverter().convert(requestedSchema);
        return new SimpleAvroRecordMaterializer(requestedSchema, avroSchema);
    }

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

    @FunctionalInterface
    private interface ValueConsumer {
        void accept(Object value);
    }

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

    private static final class ArrayFieldConverter extends GroupConverter {
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

    private static final class ListElementGroupConverter extends GroupConverter {
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

    private static final class RepeatedGroupFieldConverter extends GroupConverter {
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

    private static final class MapFieldConverter extends GroupConverter {
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

    private static final class MapKeyValueGroupConverter extends GroupConverter {
        private final GroupType groupType;
        private final Converter[] converters;
        private final MapEntryConsumer entryConsumer;
        private String currentKey;
        private Object currentValue;

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

    private static final class PrimitiveValueConverter extends PrimitiveConverter {
        private final PrimitiveType primitiveType;
        private final Schema schema;
        private final ValueConsumer consumer;

        PrimitiveValueConverter(PrimitiveType primitiveType, Schema schema, ValueConsumer consumer) {
            this.primitiveType = primitiveType;
            this.schema = unwrapNullable(schema);
            this.consumer = consumer;
        }

        @Override
        public void addInt(int value) {
            consumer.accept(value);
        }

        @Override
        public void addLong(long value) {
            consumer.accept(value);
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

        private Object convertBinary(Binary value) {
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

    private static Type findListRepeatedType(GroupType listGroup) {
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

    private static Type findMapRepeatedType(GroupType mapGroup) {
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

    private static Schema unwrapNullable(Schema schema) {
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
}
