/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 */
package io.tileverse.parquet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.parquet.variant.Variant;
import org.apache.parquet.variant.VariantBuilder;
import org.apache.parquet.variant.VariantObjectBuilder;
import org.junit.jupiter.api.Test;

class AvroMaterializerProviderTest {

    @Test
    void unwrapNullable_returnsNonNullBranchAndNullWhenOnlyNulls() {
        Schema union = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));
        Schema allNull = Schema.createUnion(List.of(Schema.create(Schema.Type.NULL)));

        Schema unwrapped = AvroMaterializerProvider.unwrapNullable(union);
        Schema nullOnly = AvroMaterializerProvider.unwrapNullable(allNull);

        assertThat(unwrapped.getType()).isEqualTo(Schema.Type.STRING);
        assertThat(nullOnly.getType()).isEqualTo(Schema.Type.NULL);
    }

    @Test
    void findListRepeatedType_and_findMapRepeatedType_coverFallbackBranches() {
        GroupType listFallback = Types.buildGroup(Type.Repetition.OPTIONAL)
                .addField(Types.primitive(PrimitiveTypeName.INT32, Type.Repetition.REPEATED)
                        .named("rep"))
                .named("not_list");
        GroupType listEmpty = Types.buildGroup(Type.Repetition.OPTIONAL)
                .as(LogicalTypeAnnotation.listType())
                .named("empty_list");
        GroupType mapFallback = Types.buildGroup(Type.Repetition.OPTIONAL)
                .addField(Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.REPEATED)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("rep"))
                .named("not_map");
        GroupType mapNoRepeated = Types.buildGroup(Type.Repetition.OPTIONAL)
                .as(LogicalTypeAnnotation.mapType())
                .addField(Types.buildGroup(Type.Repetition.OPTIONAL)
                        .required(PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("key")
                        .named("key_value"))
                .named("map_no_rep");

        Type listType = AvroMaterializerProvider.findListRepeatedType(listFallback);
        Type listNone = AvroMaterializerProvider.findListRepeatedType(listEmpty);
        Type mapType = AvroMaterializerProvider.findMapRepeatedType(mapFallback);
        Type mapFallbackType = AvroMaterializerProvider.findMapRepeatedType(mapNoRepeated);

        assertThat(listType.getName()).isEqualTo("rep");
        assertThat(listNone).isNull();
        assertThat(mapType.getName()).isEqualTo("rep");
        assertThat(mapFallbackType.getName()).isEqualTo("key_value");
    }

    @Test
    void primitiveValueConverter_addBinary_convertsStringFixedAndByteBuffer() {
        org.apache.parquet.schema.PrimitiveType binType = Types.primitive(
                        PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
                .named("b");
        org.apache.parquet.schema.PrimitiveType stringAnnType = Types.primitive(
                        PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
                .as(LogicalTypeAnnotation.stringType())
                .named("s");

        AtomicReference<Object> out = new AtomicReference<>();
        AvroMaterializerProvider.ValueConsumer consumer = out::set;

        PrimitiveConverter asString = new AvroMaterializerProvider.PrimitiveValueConverter(
                binType, Schema.create(Schema.Type.STRING), consumer);
        asString.addBinary(Binary.fromString("abc"));
        assertThat(out.get()).isEqualTo("abc");

        Schema fixedSchema = Schema.createFixed("fx", null, null, 3);
        PrimitiveConverter asFixed =
                new AvroMaterializerProvider.PrimitiveValueConverter(binType, fixedSchema, consumer);
        asFixed.addBinary(Binary.fromConstantByteArray(new byte[] {1, 2, 3}));
        assertThat(out.get()).isInstanceOf(GenericData.Fixed.class);

        PrimitiveConverter asAnnotatedString = new AvroMaterializerProvider.PrimitiveValueConverter(
                stringAnnType, Schema.create(Schema.Type.BYTES), consumer);
        asAnnotatedString.addBinary(Binary.fromString("txt"));
        assertThat(out.get()).isEqualTo("txt");

        PrimitiveConverter asBytes = new AvroMaterializerProvider.PrimitiveValueConverter(
                binType, Schema.create(Schema.Type.BYTES), consumer);
        asBytes.addBinary(Binary.fromConstantByteArray(new byte[] {9, 8}));
        assertThat(out.get()).isInstanceOf(ByteBuffer.class);
    }

    @Test
    void listElementGroupConverter_coversSingleAndRecordModes() {
        GroupType singleType = Types.buildGroup(Type.Repetition.REPEATED)
                .required(PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("element")
                .named("element_group");
        List<Object> values = new ArrayList<>();
        GroupConverter single = new AvroMaterializerProvider.ListElementGroupConverter(
                singleType, Schema.create(Schema.Type.STRING), (AvroMaterializerProvider.ValueConsumer) values::add);
        single.start();
        ((PrimitiveConverter) single.getConverter(0)).addBinary(Binary.fromString("x"));
        single.end();
        assertThat(values).containsExactly("x");

        GroupType recordType = Types.buildGroup(Type.Repetition.REPEATED)
                .required(PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("name")
                .optional(PrimitiveTypeName.INT32)
                .named("v")
                .named("record_group");
        Schema recordSchema = Schema.createRecord("Elem", null, null, false);
        recordSchema.setFields(List.of(
                new Schema.Field("name", Schema.create(Schema.Type.STRING), null, (Object) null),
                new Schema.Field("v", Schema.create(Schema.Type.INT), null, (Object) null)));

        List<Object> records = new ArrayList<>();
        GroupConverter record = new AvroMaterializerProvider.ListElementGroupConverter(
                recordType, recordSchema, (AvroMaterializerProvider.ValueConsumer) records::add);
        record.start();
        ((PrimitiveConverter) record.getConverter(0)).addBinary(Binary.fromString("n1"));
        ((PrimitiveConverter) record.getConverter(1)).addInt(7);
        record.end();
        assertThat(records).hasSize(1);
        assertThat(records.get(0)).isInstanceOf(GenericData.Record.class);
    }

    @Test
    void mapKeyValueGroupConverter_putsOnlyWhenKeyPresent_and_validatesShape() {
        GroupType keyValueType = Types.buildGroup(Type.Repetition.REPEATED)
                .required(PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("key")
                .optional(PrimitiveTypeName.INT32)
                .named("value")
                .named("key_value");
        Map<String, Object> out = new LinkedHashMap<>();
        GroupConverter keyValue = new AvroMaterializerProvider.MapKeyValueGroupConverter(
                keyValueType,
                Schema.create(Schema.Type.INT),
                (AvroMaterializerProvider.MapKeyValueGroupConverter.MapEntryConsumer) out::put);
        keyValue.start();
        ((PrimitiveConverter) keyValue.getConverter(0)).addBinary(Binary.fromString("k1"));
        ((PrimitiveConverter) keyValue.getConverter(1)).addInt(10);
        keyValue.end();
        assertThat(out).containsEntry("k1", 10);

        keyValue.start();
        ((PrimitiveConverter) keyValue.getConverter(1)).addInt(99);
        keyValue.end();
        assertThat(out).hasSize(1);

        GroupType invalid = Types.buildGroup(Type.Repetition.REPEATED)
                .required(PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("only")
                .named("invalid_key_value");
        assertThatThrownBy(() -> new AvroMaterializerProvider.MapKeyValueGroupConverter(
                        invalid,
                        Schema.create(Schema.Type.INT),
                        (AvroMaterializerProvider.MapKeyValueGroupConverter.MapEntryConsumer) out::put))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid MAP key_value group: " + invalid);
    }

    @Test
    void mapFieldConverter_rejectsUnsupportedMapEncoding_and_getConverterIndex() {
        GroupType invalidMap = Types.buildGroup(Type.Repetition.OPTIONAL)
                .as(LogicalTypeAnnotation.mapType())
                .addField(Types.optional(PrimitiveTypeName.INT32).named("v"))
                .named("bad_map");

        Schema mapSchema = Schema.createMap(Schema.create(Schema.Type.INT));
        assertThatThrownBy(() -> new AvroMaterializerProvider.MapFieldConverter(
                        invalidMap, mapSchema, (AvroMaterializerProvider.ValueConsumer) v -> {}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unsupported MAP encoding for field: bad_map");

        GroupType validMap = Types.buildGroup(Type.Repetition.OPTIONAL)
                .as(LogicalTypeAnnotation.mapType())
                .addField(Types.buildGroup(Type.Repetition.REPEATED)
                        .required(PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("key")
                        .optional(PrimitiveTypeName.INT32)
                        .named("value")
                        .named("key_value"))
                .named("ok_map");
        GroupConverter mapConverter = new AvroMaterializerProvider.MapFieldConverter(
                validMap, mapSchema, (AvroMaterializerProvider.ValueConsumer) v -> {});
        assertThatThrownBy(() -> mapConverter.getConverter(1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported MAP field index");
    }

    @Test
    void repeatedGroupFieldConverter_accumulatesValuesAcrossElements() {
        GroupType repeatedGroup = Types.buildGroup(Type.Repetition.REPEATED)
                .required(PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("name")
                .optional(PrimitiveTypeName.INT32)
                .named("n")
                .named("repeated_group");

        Schema recSchema = Schema.createRecord("Rep", null, null, false);
        recSchema.setFields(List.of(
                new Schema.Field("name", Schema.create(Schema.Type.STRING), null, (Object) null),
                new Schema.Field("n", Schema.create(Schema.Type.INT), null, (Object) null)));

        AtomicReference<Object> emitted = new AtomicReference<>();
        GroupConverter repeated = new AvroMaterializerProvider.RepeatedGroupFieldConverter(
                repeatedGroup, recSchema, (AvroMaterializerProvider.ValueConsumer) emitted::set);

        repeated.start();
        ((PrimitiveConverter) repeated.getConverter(0)).addBinary(Binary.fromString("a"));
        ((PrimitiveConverter) repeated.getConverter(1)).addInt(1);
        repeated.end();
        assertThat((List<?>) emitted.get()).hasSize(1);

        repeated.start();
        ((PrimitiveConverter) repeated.getConverter(0)).addBinary(Binary.fromString("b"));
        ((PrimitiveConverter) repeated.getConverter(1)).addInt(2);
        repeated.end();
        assertThat((List<?>) emitted.get()).hasSize(2);
    }

    // --- Logical Type Conversion Tests ---

    @Test
    void addInt_decimal_producesBigDecimal() {
        org.apache.parquet.schema.PrimitiveType decimalInt32 = Types.primitive(
                        PrimitiveTypeName.INT32, Type.Repetition.REQUIRED)
                .as(LogicalTypeAnnotation.decimalType(2, 9))
                .named("price");
        AtomicReference<Object> out = new AtomicReference<>();
        PrimitiveConverter converter = new AvroMaterializerProvider.PrimitiveValueConverter(
                decimalInt32, Schema.create(Schema.Type.INT), out::set);

        converter.addInt(4200);
        assertThat(out.get()).isEqualTo(new BigDecimal("42.00"));
    }

    @Test
    void addLong_decimal_producesBigDecimal() {
        org.apache.parquet.schema.PrimitiveType decimalInt64 = Types.primitive(
                        PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
                .as(LogicalTypeAnnotation.decimalType(6, 18))
                .named("amount");
        AtomicReference<Object> out = new AtomicReference<>();
        PrimitiveConverter converter = new AvroMaterializerProvider.PrimitiveValueConverter(
                decimalInt64, Schema.create(Schema.Type.LONG), out::set);

        converter.addLong(1234567890L);
        assertThat(out.get()).isEqualTo(new BigDecimal("1234.567890"));
    }

    @Test
    void addBinary_decimal_producesBigDecimal() {
        org.apache.parquet.schema.PrimitiveType decimalBinary = Types.primitive(
                        PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                .as(LogicalTypeAnnotation.decimalType(2, 10))
                .named("val");
        AtomicReference<Object> out = new AtomicReference<>();
        PrimitiveConverter converter = new AvroMaterializerProvider.PrimitiveValueConverter(
                decimalBinary, Schema.create(Schema.Type.BYTES), out::set);

        // 4200 as big-endian bytes
        byte[] bytes = java.math.BigInteger.valueOf(4200).toByteArray();
        converter.addBinary(Binary.fromConstantByteArray(bytes));
        assertThat(out.get()).isEqualTo(new BigDecimal("42.00"));
    }

    @Test
    void addBinary_uuid_producesUUIDString() {
        org.apache.parquet.schema.PrimitiveType uuidType = Types.primitive(
                        PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, Type.Repetition.REQUIRED)
                .length(16)
                .as(LogicalTypeAnnotation.uuidType())
                .named("id");
        AtomicReference<Object> out = new AtomicReference<>();
        Schema fixedSchema = Schema.createFixed("uuid_fixed", null, null, 16);
        PrimitiveConverter converter =
                new AvroMaterializerProvider.PrimitiveValueConverter(uuidType, fixedSchema, out::set);

        UUID expected = UUID.fromString("550e8400-e29b-41d4-a827-446655440000");
        ByteBuffer bb = ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN);
        bb.putLong(expected.getMostSignificantBits());
        bb.putLong(expected.getLeastSignificantBits());

        converter.addBinary(Binary.fromConstantByteArray(bb.array()));
        assertThat(out.get()).isEqualTo("550e8400-e29b-41d4-a827-446655440000");
    }

    @Test
    void addBinary_enum_producesEnumSymbol() {
        org.apache.parquet.schema.PrimitiveType binType = Types.primitive(
                        PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                .named("color");
        Schema enumSchema = Schema.createEnum("Color", null, null, List.of("RED", "GREEN", "BLUE"));
        AtomicReference<Object> out = new AtomicReference<>();
        PrimitiveConverter converter =
                new AvroMaterializerProvider.PrimitiveValueConverter(binType, enumSchema, out::set);

        converter.addBinary(Binary.fromString("RED"));
        assertThat(out.get()).isInstanceOf(GenericData.EnumSymbol.class);
        assertThat(out.get().toString()).isEqualTo("RED");
    }

    @Test
    void addInt_date_passesThrough() {
        org.apache.parquet.schema.PrimitiveType dateType = Types.primitive(
                        PrimitiveTypeName.INT32, Type.Repetition.REQUIRED)
                .as(LogicalTypeAnnotation.dateType())
                .named("dt");
        AtomicReference<Object> out = new AtomicReference<>();
        PrimitiveConverter converter = new AvroMaterializerProvider.PrimitiveValueConverter(
                dateType, Schema.create(Schema.Type.INT), out::set);

        converter.addInt(19000);
        assertThat(out.get()).isEqualTo(19000);
    }

    @Test
    void addLong_timestampMillis_passesThrough() {
        org.apache.parquet.schema.PrimitiveType tsType = Types.primitive(
                        PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
                .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
                .named("ts");
        AtomicReference<Object> out = new AtomicReference<>();
        PrimitiveConverter converter =
                new AvroMaterializerProvider.PrimitiveValueConverter(tsType, Schema.create(Schema.Type.LONG), out::set);

        converter.addLong(1700000000000L);
        assertThat(out.get()).isEqualTo(1700000000000L);
    }

    @Test
    void addInt_noAnnotation_passesThrough() {
        org.apache.parquet.schema.PrimitiveType plainInt = Types.primitive(
                        PrimitiveTypeName.INT32, Type.Repetition.REQUIRED)
                .named("n");
        AtomicReference<Object> out = new AtomicReference<>();
        PrimitiveConverter converter = new AvroMaterializerProvider.PrimitiveValueConverter(
                plainInt, Schema.create(Schema.Type.INT), out::set);

        converter.addInt(42);
        assertThat(out.get()).isEqualTo(42);
    }

    // --- Dictionary Support Tests ---

    @Test
    void hasDictionarySupport_trueForBinary() {
        org.apache.parquet.schema.PrimitiveType binType = Types.primitive(
                        PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                .named("s");
        AvroMaterializerProvider.PrimitiveValueConverter converter =
                new AvroMaterializerProvider.PrimitiveValueConverter(
                        binType, Schema.create(Schema.Type.STRING), v -> {});
        assertThat(converter.hasDictionarySupport()).isTrue();
    }

    @Test
    void hasDictionarySupport_falseForInt32() {
        org.apache.parquet.schema.PrimitiveType intType = Types.primitive(
                        PrimitiveTypeName.INT32, Type.Repetition.REQUIRED)
                .named("n");
        AvroMaterializerProvider.PrimitiveValueConverter converter =
                new AvroMaterializerProvider.PrimitiveValueConverter(intType, Schema.create(Schema.Type.INT), v -> {});
        assertThat(converter.hasDictionarySupport()).isFalse();
    }

    @Test
    void setDictionary_preConvertsEntries() {
        org.apache.parquet.schema.PrimitiveType binType = Types.primitive(
                        PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                .named("s");
        AtomicReference<Object> out = new AtomicReference<>();
        AvroMaterializerProvider.PrimitiveValueConverter converter =
                new AvroMaterializerProvider.PrimitiveValueConverter(
                        binType, Schema.create(Schema.Type.STRING), out::set);

        Binary[] entries = {Binary.fromString("hello"), Binary.fromString("world")};
        Dictionary dict = new Dictionary(Encoding.RLE_DICTIONARY) {
            @Override
            public int getMaxId() {
                return entries.length - 1;
            }

            @Override
            public Binary decodeToBinary(int id) {
                return entries[id];
            }
        };

        converter.setDictionary(dict);
        converter.addValueFromDictionary(0);
        assertThat(out.get()).isEqualTo("hello");
        converter.addValueFromDictionary(1);
        assertThat(out.get()).isEqualTo("world");
    }

    @Test
    void addValueFromDictionary_duplicatesByteBuffer() {
        org.apache.parquet.schema.PrimitiveType binType = Types.primitive(
                        PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                .named("b");
        List<Object> results = new ArrayList<>();
        AvroMaterializerProvider.PrimitiveValueConverter converter =
                new AvroMaterializerProvider.PrimitiveValueConverter(
                        binType, Schema.create(Schema.Type.BYTES), results::add);

        Binary[] entries = {Binary.fromConstantByteArray(new byte[] {1, 2, 3})};
        Dictionary dict = new Dictionary(Encoding.RLE_DICTIONARY) {
            @Override
            public int getMaxId() {
                return entries.length - 1;
            }

            @Override
            public Binary decodeToBinary(int id) {
                return entries[id];
            }
        };

        converter.setDictionary(dict);
        converter.addValueFromDictionary(0);
        converter.addValueFromDictionary(0);

        assertThat(results).hasSize(2);
        ByteBuffer bb1 = (ByteBuffer) results.get(0);
        ByteBuffer bb2 = (ByteBuffer) results.get(1);
        // Both should be readable independently
        assertThat(bb1.remaining()).isEqualTo(3);
        assertThat(bb2.remaining()).isEqualTo(3);
        // Reading one should not affect the other
        bb1.get();
        assertThat(bb2.remaining()).isEqualTo(3);
    }

    @Test
    void arrayFieldConverter_throwsOnUnsupportedFieldIndex() {
        Type primitiveRepeated = Types.primitive(PrimitiveTypeName.INT32, Type.Repetition.REPEATED)
                .named("items");
        Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.INT));
        GroupConverter array = new AvroMaterializerProvider.ArrayFieldConverter(
                primitiveRepeated, arraySchema, (AvroMaterializerProvider.ValueConsumer) v -> {});

        assertThatThrownBy(() -> array.getConverter(1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported LIST field index");
    }

    @Test
    void arrayFieldConverter_collectsPrimitiveAndNestedRecordElements() {
        Type primitiveRepeated = Types.primitive(PrimitiveTypeName.INT32, Type.Repetition.REPEATED)
                .named("items");
        Schema intArraySchema = Schema.createArray(Schema.create(Schema.Type.INT));
        AtomicReference<Object> primitiveOut = new AtomicReference<>();
        GroupConverter primitiveArray = new AvroMaterializerProvider.ArrayFieldConverter(
                primitiveRepeated, intArraySchema, (AvroMaterializerProvider.ValueConsumer) primitiveOut::set);

        primitiveArray.start();
        ((PrimitiveConverter) primitiveArray.getConverter(0)).addInt(1);
        ((PrimitiveConverter) primitiveArray.getConverter(0)).addInt(2);
        primitiveArray.end();
        assertThat(primitiveOut.get()).isEqualTo(List.of(1, 2));

        GroupType structGroup = Types.buildGroup(Type.Repetition.OPTIONAL)
                .required(PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("name")
                .optional(PrimitiveTypeName.INT32)
                .named("age")
                .named("struct_items");
        Schema recordSchema = Schema.createRecord("Item", null, null, false);
        recordSchema.setFields(List.of(
                new Schema.Field("name", Schema.create(Schema.Type.STRING), null, (Object) null),
                new Schema.Field("age", Schema.create(Schema.Type.INT), null, (Object) null)));
        Schema recordArraySchema = Schema.createArray(recordSchema);
        AtomicReference<Object> recordOut = new AtomicReference<>();
        GroupConverter recordArray = new AvroMaterializerProvider.ArrayFieldConverter(
                structGroup, recordArraySchema, (AvroMaterializerProvider.ValueConsumer) recordOut::set);

        recordArray.start();
        GroupConverter element = (GroupConverter) recordArray.getConverter(0);
        element.start();
        ((PrimitiveConverter) element.getConverter(0)).addBinary(Binary.fromString("alice"));
        ((PrimitiveConverter) element.getConverter(1)).addInt(33);
        element.end();
        recordArray.end();

        assertThat(recordOut.get()).isInstanceOf(List.class);
        assertThat((List<?>) recordOut.get()).hasSize(1);
        assertThat(((GenericData.Record) ((List<?>) recordOut.get()).get(0))
                        .get("name")
                        .toString())
                .isEqualTo("alice");
    }

    // --- Multi-Branch Union Tests ---

    @Test
    void unionGroupConverter_stringBranch_emitsString() {
        GroupType unionGroup = Types.buildGroup(Type.Repetition.OPTIONAL)
                .optional(PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("member0")
                .optional(PrimitiveTypeName.INT64)
                .named("member1")
                .named("union_field");
        Schema unionSchema = Schema.createUnion(
                Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.LONG));

        AtomicReference<Object> out = new AtomicReference<>();
        GroupConverter converter = new AvroMaterializerProvider.UnionGroupConverter(
                unionGroup, unionSchema, (AvroMaterializerProvider.ValueConsumer) out::set);

        converter.start();
        ((PrimitiveConverter) converter.getConverter(0)).addBinary(Binary.fromString("hello"));
        converter.end();
        assertThat(out.get()).isEqualTo("hello");
    }

    @Test
    void unionGroupConverter_longBranch_emitsLong() {
        GroupType unionGroup = Types.buildGroup(Type.Repetition.OPTIONAL)
                .optional(PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("member0")
                .optional(PrimitiveTypeName.INT64)
                .named("member1")
                .named("union_field");
        Schema unionSchema = Schema.createUnion(
                Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.LONG));

        AtomicReference<Object> out = new AtomicReference<>();
        GroupConverter converter = new AvroMaterializerProvider.UnionGroupConverter(
                unionGroup, unionSchema, (AvroMaterializerProvider.ValueConsumer) out::set);

        converter.start();
        ((PrimitiveConverter) converter.getConverter(1)).addLong(42L);
        converter.end();
        assertThat(out.get()).isEqualTo(42L);
    }

    @Test
    void unionGroupConverter_nullRow_emitsNull() {
        GroupType unionGroup = Types.buildGroup(Type.Repetition.OPTIONAL)
                .optional(PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("member0")
                .optional(PrimitiveTypeName.INT64)
                .named("member1")
                .named("union_field");
        Schema unionSchema = Schema.createUnion(
                Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.LONG));

        AtomicReference<Object> out = new AtomicReference<>();
        out.set("sentinel");
        GroupConverter converter = new AvroMaterializerProvider.UnionGroupConverter(
                unionGroup, unionSchema, (AvroMaterializerProvider.ValueConsumer) out::set);

        converter.start();
        converter.end();
        assertThat(out.get()).isNull();
    }

    @Test
    void unionGroupConverter_resetsStateBetweenRecords() {
        GroupType unionGroup = Types.buildGroup(Type.Repetition.OPTIONAL)
                .optional(PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("member0")
                .optional(PrimitiveTypeName.INT64)
                .named("member1")
                .named("union_field");
        Schema unionSchema = Schema.createUnion(
                Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.LONG));

        List<Object> emitted = new ArrayList<>();
        GroupConverter converter = new AvroMaterializerProvider.UnionGroupConverter(
                unionGroup, unionSchema, (AvroMaterializerProvider.ValueConsumer) emitted::add);

        converter.start();
        ((PrimitiveConverter) converter.getConverter(0)).addBinary(Binary.fromString("first"));
        converter.end();
        converter.start();
        converter.end();
        converter.start();
        ((PrimitiveConverter) converter.getConverter(1)).addLong(7L);
        converter.end();

        assertThat(emitted).containsExactly("first", null, 7L);
    }

    // --- VARIANT as java.util.Map Tests ---

    @Test
    void variantToJavaValue_primitiveString() {
        VariantBuilder b = new VariantBuilder();
        b.appendString("hello");
        Variant v = b.build();

        Object out = AvroMaterializerProvider.AvroVariantGroupConverter.toJavaValue(v);
        assertThat(out).isEqualTo("hello");
    }

    @Test
    void variantToJavaValue_primitivesCoverAllScalarTypes() {
        assertThat(toJavaValueOf(b -> b.appendBoolean(true))).isEqualTo(Boolean.TRUE);
        assertThat(toJavaValueOf(b -> b.appendLong(42L))).isEqualTo(42L);
        assertThat(toJavaValueOf(b -> b.appendDouble(3.5))).isEqualTo(3.5);
        assertThat(toJavaValueOf(b -> b.appendFloat(1.25f))).isEqualTo(1.25f);
        assertThat(toJavaValueOf(b -> b.appendDecimal(new BigDecimal("12.34")))).isEqualTo(new BigDecimal("12.34"));
        assertThat(toJavaValueOf(b -> b.appendDate(19723))).isEqualTo(LocalDate.ofEpochDay(19723));
        assertThat(toJavaValueOf(b -> b.appendNull())).isNull();
    }

    @Test
    void variantToJavaValue_arrayProducesList() {
        Variant v = buildVariant(b -> {
            var arr = b.startArray();
            arr.appendLong(1L);
            arr.appendLong(2L);
            arr.appendLong(3L);
            b.endArray();
        });

        Object out = AvroMaterializerProvider.AvroVariantGroupConverter.toJavaValue(v);
        assertThat(out).isEqualTo(List.of(1L, 2L, 3L));
    }

    @Test
    void variantToJavaValue_objectProducesLinkedHashMap() {
        Variant v = buildVariant(b -> {
            VariantObjectBuilder obj = b.startObject();
            obj.appendKey("a");
            obj.appendLong(1L);
            obj.appendKey("b");
            obj.appendString("hello");
            b.endObject();
        });

        Object out = AvroMaterializerProvider.AvroVariantGroupConverter.toJavaValue(v);
        assertThat(out).isInstanceOf(LinkedHashMap.class).isEqualTo(Map.of("a", 1L, "b", "hello"));
    }

    @Test
    void variantToJavaValue_nestedObjectAndArray() {
        Variant v = buildVariant(b -> {
            VariantObjectBuilder obj = b.startObject();
            obj.appendKey("outer");
            VariantObjectBuilder inner = obj.startObject();
            inner.appendKey("nums");
            var arr = inner.startArray();
            arr.appendLong(10L);
            arr.appendLong(20L);
            inner.endArray();
            obj.endObject();
            b.endObject();
        });

        Object out = AvroMaterializerProvider.AvroVariantGroupConverter.toJavaValue(v);
        assertThat(out).isEqualTo(Map.of("outer", Map.of("nums", List.of(10L, 20L))));
    }

    private static Variant buildVariant(java.util.function.Consumer<VariantBuilder> consumer) {
        VariantBuilder b = new VariantBuilder();
        consumer.accept(b);
        return b.build();
    }

    private static Object toJavaValueOf(java.util.function.Consumer<VariantBuilder> consumer) {
        return AvroMaterializerProvider.AvroVariantGroupConverter.toJavaValue(buildVariant(consumer));
    }
}
