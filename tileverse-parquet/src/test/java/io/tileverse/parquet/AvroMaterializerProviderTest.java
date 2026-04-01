/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 */
package io.tileverse.parquet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
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
}
