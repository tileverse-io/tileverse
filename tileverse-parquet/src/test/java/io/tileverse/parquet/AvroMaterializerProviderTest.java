/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 */
package io.tileverse.parquet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
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
    void unwrapNullable_returnsNonNullBranchAndNullWhenOnlyNulls() throws Exception {
        Schema union = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));
        Schema allNull = Schema.createUnion(List.of(Schema.create(Schema.Type.NULL)));

        Schema unwrapped = (Schema) invokeStatic("unwrapNullable", new Class<?>[] {Schema.class}, union);
        Schema nullOnly = (Schema) invokeStatic("unwrapNullable", new Class<?>[] {Schema.class}, allNull);

        assertThat(unwrapped.getType()).isEqualTo(Schema.Type.STRING);
        assertThat(nullOnly.getType()).isEqualTo(Schema.Type.NULL);
    }

    @Test
    void findListRepeatedType_and_findMapRepeatedType_coverFallbackBranches() throws Exception {
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

        Type listType = (Type) invokeStatic("findListRepeatedType", new Class<?>[] {GroupType.class}, listFallback);
        Type listNone = (Type) invokeStatic("findListRepeatedType", new Class<?>[] {GroupType.class}, listEmpty);
        Type mapType = (Type) invokeStatic("findMapRepeatedType", new Class<?>[] {GroupType.class}, mapFallback);
        Type mapFallbackType =
                (Type) invokeStatic("findMapRepeatedType", new Class<?>[] {GroupType.class}, mapNoRepeated);

        assertThat(listType.getName()).isEqualTo("rep");
        assertThat(listNone).isNull();
        assertThat(mapType.getName()).isEqualTo("rep");
        assertThat(mapFallbackType.getName()).isEqualTo("key_value");
    }

    @Test
    void primitiveValueConverter_addBinary_convertsStringFixedAndByteBuffer() throws Exception {
        Constructor<?> ctor = nestedClass("PrimitiveValueConverter")
                .getDeclaredConstructor(
                        org.apache.parquet.schema.PrimitiveType.class, Schema.class, valueConsumerClass());
        ctor.setAccessible(true);

        org.apache.parquet.schema.PrimitiveType binType = Types.primitive(
                        PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
                .named("b");
        org.apache.parquet.schema.PrimitiveType stringAnnType = Types.primitive(
                        PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
                .as(LogicalTypeAnnotation.stringType())
                .named("s");

        AtomicReference<Object> out = new AtomicReference<>();
        Object consumer = consumerProxy(out::set);

        PrimitiveConverter asString =
                (PrimitiveConverter) ctor.newInstance(binType, Schema.create(Schema.Type.STRING), consumer);
        asString.addBinary(Binary.fromString("abc"));
        assertThat(out.get()).isEqualTo("abc");

        Schema fixedSchema = Schema.createFixed("fx", null, null, 3);
        PrimitiveConverter asFixed = (PrimitiveConverter) ctor.newInstance(binType, fixedSchema, consumer);
        asFixed.addBinary(Binary.fromConstantByteArray(new byte[] {1, 2, 3}));
        assertThat(out.get()).isInstanceOf(GenericData.Fixed.class);

        PrimitiveConverter asAnnotatedString =
                (PrimitiveConverter) ctor.newInstance(stringAnnType, Schema.create(Schema.Type.BYTES), consumer);
        asAnnotatedString.addBinary(Binary.fromString("txt"));
        assertThat(out.get()).isEqualTo("txt");

        PrimitiveConverter asBytes =
                (PrimitiveConverter) ctor.newInstance(binType, Schema.create(Schema.Type.BYTES), consumer);
        asBytes.addBinary(Binary.fromConstantByteArray(new byte[] {9, 8}));
        assertThat(out.get()).isInstanceOf(ByteBuffer.class);
    }

    @Test
    void listElementGroupConverter_coversSingleAndRecordModes() throws Exception {
        Constructor<?> ctor = nestedClass("ListElementGroupConverter")
                .getDeclaredConstructor(GroupType.class, Schema.class, valueConsumerClass());
        ctor.setAccessible(true);

        GroupType singleType = Types.buildGroup(Type.Repetition.REPEATED)
                .required(PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("element")
                .named("element_group");
        List<Object> values = new ArrayList<>();
        GroupConverter single = (GroupConverter)
                ctor.newInstance(singleType, Schema.create(Schema.Type.STRING), consumerProxy(values::add));
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
        GroupConverter record =
                (GroupConverter) ctor.newInstance(recordType, recordSchema, consumerProxy(records::add));
        record.start();
        ((PrimitiveConverter) record.getConverter(0)).addBinary(Binary.fromString("n1"));
        ((PrimitiveConverter) record.getConverter(1)).addInt(7);
        record.end();
        assertThat(records).hasSize(1);
        assertThat(records.get(0)).isInstanceOf(GenericData.Record.class);
    }

    @Test
    void mapKeyValueGroupConverter_putsOnlyWhenKeyPresent_and_validatesShape() throws Exception {
        Constructor<?> ctor = nestedClass("MapKeyValueGroupConverter")
                .getDeclaredConstructor(GroupType.class, Schema.class, mapEntryConsumerClass());
        ctor.setAccessible(true);

        GroupType keyValueType = Types.buildGroup(Type.Repetition.REPEATED)
                .required(PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("key")
                .optional(PrimitiveTypeName.INT32)
                .named("value")
                .named("key_value");
        Map<String, Object> out = new LinkedHashMap<>();
        GroupConverter keyValue = (GroupConverter)
                ctor.newInstance(keyValueType, Schema.create(Schema.Type.INT), mapEntryConsumerProxy(out));
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
        assertThatThrownBy(() -> ctor.newInstance(invalid, Schema.create(Schema.Type.INT), mapEntryConsumerProxy(out)))
                .isInstanceOf(InvocationTargetException.class)
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage("Invalid MAP key_value group: " + invalid);
    }

    @Test
    void mapFieldConverter_rejectsUnsupportedMapEncoding_and_getConverterIndex() throws Exception {
        Constructor<?> ctor =
                nestedClass("MapFieldConverter").getDeclaredConstructor(Type.class, Schema.class, valueConsumerClass());
        ctor.setAccessible(true);

        GroupType invalidMap = Types.buildGroup(Type.Repetition.OPTIONAL)
                .as(LogicalTypeAnnotation.mapType())
                .addField(Types.optional(PrimitiveTypeName.INT32).named("v"))
                .named("bad_map");

        Schema mapSchema = Schema.createMap(Schema.create(Schema.Type.INT));
        assertThatThrownBy(() -> ctor.newInstance(invalidMap, mapSchema, consumerProxy(v -> {})))
                .isInstanceOf(InvocationTargetException.class)
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage("Unsupported MAP encoding for field: bad_map");

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
        GroupConverter mapConverter = (GroupConverter) ctor.newInstance(validMap, mapSchema, consumerProxy(v -> {}));
        assertThatThrownBy(() -> mapConverter.getConverter(1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported MAP field index");
    }

    @Test
    void repeatedGroupFieldConverter_accumulatesValuesAcrossElements() throws Exception {
        Constructor<?> ctor = nestedClass("RepeatedGroupFieldConverter")
                .getDeclaredConstructor(GroupType.class, Schema.class, valueConsumerClass());
        ctor.setAccessible(true);

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
        GroupConverter repeated =
                (GroupConverter) ctor.newInstance(repeatedGroup, recSchema, consumerProxy(emitted::set));

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
    void arrayFieldConverter_throwsOnUnsupportedFieldIndex() throws Exception {
        Constructor<?> ctor = nestedClass("ArrayFieldConverter")
                .getDeclaredConstructor(Type.class, Schema.class, valueConsumerClass());
        ctor.setAccessible(true);

        Type primitiveRepeated = Types.primitive(PrimitiveTypeName.INT32, Type.Repetition.REPEATED)
                .named("items");
        Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.INT));
        GroupConverter array =
                (GroupConverter) ctor.newInstance(primitiveRepeated, arraySchema, consumerProxy(v -> {}));

        assertThatThrownBy(() -> array.getConverter(1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported LIST field index");
    }

    private static Class<?> nestedClass(String simpleName) throws ClassNotFoundException {
        return Class.forName("io.tileverse.parquet.AvroMaterializerProvider$" + simpleName);
    }

    private static Class<?> valueConsumerClass() throws ClassNotFoundException {
        return nestedClass("ValueConsumer");
    }

    private static Class<?> mapEntryConsumerClass() throws ClassNotFoundException {
        return Class.forName(
                "io.tileverse.parquet.AvroMaterializerProvider$MapKeyValueGroupConverter$MapEntryConsumer");
    }

    private static Object consumerProxy(java.util.function.Consumer<Object> sink) throws Exception {
        Class<?> iface = valueConsumerClass();
        return Proxy.newProxyInstance(iface.getClassLoader(), new Class<?>[] {iface}, (proxy, method, args) -> {
            if ("accept".equals(method.getName())) {
                sink.accept(args[0]);
            }
            return null;
        });
    }

    private static Object mapEntryConsumerProxy(Map<String, Object> sink) throws Exception {
        Class<?> iface = mapEntryConsumerClass();
        return Proxy.newProxyInstance(iface.getClassLoader(), new Class<?>[] {iface}, (proxy, method, args) -> {
            if ("put".equals(method.getName())) {
                sink.put((String) args[0], args[1]);
            }
            return null;
        });
    }

    private static Object invokeStatic(String methodName, Class<?>[] argTypes, Object... args) throws Exception {
        Method method = AvroMaterializerProvider.class.getDeclaredMethod(methodName, argTypes);
        method.setAccessible(true);
        return method.invoke(null, args);
    }
}
