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
package io.tileverse.geotools.parquet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.feature.type.AttributeDescriptor;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;

class SchemaBuilderTest {

    private final SchemaBuilder schemaBuilder = new SchemaBuilder();

    @Test
    void typeName_isSetCorrectly() {
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message myType {
                  required int32 id;
                }
                """);
        SimpleFeatureType sft = schemaBuilder.build("testTypeName", schema);
        assertThat(sft.getTypeName()).isEqualTo("testTypeName");
    }

    @Test
    void primitiveTypes_defaultBindings() {
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message test {
                  required boolean flag;
                  required int32 intVal;
                  required int64 longVal;
                  required float floatVal;
                  required double doubleVal;
                  required binary data;
                  required fixed_len_byte_array(8) fixedData;
                }
                """);
        SimpleFeatureType sft = schemaBuilder.build("test", schema);
        assertThat(sft.getDescriptor("flag").getType().getBinding()).isEqualTo(Boolean.class);
        assertThat(sft.getDescriptor("intVal").getType().getBinding()).isEqualTo(Integer.class);
        assertThat(sft.getDescriptor("longVal").getType().getBinding()).isEqualTo(Long.class);
        assertThat(sft.getDescriptor("floatVal").getType().getBinding()).isEqualTo(Float.class);
        assertThat(sft.getDescriptor("doubleVal").getType().getBinding()).isEqualTo(Double.class);
        assertThat(sft.getDescriptor("data").getType().getBinding()).isEqualTo(byte[].class);
        assertThat(sft.getDescriptor("fixedData").getType().getBinding()).isEqualTo(byte[].class);
    }

    @Test
    void stringLogicalType() {
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message test {
                  required binary name (UTF8);
                }
                """);
        SimpleFeatureType sft = schemaBuilder.build("test", schema);
        assertThat(sft.getDescriptor("name").getType().getBinding()).isEqualTo(String.class);
    }

    @Test
    void dateLogicalType() {
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message test {
                  required int32 birthDate (DATE);
                }
                """);
        SimpleFeatureType sft = schemaBuilder.build("test", schema);
        assertThat(sft.getDescriptor("birthDate").getType().getBinding()).isEqualTo(LocalDate.class);
    }

    @Test
    void timestampUtc_mapsToInstant() {
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message test {
                  required int64 created (TIMESTAMP(MILLIS,true));
                }
                """);
        SimpleFeatureType sft = schemaBuilder.build("test", schema);
        assertThat(sft.getDescriptor("created").getType().getBinding()).isEqualTo(Instant.class);
    }

    @Test
    void timestampNonUtc_mapsToLocalDateTime() {
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message test {
                  required int64 localTs (TIMESTAMP(MILLIS,false));
                }
                """);
        SimpleFeatureType sft = schemaBuilder.build("test", schema);
        assertThat(sft.getDescriptor("localTs").getType().getBinding()).isEqualTo(LocalDateTime.class);
    }

    @Test
    void timeLogicalType() {
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message test {
                  required int32 timeMillis (TIME(MILLIS,true));
                  required int64 timeMicros (TIME(MICROS,true));
                }
                """);
        SimpleFeatureType sft = schemaBuilder.build("test", schema);
        assertThat(sft.getDescriptor("timeMillis").getType().getBinding()).isEqualTo(LocalTime.class);
        assertThat(sft.getDescriptor("timeMicros").getType().getBinding()).isEqualTo(LocalTime.class);
    }

    @Test
    void decimalLogicalType() {
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message test {
                  required binary amount (DECIMAL(9,2));
                }
                """);
        SimpleFeatureType sft = schemaBuilder.build("test", schema);
        assertThat(sft.getDescriptor("amount").getType().getBinding()).isEqualTo(BigDecimal.class);
    }

    @Test
    void uuidLogicalType() {
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message test {
                  required fixed_len_byte_array(16) uuid (UUID);
                }
                """);
        SimpleFeatureType sft = schemaBuilder.build("test", schema);
        assertThat(sft.getDescriptor("uuid").getType().getBinding()).isEqualTo(UUID.class);
    }

    @Test
    void intSignedSubtypes() {
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message test {
                  required int32 byteVal (INTEGER(8,true));
                  required int32 shortVal (INTEGER(16,true));
                  required int32 intVal (INTEGER(32,true));
                  required int64 longVal (INTEGER(64,true));
                }
                """);
        SimpleFeatureType sft = schemaBuilder.build("test", schema);
        assertThat(sft.getDescriptor("byteVal").getType().getBinding()).isEqualTo(Byte.class);
        assertThat(sft.getDescriptor("shortVal").getType().getBinding()).isEqualTo(Short.class);
        assertThat(sft.getDescriptor("intVal").getType().getBinding()).isEqualTo(Integer.class);
        assertThat(sft.getDescriptor("longVal").getType().getBinding()).isEqualTo(Long.class);
    }

    @Test
    void intUnsignedSubtypes() {
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message test {
                  required int32 u8 (INTEGER(8,false));
                  required int32 u16 (INTEGER(16,false));
                  required int32 u32 (INTEGER(32,false));
                  required int64 u64 (INTEGER(64,false));
                }
                """);
        SimpleFeatureType sft = schemaBuilder.build("test", schema);
        assertThat(sft.getDescriptor("u8").getType().getBinding()).isEqualTo(Integer.class);
        assertThat(sft.getDescriptor("u16").getType().getBinding()).isEqualTo(Integer.class);
        assertThat(sft.getDescriptor("u32").getType().getBinding()).isEqualTo(Long.class);
        assertThat(sft.getDescriptor("u64").getType().getBinding()).isEqualTo(BigInteger.class);
    }

    @Test
    void geometryLogicalType() {
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message test {
                  required binary geom (GEOMETRY);
                }
                """);
        SimpleFeatureType sft = schemaBuilder.build("test", schema);
        assertThat(sft.getDescriptor("geom").getType().getBinding()).isEqualTo(Geometry.class);
    }

    @Test
    void geographyLogicalType() {
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message test {
                  required binary geog (GEOGRAPHY);
                }
                """);
        SimpleFeatureType sft = schemaBuilder.build("test", schema);
        assertThat(sft.getDescriptor("geog").getType().getBinding()).isEqualTo(Geometry.class);
    }

    @Test
    void int96_mapsToBigInteger() {
        MessageType schema = Types.buildMessage()
                .addField(new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT96, "legacyTs"))
                .named("test");
        SimpleFeatureType sft = schemaBuilder.build("test", schema);
        assertThat(sft.getDescriptor("legacyTs").getType().getBinding()).isEqualTo(BigInteger.class);
    }

    @Test
    void requiredField_isNotNillable() {
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message test {
                  required int32 id;
                }
                """);
        SimpleFeatureType sft = schemaBuilder.build("test", schema);
        assertThat(sft.getDescriptor("id").isNillable()).isFalse();
    }

    @Test
    void optionalField_isNillable() {
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message test {
                  optional int32 id;
                }
                """);
        SimpleFeatureType sft = schemaBuilder.build("test", schema);
        assertThat(sft.getDescriptor("id").isNillable()).isTrue();
    }

    @Test
    void nestedGroup_dotJoinedAttributeName() {
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message test {
                  required group address {
                    required binary street (UTF8);
                    required int32 number;
                  }
                }
                """);
        SimpleFeatureType sft = schemaBuilder.build("test", schema);
        assertThat(sft.getDescriptor("address.street")).isNotNull();
        assertThat(sft.getDescriptor("address.street").getType().getBinding()).isEqualTo(String.class);
        assertThat(sft.getDescriptor("address.number")).isNotNull();
        assertThat(sft.getDescriptor("address.number").getType().getBinding()).isEqualTo(Integer.class);
    }

    @Test
    void multipleAttributes_correctCount() {
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message test {
                  required int32 id;
                  required binary name (UTF8);
                  required double value;
                  required boolean active;
                  required int64 timestamp;
                }
                """);
        SimpleFeatureType sft = schemaBuilder.build("test", schema);
        assertThat(sft.getAttributeCount()).isEqualTo(5);
        assertThat(sft.getAttributeDescriptors().stream().map(AttributeDescriptor::getLocalName))
                .containsExactly("id", "name", "value", "active", "timestamp");
    }

    @Test
    void bindingClassVisitor_coversAdditionalLogicalTypesAndUnknownIntWidths() {
        SchemaBuilder.BindingClassVisitor visitor = new SchemaBuilder.BindingClassVisitor();

        assertThat(LogicalTypeAnnotation.enumType().accept(visitor)).contains(String.class);
        assertThat(LogicalTypeAnnotation.jsonType().accept(visitor)).contains(String.class);
        assertThat(LogicalTypeAnnotation.listType().accept(visitor)).contains(java.util.List.class);
        assertThat(LogicalTypeAnnotation.mapType().accept(visitor)).contains(java.util.Map.class);
        assertThatThrownBy(() -> LogicalTypeAnnotation.intType(24, true)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> LogicalTypeAnnotation.intType(24, false)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void unknownLogicalType_fallsBackToPrimitiveDefaultBinding() {
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message test {
                  required binary doc (BSON);
                }
                """);
        SimpleFeatureType sft = schemaBuilder.build("test", schema);
        assertThat(sft.getDescriptor("doc").getType().getBinding()).isEqualTo(byte[].class);
    }

    @Test
    void decodeCrs_handlesParseAndFallbackPaths() {
        assertThat(SchemaBuilder.decodeCrs(null)).isEqualTo(DefaultGeographicCRS.WGS84);
        assertThat(SchemaBuilder.decodeCrs("")).isEqualTo(DefaultGeographicCRS.WGS84);
        assertThat(SchemaBuilder.decodeCrs("INVALID_CRS")).isEqualTo(DefaultGeographicCRS.WGS84);
        assertThat(SchemaBuilder.decodeCrs(DefaultGeographicCRS.WGS84.toWKT())).isNotNull();
    }
}
