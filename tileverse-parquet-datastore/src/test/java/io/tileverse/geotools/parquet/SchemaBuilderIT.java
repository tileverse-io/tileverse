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

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.feature.type.AttributeDescriptor;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SchemaBuilderIT {

    private static final Map<String, Integer> EXPECTED_ATTRIBUTE_COUNTS = Map.ofEntries(
            Map.entry("addresses-address", 15),
            Map.entry("base-bathymetry", 13),
            Map.entry("base-infrastructure", 18),
            Map.entry("base-land", 18),
            Map.entry("base-land_cover", 13),
            Map.entry("base-land_use", 17),
            Map.entry("base-water", 18),
            Map.entry("buildings-building", 29),
            Map.entry("buildings-building_part", 27),
            Map.entry("divisions-division", 29),
            Map.entry("divisions-division_area", 18),
            Map.entry("divisions-division_boundary", 16),
            Map.entry("places-place", 23),
            Map.entry("transportation-connector", 8),
            Map.entry("transportation-segment", 25));

    private final SchemaBuilder schemaBuilder = new SchemaBuilder();

    @ParameterizedTest(name = "{0}")
    @MethodSource("overtureSchemas")
    void build_withOvertureSchema_succeeds(String name, String schemaText) {
        MessageType messageType = MessageTypeParser.parseMessageType(schemaText);
        SimpleFeatureType sft = schemaBuilder.build(name, messageType);
        assertThat(sft).isNotNull();
        assertThat(sft.getAttributeCount()).isGreaterThan(0);
        assertThat(sft.getTypeName()).isEqualTo(name);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("overtureSchemas")
    void build_withOvertureSchema_producesExpectedAttributeCount(String name, String schemaText) {
        MessageType messageType = MessageTypeParser.parseMessageType(schemaText);
        SimpleFeatureType sft = schemaBuilder.build(name, messageType);

        Integer expected = EXPECTED_ATTRIBUTE_COUNTS.get(name);
        assertThat(expected).as("No expected count configured for %s", name).isNotNull();
        assertThat(sft.getAttributeCount())
                .as(
                        "attribute count for %s, attributes: %s",
                        name,
                        sft.getAttributeDescriptors().stream()
                                .map(AttributeDescriptor::getLocalName)
                                .toList())
                .isEqualTo(expected);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("overtureSchemas")
    void build_withOvertureSchema_hasCommonColumns(String name, String schemaText) {
        MessageType messageType = MessageTypeParser.parseMessageType(schemaText);
        SimpleFeatureType sft = schemaBuilder.build(name, messageType);

        // Every OvertureMaps schema has: id (String), geometry (byte[]), version (Integer),
        // bbox.xmin/xmax/ymin/ymax (Float), sources (List)
        assertThat(sft.getDescriptor("id")).isNotNull();
        assertThat(sft.getDescriptor("id").getType().getBinding()).isEqualTo(String.class);

        assertThat(sft.getDescriptor("geometry")).isNotNull();
        assertThat(sft.getDescriptor("geometry").getType().getBinding()).isEqualTo(byte[].class);

        assertThat(sft.getDescriptor("version")).isNotNull();
        assertThat(sft.getDescriptor("version").getType().getBinding()).isEqualTo(Integer.class);
        assertThat(sft.getDescriptor("version").isNillable()).isFalse();

        for (String bboxField : List.of("bbox.xmin", "bbox.xmax", "bbox.ymin", "bbox.ymax")) {
            assertThat(sft.getDescriptor(bboxField)).as(bboxField).isNotNull();
            assertThat(sft.getDescriptor(bboxField).getType().getBinding())
                    .as(bboxField)
                    .isEqualTo(Float.class);
        }

        assertThat(sft.getDescriptor("sources")).isNotNull();
        assertThat(sft.getDescriptor("sources").getType().getBinding()).isEqualTo(List.class);
    }

    @ParameterizedTest(name = "addresses-address: {1} -> {2}")
    @MethodSource("addressesAddressAttributes")
    void spotCheck_addressesAddress(String schemaText, String attrName, Class<?> expectedBinding) {
        MessageType messageType = MessageTypeParser.parseMessageType(schemaText);
        SimpleFeatureType sft = schemaBuilder.build("addresses-address", messageType);

        assertThat(sft.getDescriptor(attrName)).as(attrName).isNotNull();
        assertThat(sft.getDescriptor(attrName).getType().getBinding())
                .as(attrName)
                .isEqualTo(expectedBinding);
    }

    static Stream<Arguments> addressesAddressAttributes() throws Exception {
        String schemaText = loadSchema("addresses-address");
        return Stream.of(
                Arguments.of(schemaText, "id", String.class),
                Arguments.of(schemaText, "geometry", byte[].class),
                Arguments.of(schemaText, "bbox.xmin", Float.class),
                Arguments.of(schemaText, "version", Integer.class),
                Arguments.of(schemaText, "sources", List.class),
                Arguments.of(schemaText, "address_levels", List.class),
                Arguments.of(schemaText, "country", String.class),
                Arguments.of(schemaText, "postal_city", String.class));
    }

    @ParameterizedTest(name = "base-land: {1} -> {2}")
    @MethodSource("baseLandAttributes")
    void spotCheck_baseLand(String schemaText, String attrName, Class<?> expectedBinding) {
        MessageType messageType = MessageTypeParser.parseMessageType(schemaText);
        SimpleFeatureType sft = schemaBuilder.build("base-land", messageType);

        assertThat(sft.getDescriptor(attrName)).as(attrName).isNotNull();
        assertThat(sft.getDescriptor(attrName).getType().getBinding())
                .as(attrName)
                .isEqualTo(expectedBinding);
    }

    static Stream<Arguments> baseLandAttributes() throws Exception {
        String schemaText = loadSchema("base-land");
        return Stream.of(
                Arguments.of(schemaText, "names.primary", String.class),
                Arguments.of(schemaText, "names.common", Map.class),
                Arguments.of(schemaText, "names.rules", List.class),
                Arguments.of(schemaText, "source_tags", Map.class),
                Arguments.of(schemaText, "elevation", Integer.class));
    }

    @ParameterizedTest(name = "transportation-segment: {1} -> {2}")
    @MethodSource("transportationSegmentAttributes")
    void spotCheck_transportationSegment(String schemaText, String attrName, Class<?> expectedBinding) {
        MessageType messageType = MessageTypeParser.parseMessageType(schemaText);
        SimpleFeatureType sft = schemaBuilder.build("transportation-segment", messageType);

        assertThat(sft.getDescriptor(attrName)).as(attrName).isNotNull();
        assertThat(sft.getDescriptor(attrName).getType().getBinding())
                .as(attrName)
                .isEqualTo(expectedBinding);
    }

    static Stream<Arguments> transportationSegmentAttributes() throws Exception {
        String schemaText = loadSchema("transportation-segment");
        return Stream.of(
                Arguments.of(schemaText, "connectors", List.class),
                Arguments.of(schemaText, "routes", List.class),
                Arguments.of(schemaText, "access_restrictions", List.class),
                Arguments.of(schemaText, "speed_limits", List.class),
                Arguments.of(schemaText, "names.primary", String.class),
                Arguments.of(schemaText, "names.common", Map.class));
    }

    static Stream<Arguments> overtureSchemas() throws Exception {
        Path dir = schemaResourceDir();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.schema")) {
            Stream.Builder<Arguments> builder = Stream.builder();
            for (Path path : stream) {
                String fileName = path.getFileName().toString();
                String name = fileName.substring(0, fileName.length() - ".schema".length());
                String content = Files.readString(path, StandardCharsets.UTF_8);
                builder.add(Arguments.of(name, content));
            }
            return builder.build();
        }
    }

    private static String loadSchema(String name) throws IOException, URISyntaxException {
        URL url = SchemaBuilderIT.class.getClassLoader().getResource("overturemaps/" + name + ".schema");
        assertThat(url)
                .as("Schema resource overturemaps/%s.schema not found", name)
                .isNotNull();
        return Files.readString(Path.of(url.toURI()), StandardCharsets.UTF_8);
    }

    private static Path schemaResourceDir() throws URISyntaxException {
        URL url = SchemaBuilderIT.class.getClassLoader().getResource("overturemaps");
        assertThat(url).as("overturemaps resource directory not found").isNotNull();
        return Path.of(url.toURI());
    }
}
