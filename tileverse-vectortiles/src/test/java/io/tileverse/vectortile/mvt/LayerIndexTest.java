/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.assertj.core.api.AbstractThrowableAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

class LayerIndexTest {

    // Field numbers of the vector tile schema
    private static final int TILE_LAYER = 3;
    private static final int LAYER_NAME = 1;
    private static final int LAYER_FEATURE = 2;
    private static final int LAYER_KEY = 3;
    private static final int LAYER_VALUE = 4;
    private static final int LAYER_EXTENT = 5;
    private static final int LAYER_VERSION = 15;
    private static final int FEATURE_ID = 1;
    private static final int FEATURE_TAGS = 2;
    private static final int FEATURE_TYPE = 3;
    private static final int FEATURE_GEOMETRY = 4;
    private static final int VALUE_STRING = 1;
    private static final int VALUE_FLOAT = 2;
    private static final int VALUE_DOUBLE = 3;
    private static final int VALUE_INT = 4;
    private static final int VALUE_UINT = 5;
    private static final int VALUE_SINT = 6;
    private static final int VALUE_BOOL = 7;

    /** A field number used by no message of the schema. */
    private static final int UNKNOWN = 9;

    private static final int LENGTH_DELIMITED = 2;
    private static final int POINT = 1;
    private static final int LINESTRING = 2;

    /** Unknown fields of each wire type, a group included, as the raw bytes taken by {@link ProtoWriter#raw}. */
    private static final int[] UNKNOWN_FIELDS = unsignedBytes(new ProtoWriter()
            .varintField(UNKNOWN, 300)
            .fixed32Field(UNKNOWN, 7)
            .fixed64Field(UNKNOWN, 7L)
            .bytesField(UNKNOWN, new byte[] {1, 2, 3})
            .group(UNKNOWN, new ProtoWriter().varintField(1, 1).toByteArray())
            .toByteArray());

    private final GeometryFactory geometryFactory = new GeometryFactory();

    @Test
    void indexesBuiltTile() throws IOException {
        VectorTileBuilder builder = new VectorTileBuilder().setExtent(512);
        builder.layer()
                .name("a")
                .feature()
                .geometry(point(1, 1))
                .attribute("name", "x")
                .attribute("rank", 3)
                .build()
                .feature()
                .geometry(point(2, 2))
                .attribute("name", "y")
                .attribute("open", true)
                .build()
                .build();
        builder.layer().name("b").feature().geometry(point(3, 3)).build().build();
        byte[] tile = new VectorTileCodec().encode(builder.build());

        LayerIndex[] layers = LayerIndex.indexTile(tile, false);

        assertThat(layers)
                .extracting(LayerIndex::name, LayerIndex::extent, LayerIndex::featureCount)
                .containsExactly(tuple("a", 512, 2), tuple("b", 512, 1));
        LayerIndex a = layers[0];
        LayerIndex b = layers[1];
        assertThat(List.of(a.key(0), a.key(1), a.key(2))).containsExactly("name", "rank", "open");
        assertThat(List.of(a.value(0), a.value(1), a.value(2), a.value(3))).containsExactly("x", 3L, "y", true);
        assertThat(b.attributeNames()).isEmpty();
        VectorTileProto.Tile reference = VectorTileProto.Tile.parseFrom(tile);
        assertThat(featureAt(tile, a, 0)).isEqualTo(reference.getLayers(0).getFeatures(0));
        assertThat(featureAt(tile, a, 1)).isEqualTo(reference.getLayers(0).getFeatures(1));
        assertThat(featureAt(tile, b, 0)).isEqualTo(reference.getLayers(1).getFeatures(0));
    }

    @Test
    void keepsLayerOrder() throws IOException {
        byte[] tile = tile(layer("c"), layer("b"), layer("a"), layer("b"));

        assertThat(LayerIndex.indexTile(tile, false))
                .extracting(LayerIndex::name)
                .containsExactly("c", "b", "a", "b");
    }

    @Test
    void defaultsExtentTo4096() throws IOException {
        LayerIndex layer = onlyLayer(tile(layer("a")));

        assertThat(layer.extent()).isEqualTo(4096);
    }

    @Test
    void keepsLastName() throws IOException {
        LayerIndex layer = onlyLayer(tile(layer("first").stringField(LAYER_NAME, "second")));

        assertThat(layer.name()).isEqualTo("second");
    }

    // Features before the keys and values referenced by their tags, and the version last
    @Test
    void acceptsFieldsInAnyOrder() throws IOException {
        ProtoWriter layer = new ProtoWriter()
                .bytesField(LAYER_FEATURE, roadFeature())
                .bytesField(LAYER_FEATURE, roadFeature())
                .stringField(LAYER_KEY, "class")
                .bytesField(LAYER_VALUE, stringValue("primary"))
                .stringField(LAYER_KEY, "lanes")
                .bytesField(LAYER_VALUE, intValue(2))
                .stringField(LAYER_NAME, "roads")
                .varintField(LAYER_EXTENT, 512)
                .varintField(LAYER_VERSION, 2);

        assertRoadsLayer(onlyLayer(roadsTile()));
        assertRoadsLayer(onlyLayer(tile(layer)));
    }

    @Test
    void rejectsLayerWithoutName() {
        ProtoWriter layer = new ProtoWriter().varintField(LAYER_VERSION, 2).bytesField(LAYER_FEATURE, roadFeature());

        assertRejected(tile(layer));
    }

    @Test
    void rejectsLayerWithoutVersion() {
        ProtoWriter layer = new ProtoWriter().stringField(LAYER_NAME, "roads").bytesField(LAYER_FEATURE, roadFeature());

        assertRejected(tile(layer)).hasMessageContaining("layer roads without its required version field");
    }

    @Test
    void ignoresWrongWireType() throws IOException {
        byte[] extentAsString = tile(layer("a").stringField(LAYER_EXTENT, "512"));
        byte[] nameAsVarint =
                tile(new ProtoWriter().varintField(LAYER_VERSION, 2).varintField(LAYER_NAME, 0));

        assertThat(onlyLayer(extentAsString).extent()).isEqualTo(4096);
        assertRejected(nameAsVarint);
    }

    @Test
    void skipsUnknownFields() throws IOException {
        assertRoadsLayer(onlyLayer(roadsTile()));
        assertRoadsLayer(onlyLayer(roadsTile(UNKNOWN_FIELDS)));
    }

    @ParameterizedTest
    @MethodSource("valueFields")
    void decodesValueTypes(byte[] value, Object expected) throws IOException {
        LayerIndex layer = onlyLayer(tile(layer("a").bytesField(LAYER_VALUE, value)));

        assertThat(layer.value(0)).isEqualTo(expected);
    }

    static Stream<Arguments> valueFields() {
        return Stream.of(
                arguments(named("string", stringValue("text")), "text"),
                arguments(named("float", floatValue(3.1f)), 3.1f),
                arguments(named("double", doubleValue(1.23)), 1.23),
                arguments(named("int", intValue(6)), 6L),
                arguments(named("uint", uintValue(87948)), 87948L),
                arguments(named("sint", sintValue(-87948)), -87948L),
                arguments(named("bool", boolValue(true)), true));
    }

    // The winning field comes first in one value and last in the other
    @Test
    void appliesValuePrecedence() throws IOException {
        byte[] intAndString = new ProtoWriter()
                .varintField(VALUE_INT, 6)
                .stringField(VALUE_STRING, "text")
                .toByteArray();
        byte[] doubleAndBool = new ProtoWriter()
                .fixed64Field(VALUE_DOUBLE, Double.doubleToLongBits(1.23))
                .varintField(VALUE_BOOL, 1)
                .toByteArray();

        LayerIndex layer = onlyLayer(
                tile(layer("a").bytesField(LAYER_VALUE, intAndString).bytesField(LAYER_VALUE, doubleAndBool)));

        assertThat(layer.value(0)).isEqualTo(6L);
        assertThat(layer.value(1)).isEqualTo(true);
    }

    @Test
    void keepsLastValueField() throws IOException {
        byte[] value = new ProtoWriter()
                .varintField(VALUE_INT, 1)
                .varintField(VALUE_INT, 2)
                .toByteArray();

        LayerIndex layer = onlyLayer(tile(layer("a").bytesField(LAYER_VALUE, value)));

        assertThat(layer.value(0)).isEqualTo(2L);
    }

    @Test
    void decodesEmptyValueAsNull() throws IOException {
        LayerIndex layer = onlyLayer(tile(layer("a").bytesField(LAYER_VALUE, new byte[0])));

        assertThat(layer.value(0)).isNull();
    }

    // The type field after the run could complete its last varint
    @Test
    void rejectsCutPackedTags() {
        byte[] feature = new ProtoWriter()
                .bytesField(FEATURE_TAGS, new byte[] {0x00, (byte) 0x80})
                .varintField(FEATURE_TYPE, LINESTRING)
                .toByteArray();

        assertRejected(tile(layer("a").bytesField(LAYER_FEATURE, feature)));
    }

    @Test
    void rejectsCutPackedGeometry() {
        byte[] feature = new ProtoWriter()
                .bytesField(FEATURE_GEOMETRY, new byte[] {0x09, (byte) 0x80})
                .varintField(FEATURE_TYPE, LINESTRING)
                .toByteArray();

        assertRejected(tile(layer("a").bytesField(LAYER_FEATURE, feature)));
    }

    // The feature announces 5 bytes: the 2 left in the layer and the 3 of the next field of the tile
    @Test
    void rejectsFeaturePastLayer() {
        ProtoWriter layer =
                layer("a").tag(LAYER_FEATURE, LENGTH_DELIMITED).varint(5).varintField(FEATURE_ID, 1);
        byte[] tile = new ProtoWriter()
                .bytesField(TILE_LAYER, layer.toByteArray())
                .varintField(UNKNOWN, 300)
                .toByteArray();

        assertRejected(tile);
    }

    @Test
    void rejectsKeyOutOfRange() throws IOException {
        LayerIndex layer = onlyLayer(roadsTile());

        assertThatIllegalArgumentException().isThrownBy(() -> layer.key(-1));
        assertThatIllegalArgumentException().isThrownBy(() -> layer.key(2));
    }

    @Test
    void rejectsValueOutOfRange() throws IOException {
        LayerIndex layer = onlyLayer(roadsTile());

        assertThatIllegalArgumentException().isThrownBy(() -> layer.value(-1));
        assertThatIllegalArgumentException().isThrownBy(() -> layer.value(2));
    }

    @Test
    void indexesEmptyTile() throws IOException {
        assertThat(LayerIndex.indexTile(new byte[0], false)).isEmpty();
    }

    @Test
    void sumsHeapSizeOfLayer() throws IOException {
        ProtoWriter layer = layer("a")
                .stringField(LAYER_KEY, "k1")
                .stringField(LAYER_KEY, "k2")
                .bytesField(LAYER_VALUE, intValue(1000))
                .bytesField(LAYER_VALUE, stringValue("v"))
                .bytesField(LAYER_VALUE, boolValue(true))
                .bytesField(LAYER_FEATURE, roadFeature())
                .bytesField(LAYER_FEATURE, roadFeature());

        LayerIndex index = onlyLayer(tile(layer));

        long indexObject = HeapSize.object(6 * HeapSize.REFERENCE + Integer.BYTES);
        long keysTable = HeapSize.array(HeapSize.REFERENCE, 2);
        long valuesTable = HeapSize.array(HeapSize.REFERENCE, 3);
        long featureStarts = HeapSize.array(Integer.BYTES, 2);
        long featureEnds = HeapSize.array(Integer.BYTES, 2);
        // Two roads of two points: 8 ordinates of 16 bits, 2 part ends, 2 first parts and the part count
        long geometries = HeapSize.object(3 * HeapSize.REFERENCE)
                + HeapSize.object(HeapSize.REFERENCE)
                + HeapSize.array(Short.BYTES, 8)
                + HeapSize.array(Integer.BYTES, 2)
                + HeapSize.array(Integer.BYTES, 3);
        long expected = indexObject
                + HeapSize.string("a")
                + keysTable
                + HeapSize.string("k1")
                + HeapSize.string("k2")
                + valuesTable
                + HeapSize.value(1000L)
                + HeapSize.string("v")
                + HeapSize.value(true)
                + featureStarts
                + featureEnds
                + geometries;
        assertThat(index.heapSize()).isEqualTo(expected);
    }

    @Test
    void packsGeometryOfFeatures() throws IOException {
        LayerIndex layer = onlyLayer(roadsTile());

        PackedGeometries geometries = layer.geometries();
        assertThat(geometries.featureCount()).isEqualTo(2);
        assertThat(coordinates(geometries, 0)).containsExactly(2, 3, 4, 5);
        assertThat(coordinates(geometries, 1)).containsExactly(2, 3, 4, 5);
    }

    // Two packed runs and a single varint: the MoveTo comes apart from its parameters
    @Test
    void packsSplitGeometryRuns() throws IOException {
        byte[] feature = new ProtoWriter()
                .varintField(FEATURE_TYPE, LINESTRING)
                .packedField(FEATURE_GEOMETRY, 9)
                .packedField(FEATURE_GEOMETRY, 4, 6)
                .varintField(FEATURE_GEOMETRY, 10)
                .packedField(FEATURE_GEOMETRY, 4, 4)
                .toByteArray();

        LayerIndex layer = onlyLayer(tile(layer("a").bytesField(LAYER_FEATURE, feature)));

        assertThat(coordinates(layer.geometries(), 0)).containsExactly(2, 3, 4, 5);
    }

    // The last type wins: as a line, the part of the first MoveTo holds one point and is skipped
    @Test
    void packsGeometryBeforeType() throws IOException {
        byte[] feature = new ProtoWriter()
                .packedField(FEATURE_GEOMETRY, 9, 4, 6, 9, 2, 2, 10, 2, 2)
                .varintField(FEATURE_TYPE, POINT)
                .varintField(FEATURE_TYPE, LINESTRING)
                .toByteArray();

        LayerIndex layer = onlyLayer(tile(layer("a").bytesField(LAYER_FEATURE, feature)));

        assertThat(coordinates(layer.geometries(), 0)).containsExactly(3, 4, 4, 5);
    }

    @Test
    void packsMalformedAsEmpty() throws IOException {
        LayerIndex layer = onlyLayer(tileWithMalformedSecondFeature());

        assertThat(layer.geometries().partCount(0)).isEqualTo(1);
        assertThat(layer.geometries().partCount(1)).isZero();
    }

    // The name of the layer comes after its features
    @Test
    void failsOnMalformedGeometry() throws IOException {
        byte[] tile = tileWithMalformedSecondFeature();
        int featureStart = onlyLayer(tile).featureStart(1);

        assertThatThrownBy(() -> LayerIndex.indexTile(tile, true))
                .isInstanceOf(IOException.class)
                .hasCauseInstanceOf(MalformedTileException.class)
                .hasMessage("Malformed vector tile. At byte " + featureStart
                        + ", in feature 1 of layer roads: the geometry starts with LineTo instead of MoveTo");
    }

    /** Layer "roads" with a road feature, then a feature starting with LineTo, then its name. */
    private static byte[] tileWithMalformedSecondFeature() {
        byte[] malformed = new ProtoWriter()
                .varintField(FEATURE_TYPE, LINESTRING)
                .packedField(FEATURE_GEOMETRY, 10, 4, 4)
                .toByteArray();
        ProtoWriter layer = new ProtoWriter()
                .varintField(LAYER_VERSION, 2)
                .bytesField(LAYER_FEATURE, roadFeature())
                .bytesField(LAYER_FEATURE, malformed)
                .stringField(LAYER_NAME, "roads");
        return tile(layer);
    }

    /** The coordinates of the only part of a feature, x then y. */
    private static int[] coordinates(PackedGeometries geometries, int feature) {
        assertThat(geometries.partCount(feature)).isEqualTo(1);
        int part = geometries.firstPart(feature);
        int start = geometries.partStart(part);
        int end = geometries.partEnd(part);
        int[] xy = new int[2 * (end - start)];
        for (int i = start; i < end; i++) {
            xy[2 * (i - start)] = geometries.x(i);
            xy[2 * (i - start) + 1] = geometries.y(i);
        }
        return xy;
    }

    /**
     * Writes layer "roads" in a tile: version 2, extent 512, keys "class" and "lanes", values "primary" and 2, and two
     * features. The fields of {@code leadingFields} start each message: the tile, the layer, its features and its
     * values.
     */
    private static byte[] roadsTile(int... leadingFields) {
        ProtoWriter layer = new ProtoWriter()
                .raw(leadingFields)
                .varintField(LAYER_VERSION, 2)
                .stringField(LAYER_NAME, "roads")
                .varintField(LAYER_EXTENT, 512)
                .stringField(LAYER_KEY, "class")
                .stringField(LAYER_KEY, "lanes")
                .bytesField(LAYER_VALUE, stringValue("primary", leadingFields))
                .bytesField(LAYER_VALUE, intValue(2, leadingFields))
                .bytesField(LAYER_FEATURE, roadFeature(leadingFields))
                .bytesField(LAYER_FEATURE, roadFeature(leadingFields));
        return new ProtoWriter()
                .raw(leadingFields)
                .bytesField(TILE_LAYER, layer.toByteArray())
                .toByteArray();
    }

    /** Feature with id 1, the tags class=primary and lanes=2, type LINESTRING, MoveTo(2, 3) and LineTo(4, 5). */
    private static byte[] roadFeature(int... leadingFields) {
        return new ProtoWriter()
                .raw(leadingFields)
                .varintField(FEATURE_ID, 1)
                .packedField(FEATURE_TAGS, 0, 0, 1, 1)
                .varintField(FEATURE_TYPE, LINESTRING)
                .packedField(FEATURE_GEOMETRY, 9, 4, 6, 10, 4, 4)
                .toByteArray();
    }

    /** Checks the content written by {@link #roadsTile}. */
    private static void assertRoadsLayer(LayerIndex layer) {
        assertThat(layer.name()).isEqualTo("roads");
        assertThat(layer.extent()).isEqualTo(512);
        assertThat(layer.featureCount()).isEqualTo(2);
        assertThat(List.of(layer.key(0), layer.key(1))).containsExactly("class", "lanes");
        assertThat(List.of(layer.value(0), layer.value(1))).containsExactly("primary", 2L);
        assertThatIllegalArgumentException().isThrownBy(() -> layer.key(2));
        assertThatIllegalArgumentException().isThrownBy(() -> layer.value(2));
    }

    private static byte[] stringValue(String value, int... leadingFields) {
        return new ProtoWriter()
                .raw(leadingFields)
                .stringField(VALUE_STRING, value)
                .toByteArray();
    }

    private static byte[] intValue(long value, int... leadingFields) {
        return new ProtoWriter()
                .raw(leadingFields)
                .varintField(VALUE_INT, value)
                .toByteArray();
    }

    private static byte[] floatValue(float value) {
        return new ProtoWriter()
                .fixed32Field(VALUE_FLOAT, Float.floatToIntBits(value))
                .toByteArray();
    }

    private static byte[] doubleValue(double value) {
        return new ProtoWriter()
                .fixed64Field(VALUE_DOUBLE, Double.doubleToLongBits(value))
                .toByteArray();
    }

    private static byte[] boolValue(boolean value) {
        return new ProtoWriter().varintField(VALUE_BOOL, value ? 1 : 0).toByteArray();
    }

    private static byte[] uintValue(long value) {
        return new ProtoWriter().varintField(VALUE_UINT, value).toByteArray();
    }

    /** Writes the value with the zigzag encoding of a protobuf sint64. */
    private static byte[] sintValue(long value) {
        long zigZag = (value << 1) ^ (value >> 63);
        return new ProtoWriter().varintField(VALUE_SINT, zigZag).toByteArray();
    }

    /** A layer with its required fields: version 2 and the name. */
    private static ProtoWriter layer(String name) {
        return new ProtoWriter().varintField(LAYER_VERSION, 2).stringField(LAYER_NAME, name);
    }

    private static byte[] tile(ProtoWriter... layers) {
        ProtoWriter tile = new ProtoWriter();
        for (ProtoWriter layer : layers) {
            tile.bytesField(TILE_LAYER, layer.toByteArray());
        }
        return tile.toByteArray();
    }

    private static LayerIndex onlyLayer(byte[] tile) throws IOException {
        LayerIndex[] layers = LayerIndex.indexTile(tile, false);
        assertThat(layers).hasSize(1);
        return layers[0];
    }

    private static AbstractThrowableAssert<?, ? extends Throwable> assertRejected(byte[] tile) {
        return assertThatThrownBy(() -> LayerIndex.indexTile(tile, false))
                .isInstanceOf(IOException.class)
                .hasCauseInstanceOf(MalformedTileException.class);
    }

    /** Parses the bytes of a feature range with protobuf. */
    private static VectorTileProto.Tile.Feature featureAt(byte[] tile, LayerIndex layer, int feature)
            throws IOException {
        byte[] bytes = Arrays.copyOfRange(tile, layer.featureStart(feature), layer.featureEnd(feature));
        return VectorTileProto.Tile.Feature.parseFrom(bytes);
    }

    private Point point(int x, int y) {
        return geometryFactory.createPoint(new Coordinate(x, y));
    }

    private static int[] unsignedBytes(byte[] bytes) {
        int[] values = new int[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            values[i] = bytes[i] & 0xFF;
        }
        return values;
    }
}
