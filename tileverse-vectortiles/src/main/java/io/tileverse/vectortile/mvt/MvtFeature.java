/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import static io.tileverse.vectortile.mvt.LayerIndex.FEATURE_AND_VALUE_DEPTH;
import static io.tileverse.vectortile.mvt.LayerIndex.FEATURE_GEOMETRY;
import static io.tileverse.vectortile.mvt.LayerIndex.FEATURE_ID;
import static io.tileverse.vectortile.mvt.LayerIndex.FEATURE_TAGS;
import static io.tileverse.vectortile.mvt.LayerIndex.FEATURE_TYPE;
import static io.tileverse.vectortile.mvt.LayerIndex.FEATURE_UNPACKED_GEOMETRY;
import static io.tileverse.vectortile.mvt.LayerIndex.FEATURE_UNPACKED_TAGS;
import static io.tileverse.vectortile.mvt.LayerIndex.NO_RUN;
import static io.tileverse.vectortile.mvt.LayerIndex.copyRuns;
import static io.tileverse.vectortile.mvt.LayerIndex.nextRun;
import static io.tileverse.vectortile.mvt.LayerIndex.readGeomType;
import static io.tileverse.vectortile.mvt.LayerIndex.skipRun;
import static java.util.Objects.requireNonNull;

import io.tileverse.vectortile.model.GeometryReader;
import io.tileverse.vectortile.model.VectorTile;
import io.tileverse.vectortile.model.VectorTile.Layer;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import org.locationtech.jts.geom.Geometry;

/**
 * A feature of a decoded tile. Creating the feature reads its id and its type from the tile bytes, and finds its tags.
 * The attributes are decoded from the tags on first use. The geometry is built on first use from the geometries of the
 * layer, packed when the tile was decoded. The feature keeps both.
 *
 * <p>The fields follow the rules of protobuf's parser: a field keeps its last occurrence, and the tags may come as one
 * packed run, as several runs, or as single varints. A field of one run is read from the tile bytes. A field of several
 * runs is copied into an array of its own when read.
 */
final class MvtFeature implements VectorTile.Layer.Feature {

    private final MvtLayer layer;
    private final int index;
    private final GeometryReader decoder;
    private long id;
    private boolean hasId;
    private GeomType type = GeomType.UNKNOWN;

    // The bounds of the last run of the tags in the tile bytes. A run follows the tag of its field, and its end is
    // above 0. A feature without tags keeps the bounds 0 and 0.
    private int tagsStart;
    private int tagsEnd;

    private boolean severalTagRuns;

    private Map<String, Object> attributes;
    private Geometry geometry;

    /** Reads the fields of the feature at {@code index} in the layer. */
    MvtFeature(MvtLayer layer, int index, GeometryReader decoder) {
        this.layer = requireNonNull(layer, "layer");
        this.index = index;
        this.decoder = requireNonNull(decoder, "decoder");
        readFields();
    }

    /*
     * Decoding the tile validated the feature: these reads do not fail. The reader is created here, where the JIT can
     * keep it off the heap.
     */
    private void readFields() {
        WireReader reader = fieldReader();
        while (reader.hasRemaining()) {
            int tag = reader.readTag();
            switch (tag) {
                case FEATURE_ID -> readId(reader);
                case FEATURE_TYPE -> type = readGeomType(reader, type);
                case FEATURE_TAGS, FEATURE_UNPACKED_TAGS -> readTagsRun(reader, tag);
                default -> reader.skipField(tag);
            }
        }
    }

    private WireReader fieldReader() {
        LayerIndex layerIndex = layer.index();
        int start = layerIndex.featureStart(index);
        int end = layerIndex.featureEnd(index);
        return new WireReader(tile(), start, end, FEATURE_AND_VALUE_DEPTH);
    }

    private byte[] tile() {
        return layer.tile().bytes();
    }

    private void readId(WireReader reader) {
        id = reader.readVarint64();
        hasId = true;
    }

    private void readTagsRun(WireReader reader, int tag) {
        if (tagsEnd > 0) {
            severalTagRuns = true;
        }
        tagsStart = skipRun(reader, tag);
        tagsEnd = reader.position();
    }

    @Override
    public long getId() {
        return id;
    }

    /**
     * Whether the feature has an id field. {@link #getId()} returns 0 for a feature without one, as for an id of 0. An
     * id of 2^63 or above is returned as a negative long.
     */
    boolean hasId() {
        return hasId;
    }

    @Override
    public Layer getLayer() {
        return layer;
    }

    @Override
    public Geometry getGeometry() {
        if (geometry == null) {
            geometry = decoder.decode(this);
        }
        return geometry;
    }

    @Override
    public Object getAttribute(String attributeName) {
        return getAttributes().getOrDefault(attributeName, null);
    }

    /**
     * Returns the attributes, decoded on the first call. The map is mutable and kept by the feature.
     *
     * @throws IllegalArgumentException on an odd number of tags, or on a tag outside the keys or the values of the
     *     layer
     */
    @Override
    public Map<String, Object> getAttributes() {
        if (attributes == null) {
            attributes = readAttributes();
        }
        return attributes;
    }

    private Map<String, Object> readAttributes() {
        Map<String, Object> values = new HashMap<>();
        forEachTag((key, value) -> values.put(key, modelValue(value)));
        return values;
    }

    // The model has no unsigned long: a uint value of 2^63 or above reads as a negative long
    private static Object modelValue(Object value) {
        if (value instanceof OpenLayer.UnsignedValue unsigned) {
            return Long.valueOf(unsigned.bits());
        }
        return value;
    }

    /**
     * Hands the key and the value of each attribute to the action, as a merge writes them. Once
     * {@link #getAttributes()} has built its map, the action receives the entries of the map, with the changes made
     * through it. Until then, it receives the tags in tile order, without building the map. A uint value of 2^63 or
     * above comes as an {@link OpenLayer.UnsignedValue}, apart from the negative long read by the model, also from an
     * unchanged entry of the map.
     *
     * @throws IllegalArgumentException on an odd number of tags, or on a tag outside the keys or the values of the
     *     layer
     */
    void forEachAttribute(BiConsumer<String, Object> action) {
        if (attributes == null) {
            forEachTag(action);
            return;
        }
        for (Map.Entry<String, Object> attribute : attributes.entrySet()) {
            String key = attribute.getKey();
            action.accept(key, wireValue(key, attribute.getValue()));
        }
    }

    /**
     * Returns the value of an entry of the map as a merge writes it. The model reads a uint value of 2^63 or above as a
     * negative long: when the entry keeps this long, the merge writes the uint value of the tag.
     */
    private Object wireValue(String key, Object value) {
        if (value instanceof Long number && number < 0) {
            Object tagValue = tagValue(key);
            if (tagValue instanceof OpenLayer.UnsignedValue unsigned && unsigned.bits() == number) {
                return unsigned;
            }
        }
        return value;
    }

    /** Returns the value of the last tag of the key, as kept by the index of the layer, or null without one. */
    private Object tagValue(String key) {
        Object[] lastValue = new Object[1];
        forEachTag((tagKey, value) -> {
            if (tagKey.equals(key)) {
                lastValue[0] = value;
            }
        });
        return lastValue[0];
    }

    /**
     * Hands the key and the value of each tag to the action, in tile order. The tags come in pairs: the index of a key,
     * then the index of its value, in the tables of the layer. The values come as kept by the index of the layer: a
     * value without a known field is null.
     *
     * @throws IllegalArgumentException on an odd number of tags, or on a tag outside the keys or the values of the
     *     layer
     */
    private void forEachTag(BiConsumer<String, Object> action) {
        LayerIndex layerIndex = layer.index();
        VarintCursor cursor = tags();
        while (cursor.hasNext()) {
            int keyIndex = cursor.next();
            requireValueIndex(cursor);
            int valueIndex = cursor.next();
            action.accept(layerIndex.key(keyIndex), layerIndex.value(valueIndex));
        }
    }

    private static void requireValueIndex(VarintCursor tags) {
        if (!tags.hasNext()) {
            throw new IllegalArgumentException("The feature has an odd number of tags: " + tags.size());
        }
    }

    @Override
    public String toString() {
        return geometryType().toString() + GeometryDecoder.toString(geometryCommands()) + " (id: " + getId() + ")";
    }

    /** The last type of the feature from 0 to 3, or UNKNOWN when it has none. */
    GeomType geometryType() {
        return type;
    }

    /** The position of this feature in its layer. */
    int index() {
        return index;
    }

    /** The packed geometries of the layer of this feature. The geometry of this feature is at {@link #index()}. */
    PackedGeometries layerGeometries() {
        return layer.index().geometries();
    }

    /** Whether the geometry of this feature is malformed: it reads as empty, and its commands stay in the tile. */
    boolean hasMalformedGeometry() {
        return layer.index().hasMalformedGeometry(index);
    }

    /** Whether the geometry of this feature has a part once packed: its points, a line or a ring. */
    boolean hasGeometryParts() {
        return layerGeometries().partCount(index) > 0;
    }

    /**
     * Whether the first ring of this polygon has a positive area, as required of an exterior ring by version 2 of the
     * specification. The decoder takes the first ring as exterior whatever the sign of its area, and the rings of the
     * same sign as the exterior rings of the next polygons.
     */
    boolean startsWithExteriorRing() {
        PackedGeometries geometries = layerGeometries();
        if (geometries.partCount(index) == 0) {
            return false;
        }
        return geometries.ringArea(geometries.firstPart(index)) > 0;
    }

    /**
     * Returns a new cursor over a copy of the geometry commands of this feature, at the first command. The feature
     * finds its commands at each call: its geometry comes from the packed geometries of the layer, and only the tests
     * and {@link #toString()} read the commands.
     */
    VarintCursor geometryCommands() {
        return cursorOverCopiedRuns(FEATURE_GEOMETRY, FEATURE_UNPACKED_GEOMETRY);
    }

    /** The length of the field written by {@link #writeGeometryField}: 0 for a feature without geometry commands. */
    int geometryFieldLength() {
        int runsLength = geometryRunsLength();
        if (runsLength == 0) {
            return 0;
        }
        return WireWriter.lengthDelimitedFieldLength(OpenLayer.FEATURE_GEOMETRY_FIELD, runsLength);
    }

    /**
     * Writes the geometry commands of this feature as one packed field, as protobuf writes the field once parsed. The
     * runs of the field are copied from the tile bytes in tile order, without decoding them. Writes nothing for a
     * feature without geometry commands.
     */
    void writeGeometryField(WireWriter target) {
        int runsLength = geometryRunsLength();
        if (runsLength > 0) {
            target.writeFieldHeader(OpenLayer.FEATURE_GEOMETRY_FIELD, runsLength);
            appendGeometryRuns(target);
        }
    }

    /** The number of bytes in the runs of the geometry commands of this feature. */
    private int geometryRunsLength() {
        WireReader reader = fieldReader();
        int length = 0;
        int runStart = nextGeometryRun(reader);
        while (runStart != NO_RUN) {
            length += reader.position() - runStart;
            runStart = nextGeometryRun(reader);
        }
        return length;
    }

    private void appendGeometryRuns(WireWriter target) {
        WireReader reader = fieldReader();
        int runStart = nextGeometryRun(reader);
        while (runStart != NO_RUN) {
            target.appendBytes(tile(), runStart, reader.position());
            runStart = nextGeometryRun(reader);
        }
    }

    private static int nextGeometryRun(WireReader reader) {
        return nextRun(reader, FEATURE_GEOMETRY, FEATURE_UNPACKED_GEOMETRY);
    }

    private VarintCursor tags() {
        if (severalTagRuns) {
            return cursorOverCopiedRuns(FEATURE_TAGS, FEATURE_UNPACKED_TAGS);
        }
        return new VarintCursor(tile(), tagsStart, tagsEnd);
    }

    /** Returns a cursor over a copy of the varints of a field, in tile order. */
    private VarintCursor cursorOverCopiedRuns(int packedTag, int unpackedTag) {
        LayerIndex layerIndex = layer.index();
        int start = layerIndex.featureStart(index);
        int end = layerIndex.featureEnd(index);
        byte[] copy = copyRuns(tile(), start, end, packedTag, unpackedTag);
        return new VarintCursor(copy, 0, copy.length);
    }
}
