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
import static io.tileverse.vectortile.mvt.LayerIndex.copyRuns;
import static io.tileverse.vectortile.mvt.LayerIndex.readGeomType;
import static io.tileverse.vectortile.mvt.LayerIndex.skipRun;
import static java.util.Objects.requireNonNull;

import io.tileverse.vectortile.model.GeometryReader;
import io.tileverse.vectortile.model.VectorTile;
import io.tileverse.vectortile.model.VectorTile.Layer;
import java.util.HashMap;
import java.util.Map;
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
                case FEATURE_ID -> id = reader.readVarint64();
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

    /** Reads the tags in pairs: the index of a key, then the index of its value, in the tables of the layer. */
    private Map<String, Object> readAttributes() {
        LayerIndex layerIndex = layer.index();
        VarintCursor cursor = tags();
        Map<String, Object> values = new HashMap<>();
        while (cursor.hasNext()) {
            int keyIndex = cursor.next();
            requireValueIndex(cursor);
            int valueIndex = cursor.next();
            values.put(layerIndex.key(keyIndex), layerIndex.value(valueIndex));
        }
        return values;
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

    /**
     * Returns a new cursor over a copy of the geometry commands of this feature, at the first command. The feature
     * finds its commands at each call: its geometry comes from the packed geometries of the layer, and only the tests
     * and {@link #toString()} read the commands.
     */
    VarintCursor geometryCommands() {
        return cursorOverCopiedRuns(FEATURE_GEOMETRY, FEATURE_UNPACKED_GEOMETRY);
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
