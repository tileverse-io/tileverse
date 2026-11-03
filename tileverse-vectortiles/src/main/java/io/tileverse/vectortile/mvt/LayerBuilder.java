/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import io.tileverse.vectortile.mvt.VectorTileBuilder.BuildParams;
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;
import org.locationtech.jts.geom.Geometry;

/**
 * Builder for creating vector tile layers with features.
 *
 * <p>Manages feature collection and key/value deduplication for efficient encoding, and encodes the layer in the MVT
 * wire format.
 */
public class LayerBuilder {

    /** The version of the vector tile specification followed by the built layers. */
    private static final int VERSION = 2;

    // Field numbers of the Layer and Value messages of the vector tile schema
    private static final int LAYER_NAME = 1;
    private static final int LAYER_FEATURES = 2;
    private static final int LAYER_KEYS = 3;
    private static final int LAYER_VALUES = 4;
    private static final int LAYER_EXTENT = 5;
    private static final int LAYER_VERSION = 15;
    private static final int VALUE_STRING = 1;
    private static final int VALUE_FLOAT = 2;
    private static final int VALUE_DOUBLE = 3;
    private static final int VALUE_SINT = 6;
    private static final int VALUE_BOOL = 7;

    final VectorTileBuilder vectorTileBuilder;
    private String layerName;

    // Key/value deduplication maps
    private final Map<String, Integer> keys = new LinkedHashMap<>();
    private final Map<Object, Integer> values = new LinkedHashMap<>();

    // Built features ready for encoding, each written as a field of the layer
    private final WireWriter features = new WireWriter();

    // Reusable feature builder to avoid object creation overhead
    private final FeatureBuilder reusableFeatureBuilder;

    LayerBuilder(VectorTileBuilder vectorTileBuilder) {
        this.vectorTileBuilder = vectorTileBuilder;
        this.reusableFeatureBuilder = new FeatureBuilder(this);
    }

    /**
     * Set the layer name.
     *
     * @param layerName a {@link String} with the vector tile layer name.
     * @return this LayerBuilder for method chaining
     */
    public LayerBuilder name(String layerName) {
        this.layerName = layerName;
        return this;
    }

    /**
     * Get a reusable feature builder for this layer. The builder is reset for each new feature to avoid object creation
     * overhead.
     *
     * @return a reusable FeatureBuilder instance
     */
    public FeatureBuilder feature() {
        return reusableFeatureBuilder.reset(params().getNextFeatureId());
    }

    /**
     * Convenience method to add a feature with properties and geometry.
     *
     * @param properties map of attribute names to values
     * @param geom the feature geometry
     * @return this LayerBuilder for method chaining
     */
    public LayerBuilder feature(Map<String, Object> properties, Geometry geom) {
        FeatureBuilder fb = feature();
        return fb.attributes(properties).geometry(geom).build();
    }

    /**
     * Convenience method to add a feature with properties, geometry, and explicit ID.
     *
     * @param properties map of attribute names to values
     * @param geom the feature geometry
     * @param id the feature identifier
     * @return this LayerBuilder for method chaining
     */
    public LayerBuilder feature(Map<String, Object> properties, Geometry geom, long id) {
        FeatureBuilder fb = feature();
        return fb.id(id).attributes(properties).geometry(geom).build();
    }

    /**
     * Complete building this layer and add it directly to the parent VectorTileBuilder.
     *
     * @return the parent VectorTileBuilder
     */
    public VectorTileBuilder build() {
        if (layerName == null || layerName.isEmpty()) {
            throw new IllegalStateException("Layer name must be set");
        }
        if (!features.isEmpty()) {
            vectorTileBuilder.addLayer(encodeLayer());
        }
        return vectorTileBuilder;
    }

    /** Writes the fields of the layer in the order of their field numbers, as protobuf does. */
    private WireWriter encodeLayer() {
        WireWriter layer = new WireWriter();
        layer.writeString(LAYER_NAME, layerName);
        layer.append(features);
        writeKeys(layer);
        writeValues(layer);
        layer.writeVarint32(LAYER_EXTENT, params().getExtent());
        layer.writeVarint32(LAYER_VERSION, VERSION);
        return layer;
    }

    private void writeKeys(WireWriter layer) {
        for (String key : keys.keySet()) {
            layer.writeString(LAYER_KEYS, key);
        }
    }

    private void writeValues(WireWriter layer) {
        WireWriter encodedValue = new WireWriter();
        for (Object value : values.keySet()) {
            encodedValue.clear();
            encodeValue(value, encodedValue);
            layer.writeMessage(LAYER_VALUES, encodedValue);
        }
    }

    /** Writes the value in the field of the Value message for its type. */
    private static void encodeValue(Object value, WireWriter encodedValue) {
        if (value instanceof String string) {
            encodedValue.writeString(VALUE_STRING, string);
        } else if (value instanceof Integer integer) {
            encodedValue.writeVarint64(VALUE_SINT, encodeZigZag64(integer.intValue()));
        } else if (value instanceof Long long1) {
            encodedValue.writeVarint64(VALUE_SINT, encodeZigZag64(long1.longValue()));
        } else if (value instanceof Float float1) {
            encodedValue.writeFixed32(VALUE_FLOAT, Float.floatToRawIntBits(float1.floatValue()));
        } else if (value instanceof Double double1) {
            encodedValue.writeFixed64(VALUE_DOUBLE, Double.doubleToRawLongBits(double1.doubleValue()));
        } else if (value instanceof BigDecimal) {
            encodedValue.writeString(VALUE_STRING, value.toString());
        } else if (value instanceof Number number) {
            encodedValue.writeFixed64(VALUE_DOUBLE, Double.doubleToRawLongBits(number.doubleValue()));
        } else if (value instanceof Boolean boolean1) {
            encodedValue.writeVarint32(VALUE_BOOL, boolean1.booleanValue() ? 1 : 0);
        } else {
            encodedValue.writeString(VALUE_STRING, value.toString());
        }
    }

    // A sint64 field holds the zigzag encoding of the value: values of either sign near zero take few bytes
    private static long encodeZigZag64(long value) {
        return (value << 1) ^ (value >> 63);
    }

    /**
     * Get or create a key index for attribute deduplication.
     *
     * @param key the attribute key
     * @return the index of the key in the keys array
     */
    Integer getKeyIndex(String key) {
        return keys.computeIfAbsent(key, k -> keys.size());
    }

    /**
     * Get or create a value index for attribute deduplication.
     *
     * @param value the attribute value
     * @return the index of the value in the values array
     */
    Integer getValueIndex(Object value) {
        return values.computeIfAbsent(value, v -> values.size());
    }

    /** Internal method for FeatureBuilder to add completed features. */
    void addFeature(WireWriter feature) {
        features.writeMessage(LAYER_FEATURES, feature);
    }

    BuildParams params() {
        return vectorTileBuilder.params();
    }
}
