/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.vectortile.mvt;

import io.tileverse.vectortile.mvt.VectorTileBuilder.BuildParams;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.locationtech.jts.geom.Geometry;

public class LayerBuilder {

    final VectorTileBuilder vectorTileBuilder;
    private String layerName;

    // Key/value deduplication maps
    private final Map<String, Integer> keys = new LinkedHashMap<>();
    private final Map<Object, Integer> values = new LinkedHashMap<>();

    // Built features ready for encoding
    private final List<VectorTileProto.Tile.Feature> features = new ArrayList<>();

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
     * Get a reusable feature builder for this layer.
     * The builder is reset for each new feature to avoid object creation overhead.
     *
     * @return a reusable FeatureBuilder instance
     */
    public FeatureBuilder feature() {
        return reusableFeatureBuilder.reset(params().getNextFeatureId());
    }

    public LayerBuilder feature(Map<String, Object> properties, Geometry geom) {
        FeatureBuilder fb = feature();
        return fb.attributes(properties).geometry(geom).build();
    }

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
        VectorTileProto.Tile.Layer builtLayer = buildLayer();
        vectorTileBuilder.addLayer(builtLayer);
        return vectorTileBuilder;
    }

    /**
     * Get or create a key index for attribute deduplication.
     *
     * @param key the attribute key
     * @return the index of the key in the keys array
     */
    Integer getKeyIndex(String key) {
        Integer index = keys.get(key);
        if (index == null) {
            index = keys.size();
            keys.put(key, index);
        }
        return index;
    }

    /**
     * Get or create a value index for attribute deduplication.
     *
     * @param value the attribute value
     * @return the index of the value in the values array
     */
    Integer getValueIndex(Object value) {
        Integer index = values.get(value);
        if (index == null) {
            index = values.size();
            values.put(value, index);
        }
        return index;
    }

    /**
     * Internal method for FeatureBuilder to add completed features.
     */
    void addFeature(VectorTileProto.Tile.Feature feature) {
        if (feature != null) {
            features.add(feature);
        }
    }

    /**
     * Build the protobuf layer structure.
     * This is called internally when build() is called.
     *
     * @return the built VectorTile.Tile.Layer or null if no valid features
     */
    private VectorTileProto.Tile.Layer buildLayer() {
        if (layerName == null || layerName.isEmpty()) {
            throw new IllegalStateException("Layer name must be set");
        }

        if (features.isEmpty()) {
            return null; // No features to encode
        }

        VectorTileProto.Tile.Layer.Builder layerBuilder = VectorTileProto.Tile.Layer.newBuilder();

        layerBuilder.setVersion(2);
        layerBuilder.setName(layerName);
        layerBuilder.setExtent(params().getExtent());

        // Add all keys
        for (String key : keys.keySet()) {
            layerBuilder.addKeys(key);
        }

        // Add all values with proper type encoding
        for (Object value : values.keySet()) {
            VectorTileProto.Tile.Value.Builder valueBuilder = VectorTileProto.Tile.Value.newBuilder();
            encodeValue(value, valueBuilder);
            VectorTileProto.Tile.Value encodedValue = valueBuilder.build();
            layerBuilder.addValues(encodedValue);
        }

        // Add all pre-built features
        for (VectorTileProto.Tile.Feature feature : features) {
            layerBuilder.addFeatures(feature);
        }

        return layerBuilder.build();
    }

    /**
     * Encode a value into the protobuf Value structure with proper type handling.
     */
    private void encodeValue(Object value, VectorTileProto.Tile.Value.Builder valueBuilder) {
        if (value instanceof String string) {
            valueBuilder.setStringValue(string);
        } else if (value instanceof Integer integer) {
            valueBuilder.setSintValue(integer.intValue());
        } else if (value instanceof Long long1) {
            valueBuilder.setSintValue(long1.longValue());
        } else if (value instanceof Float float1) {
            valueBuilder.setFloatValue(float1.floatValue());
        } else if (value instanceof Double double1) {
            valueBuilder.setDoubleValue(double1.doubleValue());
        } else if (value instanceof BigDecimal) {
            valueBuilder.setStringValue(value.toString());
        } else if (value instanceof Number number) {
            valueBuilder.setDoubleValue(number.doubleValue());
        } else if (value instanceof Boolean boolean1) {
            valueBuilder.setBoolValue(boolean1.booleanValue());
        } else {
            valueBuilder.setStringValue(value.toString());
        }
    }

    BuildParams params() {
        return vectorTileBuilder.params();
    }
}
