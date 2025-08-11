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

import io.tileverse.vectortile.model.Feature;
import io.tileverse.vectortile.model.Layer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

final class MvtFeature implements Feature {

    private static final Geometry UNSET = new GeometryFactory().createEmpty(0);

    final MvtLayer layer;
    final VectorTileProto.Tile.Feature feature;

    private Geometry geometry = UNSET;
    private Map<String, Object> valueIndex;

    public MvtFeature(MvtLayer layer, VectorTileProto.Tile.Feature feature) {
        this.layer = Objects.requireNonNull(layer, "layer");
        this.feature = Objects.requireNonNull(feature, "feature");
    }

    @Override
    public long getId() {
        return feature.getId();
    }

    @Override
    public Layer getLayer() {
        return layer;
    }

    @Override
    public Geometry getGeometry() {
        if (geometry == UNSET) {
            geometry = layer.decoder().decode(feature);
        }
        return geometry;
    }

    @Override
    public Object getAttribute(String attributeName) {
        return getAttributes().getOrDefault(attributeName, null);
    }

    @Override
    public Map<String, Object> getAttributes() {
        if (valueIndex == null) {
            valueIndex = buildValues();
        }
        return valueIndex;
    }

    @Override
    public String toString() {
        return feature.getType().toString() + GeometryDecoder.toString(this.feature.getGeometryList()) + " (id: "
                + getId() + ")";
    }

    private Map<String, Object> buildValues() {
        Map<String, Object> values = new HashMap<>();
        VectorTileProto.Tile.Layer tileLayer = this.layer.layer();
        List<Integer> tags = feature.getTagsList();
        for (int i = 0; i < tags.size(); i += 2) {
            int keyTag = tags.get(i);
            int valueTag = tags.get(i + 1);
            String attName = tileLayer.getKeys(keyTag);
            VectorTileProto.Tile.Value value = tileLayer.getValues(valueTag);
            values.put(attName, parseValue(value));
        }
        return values;
    }

    private Object parseValue(VectorTileProto.Tile.Value value) {
        if (value.hasBoolValue()) {
            return Boolean.valueOf(value.getBoolValue());
        } else if (value.hasDoubleValue()) {
            return Double.valueOf(value.getDoubleValue());
        } else if (value.hasFloatValue()) {
            return Float.valueOf(value.getFloatValue());
        } else if (value.hasIntValue()) {
            return Long.valueOf(value.getIntValue());
        } else if (value.hasSintValue()) {
            return Long.valueOf(value.getSintValue());
        } else if (value.hasUintValue()) {
            return Long.valueOf(value.getUintValue());
        } else if (value.hasStringValue()) {
            return value.getStringValue();
        }
        return null;
    }
}
