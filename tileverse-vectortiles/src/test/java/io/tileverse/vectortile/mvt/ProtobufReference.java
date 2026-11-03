/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import io.tileverse.vectortile.model.VectorTile;
import io.tileverse.vectortile.model.VectorTile.Layer;
import io.tileverse.vectortile.model.VectorTile.Layer.Feature;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Reads the content of a tile with protobuf's parser, and through the model, for the tests comparing the decoder with
 * protobuf. The content is what a caller of the model reads: the name, the extent and the attribute names of each
 * layer, and the id, the type, the attributes and the geometry commands of each feature.
 */
final class ProtobufReference {

    /** The attributes of a feature with an odd number of tags, or with a tag outside the keys or the values. */
    static final String INVALID_TAGS = "invalid tags";

    record TileContent(List<LayerContent> layers) {}

    record LayerContent(String name, int extent, Set<String> attributeNames, List<FeatureContent> features) {}

    /** The attributes are a map, or {@link #INVALID_TAGS} when the tags cannot be read. */
    record FeatureContent(long id, GeomType type, Object attributes, List<Integer> geometry) {}

    private ProtobufReference() {
        // static methods only
    }

    /** Parses the tile with the generated protobuf classes, and reads the attributes as in earlier releases. */
    static TileContent parse(byte[] tile) throws IOException {
        VectorTileProto.Tile parsed = VectorTileProto.Tile.parser().parseFrom(tile);
        List<LayerContent> layers = new ArrayList<>();
        for (VectorTileProto.Tile.Layer layer : parsed.getLayersList()) {
            layers.add(parseLayer(layer));
        }
        return new TileContent(layers);
    }

    private static LayerContent parseLayer(VectorTileProto.Tile.Layer layer) {
        List<FeatureContent> features = new ArrayList<>();
        for (VectorTileProto.Tile.Feature feature : layer.getFeaturesList()) {
            features.add(parseFeature(layer, feature));
        }
        Set<String> attributeNames = Set.copyOf(layer.getKeysList());
        return new LayerContent(layer.getName(), layer.getExtent(), attributeNames, features);
    }

    private static FeatureContent parseFeature(VectorTileProto.Tile.Layer layer, VectorTileProto.Tile.Feature feature) {
        Object attributes = parseAttributes(layer, feature.getTagsList());
        List<Integer> geometry = List.copyOf(feature.getGeometryList());
        GeomType type = GeomType.forNumber(feature.getType().getNumber());
        return new FeatureContent(feature.getId(), type, attributes, geometry);
    }

    // The lists of protobuf throw IndexOutOfBoundsException on an odd number of tags or an index past a table
    private static Object parseAttributes(VectorTileProto.Tile.Layer layer, List<Integer> tags) {
        Map<String, Object> attributes = new HashMap<>();
        try {
            for (int i = 0; i < tags.size(); i += 2) {
                String key = layer.getKeys(tags.get(i));
                VectorTileProto.Tile.Value value = layer.getValues(tags.get(i + 1));
                attributes.put(key, parseValue(value));
            }
        } catch (IndexOutOfBoundsException e) {
            return INVALID_TAGS;
        }
        return attributes;
    }

    /** The first field present in this order: bool, double, float, int, sint, uint and string. */
    private static Object parseValue(VectorTileProto.Tile.Value value) {
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

    /** Reads the tile through the model, with the type and the geometry commands of each feature. */
    static TileContent read(VectorTile tile) {
        List<LayerContent> layers = new ArrayList<>();
        for (Layer layer : tile.getLayers()) {
            layers.add(readLayer(layer));
        }
        return new TileContent(layers);
    }

    private static LayerContent readLayer(Layer layer) {
        List<FeatureContent> features =
                layer.getFeatures().map(ProtobufReference::readFeature).toList();
        return new LayerContent(layer.getName(), layer.getExtent(), layer.getAttributeNames(), features);
    }

    private static FeatureContent readFeature(Feature feature) {
        MvtFeature mvtFeature = (MvtFeature) feature;
        Object attributes = readAttributes(feature);
        List<Integer> geometry = toList(mvtFeature.geometryCommands());
        return new FeatureContent(feature.getId(), mvtFeature.geometryType(), attributes, geometry);
    }

    private static Object readAttributes(Feature feature) {
        try {
            return new HashMap<>(feature.getAttributes());
        } catch (IllegalArgumentException e) {
            return INVALID_TAGS;
        }
    }

    private static List<Integer> toList(VarintCursor cursor) {
        List<Integer> values = new ArrayList<>(cursor.size());
        while (cursor.hasNext()) {
            values.add(cursor.next());
        }
        return values;
    }
}
