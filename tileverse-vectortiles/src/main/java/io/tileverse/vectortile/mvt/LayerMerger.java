/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import io.tileverse.vectortile.model.GeometryReader;
import io.tileverse.vectortile.model.VectorTile.Layer;
import io.tileverse.vectortile.model.VectorTile.Layer.Feature;
import io.tileverse.vectortile.pipeline.GeometryPipeline;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.locationtech.jts.geom.Geometry;

/**
 * Writes the features of a layer of another tile into the open layer, for {@link LayerBuilder#merge}. The features skip
 * the pipeline of the tile builder.
 *
 * <p>A decoded feature keeps the id written in its tile, also when a layer of another implementation hands it out. Its
 * attributes are read from its tags, or from the map of its attributes once built, with the changes made through the
 * map. When a layer of a decoded or built tile follows version 2 of the specification and has the extent of the tile
 * builder, the geometry fields of its features are copied from the tile bytes. Otherwise the geometries are decoded and
 * encoded again, as those of another implementation: version 1 sets no orientation for the rings, and another extent
 * needs the coordinates scaled. The encoder multiplies them by the ratio of the extents when it rounds them. A polygon
 * starting with a ring of negative area is encoded again too: version 2 wants a positive area for an exterior ring, and
 * the encoder orients the rings.
 *
 * <p>The tile builder has one merger, and the merger keeps its arrays from one feature to the next. It is not
 * thread-safe.
 */
final class LayerMerger {

    private final VectorTileBuilder tileBuilder;
    private final OpenLayer openLayer;
    private final GeometryReader decoder = new GeometryDecoder();
    private final CommandEncoder encoder = new CommandEncoder();
    private final IntArray tags = new IntArray();

    // Created once: a method reference in the loops would allocate an object per feature
    private final Consumer<Geometry> partWriter = this::writePart;
    private final BiConsumer<String, Object> tagWriter = this::addTag;

    // The id of the feature being merged
    private boolean hasId;
    private long id;

    // The largest id merged by the call, noted by the tile builder once the layer is merged. An id of 2^63 or above is
    // negative, and moves no automatic id.
    private long largestId;

    /** The number of tile units of the tile builder in one unit of the layer being merged. */
    private double layerScale;

    LayerMerger(VectorTileBuilder tileBuilder, OpenLayer openLayer) {
        this.tileBuilder = tileBuilder;
        this.openLayer = openLayer;
    }

    /**
     * Writes the features of the layer into the open layer. A feature without geometry commands, or with a geometry
     * read as empty, such as a malformed one, is left out. The layer is merged whole or not at all: when a feature
     * fails, also on an Error, the features written by this call are removed and the automatic ids stay as they were.
     * The exception goes to the caller. The keys and values added by the merged features stay in the tables of the open
     * layer: the layer stays valid, with these entries unused.
     */
    void merge(Layer layer) {
        WireWriter features = openLayer.features();
        int sizeBefore = features.size();
        largestId = FeatureBuilder.NO_ID;
        boolean completed = false;
        try {
            mergeFeatures(layer);
            completed = true;
        } finally {
            if (!completed) {
                features.truncate(sizeBefore);
            }
        }
        tileBuilder.noteFeatureId(largestId);
    }

    private void mergeFeatures(Layer layer) {
        if (layer instanceof MvtLayer decoded) {
            mergeDecodedLayer(decoded);
        } else {
            mergeModelLayer(layer);
        }
    }

    /** Merges the features of a layer of a decoded or built tile, read from the tile bytes. */
    private void mergeDecodedLayer(MvtLayer layer) {
        LayerIndex index = layer.index();
        boolean copiesGeometry = copiesGeometryOf(index);
        layerScale = scaleOf(index.extent());
        for (int i = 0; i < index.featureCount(); i++) {
            mergeDecodedFeature(new MvtFeature(layer, i, decoder), copiesGeometry);
        }
    }

    private boolean copiesGeometryOf(LayerIndex layer) {
        return layer.version() == OpenLayer.VERSION && layer.extent() == tileBuilder.extent();
    }

    /** Takes the id and the tags written in the tile, then copies the geometry field or encodes the geometry again. */
    private void mergeDecodedFeature(MvtFeature feature, boolean copiesGeometry) {
        takeIdAndTags(feature);
        if (copiesGeometry && !hasReversedRings(feature)) {
            writeWithGeometryField(feature);
        } else {
            writeGeometry(feature.getGeometry());
        }
    }

    /**
     * Whether the feature is a polygon without an exterior ring first, such as a polygon starting with a ring of
     * negative area: version 2 of the specification wants a positive area. Encoding it again orients its rings. A
     * polygon without rings is left out either way.
     */
    private static boolean hasReversedRings(MvtFeature feature) {
        return feature.geometryType() == GeomType.POLYGON && !feature.startsWithExteriorRing();
    }

    /** Merges the features of a layer of another implementation, read through the model. */
    private void mergeModelLayer(Layer layer) {
        layerScale = scaleOf(layer.getExtent());
        try (Stream<Feature> features = layer.getFeatures()) {
            features.forEach(this::mergeModelFeature);
        }
    }

    /**
     * Takes the id and the attributes of a feature of a layer of another implementation, then encodes the geometry
     * again. Such a layer can hand out decoded features, as a layer filtering the features of a decoded tile does: a
     * decoded feature keeps the id written in its tile, and its attributes come from
     * {@link MvtFeature#forEachAttribute}.
     */
    private void mergeModelFeature(Feature feature) {
        if (feature instanceof MvtFeature decoded) {
            takeIdAndTags(decoded);
        } else {
            takeIdAndAttributes(feature);
        }
        writeGeometry(feature.getGeometry());
    }

    /** Takes the id written in the tile of a decoded feature, of any value, and its attributes. */
    private void takeIdAndTags(MvtFeature feature) {
        takeId(feature.hasId(), feature.getId());
        takeTags(feature);
    }

    /**
     * Takes the id and the attributes read through the model. The model returns 0 for a feature without an id: 0 is
     * taken as none, and another value as an id. A negative id is written as its unsigned value, as for a decoded
     * feature: the model returns an id of 2^63 or above as a negative long.
     */
    private void takeIdAndAttributes(Feature feature) {
        long featureId = feature.getId();
        takeId(featureId != 0, featureId);
        takeAttributes(feature);
    }

    /**
     * Takes the id of the feature being merged. The largest id moves the automatic ids of the tile past it, once the
     * layer is merged. The id of a feature left out counts, as the id of a feature dropped by the pipeline does.
     */
    private void takeId(boolean hasId, long id) {
        this.hasId = hasId;
        this.id = id;
        if (hasId) {
            largestId = Math.max(largestId, id);
        }
    }

    private void takeTags(MvtFeature feature) {
        tags.clear();
        feature.forEachAttribute(tagWriter);
    }

    private void takeAttributes(Feature feature) {
        tags.clear();
        feature.getAttributes().forEach(tagWriter);
    }

    // A null value adds no tag, as with the feature builder
    private void addTag(String key, Object value) {
        openLayer.addTag(tags, key, value);
    }

    /**
     * Writes a decoded feature with the bytes of its geometry field, unless the geometry reads as empty: without
     * commands, malformed, or without parts, such as a MoveTo without points. Encoding the geometry again would leave
     * such a feature out.
     */
    private void writeWithGeometryField(MvtFeature feature) {
        if (hasGeometryToCopy(feature)) {
            openLayer.writeFeature(hasId, id, tags, feature);
        }
    }

    // A geometry of unknown type is read without parts: its well-formed commands are copied as they are
    private static boolean hasGeometryToCopy(MvtFeature feature) {
        if (feature.geometryFieldLength() == 0 || feature.hasMalformedGeometry()) {
            return false;
        }
        return feature.geometryType() == GeomType.UNKNOWN || feature.hasGeometryParts();
    }

    /**
     * Writes the geometry as a feature with the id and the tags taken, at the scale of the layer. The pipeline without
     * steps splits a {@code GeometryCollection} into its members, one feature each.
     */
    private void writeGeometry(Geometry geometry) {
        GeometryPipeline.empty().run(geometry, partWriter);
    }

    /** Writes a geometry as a feature with the id and the tags taken, unless it has no commands once rounded. */
    private void writePart(Geometry part) {
        if (encoder.encode(part, layerScale)) {
            openLayer.writeFeature(hasId, id, tags, encoder);
        }
    }

    /**
     * Returns the number of tile units of the tile builder in one unit of a layer of the extent: 1 for the extent of
     * the tile builder.
     *
     * @throws IllegalArgumentException if the extent is below 1, as read from a malformed tile: a negative scale would
     *     mirror the coordinates
     */
    private double scaleOf(int sourceExtent) {
        int extent = tileBuilder.extent();
        if (sourceExtent == extent) {
            return 1;
        }
        if (sourceExtent < 1) {
            throw new IllegalArgumentException(
                    "Cannot merge a layer of extent " + sourceExtent + ": the extent of a layer is at least 1");
        }
        return extent / (double) sourceExtent;
    }

    /**
     * Estimates the heap memory held by the arrays of this merger, in bytes: the tags and the geometry commands of a
     * feature, by capacity. The arrays are kept from one feature to the next.
     */
    long heapSize() {
        IntArray commands = encoder.commands();
        return HeapSize.array(Integer.BYTES, tags.capacity()) + HeapSize.array(Integer.BYTES, commands.capacity());
    }
}
