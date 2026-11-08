/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import io.tileverse.vectortile.model.VectorTile;
import java.util.Map;
import org.locationtech.jts.geom.Geometry;

/**
 * Builder of a layer of a tile: its name and its features.
 *
 * <p>A tile is written one layer at a time. The features of a layer are added between {@link VectorTileBuilder#layer()}
 * and {@link #build()}, and the build writes the layer into the tile. Once the layer is built, or the next layer
 * started, the layer builder is closed and its methods fail with an {@link IllegalStateException}. A name is written
 * once per tile: {@link #name(String)} rejects the name of a layer written before. A layer without features is left
 * out, and leaves its name free.
 *
 * <p>The geometries go through the pipeline of the tile builder before they are written. The features taken from the
 * layers of other tiles with {@link #merge(VectorTile.Layer)} skip it. The features of the layer share its tables of
 * keys and values. A layer builder is not thread-safe.
 */
public class LayerBuilder {

    private final VectorTileBuilder tileBuilder;
    private final FeatureBuilder featureBuilder;
    private String layerName;
    private boolean open = true;

    LayerBuilder(VectorTileBuilder tileBuilder) {
        this.tileBuilder = tileBuilder;
        this.featureBuilder = tileBuilder.newFeatureBuilder(this);
    }

    /**
     * Set the layer name.
     *
     * @param layerName a {@link String} with the vector tile layer name.
     * @return this LayerBuilder for method chaining
     * @throws IllegalArgumentException if the tile holds a layer of this name, or if the name has an unpaired
     *     surrogate: the name is written in UTF-8
     * @throws IllegalStateException if this layer builder is closed
     */
    public LayerBuilder name(String layerName) {
        requireOpen();
        VectorTileBuilder.requireWellFormedName(layerName);
        tileBuilder.requireNameNotWritten(layerName);
        this.layerName = layerName;
        return this;
    }

    /**
     * Starts a feature of this layer. The layer builder has one feature builder, reset for each feature to avoid object
     * creation overhead.
     *
     * @return the feature builder, without id, attributes or geometry
     * @throws IllegalStateException if this layer builder is closed
     */
    public FeatureBuilder feature() {
        requireOpen();
        return featureBuilder.reset();
    }

    /**
     * Convenience method to add a feature with properties and geometry.
     *
     * @param properties map of attribute names to values
     * @param geom the feature geometry
     * @return this LayerBuilder for method chaining
     * @throws IllegalStateException if this layer builder is closed, or if no automatic id is left: see
     *     {@link FeatureBuilder#build()}
     */
    public LayerBuilder feature(Map<String, Object> properties, Geometry geom) {
        FeatureBuilder feature = feature();
        return feature.attributes(properties).geometry(geom).build();
    }

    /**
     * Convenience method to add a feature with properties, geometry, and explicit ID.
     *
     * @param properties map of attribute names to values
     * @param geom the feature geometry
     * @param id the feature identifier, or a negative value for none
     * @return this LayerBuilder for method chaining
     * @throws IllegalStateException if this layer builder is closed, or if the id is negative and no automatic id is
     *     left: see {@link FeatureBuilder#build()}
     */
    public LayerBuilder feature(Map<String, Object> properties, Geometry geom, long id) {
        FeatureBuilder feature = feature();
        return feature.id(id).attributes(properties).geometry(geom).build();
    }

    /**
     * Adds the features of a layer of another tile to this layer, in their order, without the steps of the pipeline.
     * The features keep their ids and attributes, and a feature without an id stays without one, also with automatic
     * ids on. A merged id moves the automatic ids of the tile past it, also when its feature is left out, as the id of
     * a feature dropped by the pipeline does. The geometries are written in the extent of the tile builder: a layer of
     * another extent has its coordinates scaled, then rounded. A feature without geometry commands, or with a geometry
     * read as empty, such as a malformed one, is left out. Several layers can be merged into this layer, such as the
     * layers of one name in several tiles.
     *
     * <p>A feature of a tile decoded by {@link VectorTileCodec} or built by a {@link VectorTileBuilder} keeps its id as
     * written, the id 0 included, also when a layer of another implementation hands it out. An id of 2^63 or above is
     * written as it is, and leaves the automatic ids as they are: they stay below 2^63. When a layer of such a tile
     * follows version 2 of the specification and has the extent of the tile builder, the geometry bytes of its features
     * are copied as they are. A polygon starting with a ring of negative area is encoded again: version 2 wants a
     * positive area for an exterior ring. The features of another {@link VectorTile} implementation are read through
     * the model. The model returns 0 for a feature without an id: a feature with the id 0 is written without an id, and
     * another id is written as its unsigned value. A negative id stands for an id of 2^63 or above, as for a decoded
     * feature.
     *
     * <p>The layer is merged whole or not at all: when a feature fails, for example on tags outside the tables of its
     * layer or on a NaN ordinate, the features merged by this call are removed and the automatic ids stay as they were.
     * The exception goes to the caller. The keys and values added by the merged features stay in the tables of this
     * layer: the layer stays valid, with these entries unused.
     *
     * @param layer the layer whose features are added to this layer
     * @return this LayerBuilder for method chaining
     * @throws IllegalStateException if this layer builder is closed. Also when the layer calls a builder of the tile to
     *     change it while its features are merged.
     */
    public LayerBuilder merge(VectorTile.Layer layer) {
        requireOpen();
        tileBuilder.mergeLayer(layer);
        return this;
    }

    /**
     * Writes this layer into the tile when it has features, and closes this layer builder. A layer without features is
     * left out of the tile, and a later layer can take its name.
     *
     * @return the parent VectorTileBuilder
     * @throws IllegalStateException if the name is not set, or if this layer builder is closed
     */
    public VectorTileBuilder build() {
        requireOpen();
        if (!hasName()) {
            throw new IllegalStateException("Layer name must be set");
        }
        tileBuilder.writeLayer(layerName);
        open = false;
        return tileBuilder;
    }

    /**
     * Fails if this layer builder is closed, or while the tile builder runs code of the caller, such as a step of the
     * pipeline: the step would change the feature being written.
     *
     * @throws IllegalStateException if the layer is built, or the next layer started, or if code run by the tile
     *     builder calls this layer builder or its feature builder
     */
    void requireOpen() {
        tileBuilder.requireNoCallback();
        if (!open) {
            throw new IllegalStateException(
                    "The builder of " + description() + " is closed: the layer is built, or the next layer started");
        }
    }

    /** Closes this layer builder without writing its layer. */
    void close() {
        open = false;
    }

    /** The layer as named by the messages: "layer roads", or "the unnamed layer" until the name is set. */
    String description() {
        return hasName() ? "layer " + layerName : "the unnamed layer";
    }

    private boolean hasName() {
        return layerName != null && !layerName.isEmpty();
    }
}
