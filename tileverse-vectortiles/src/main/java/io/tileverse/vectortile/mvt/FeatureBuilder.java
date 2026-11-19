/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import org.locationtech.jts.geom.Geometry;

/**
 * Builder of a feature of the open layer: an id, attributes and a geometry. The geometry is in tile coordinates, or in
 * units of the input when the tile builder has an {@link VectorTileBuilder#setInputScale(double) input scale}: the tile
 * builder multiplies the coordinates by the scale when it rounds them.
 *
 * <p>{@link #build()} runs the geometry through the {@link VectorTileBuilder#pipeline() pipeline} of the tile builder
 * and writes the result into the layer. A {@code GeometryCollection} gives one feature per member, each with the id and
 * the attributes of the feature. A geometry dropped by a step, or left without commands once rounded, gives no feature.
 *
 * <p>A tile is written one layer at a time, and the feature builder writes into the layer of the
 * {@link LayerBuilder#feature()} call that returned it: it fails once that layer is built or the next layer started. A
 * layer builder has one feature builder, reset for each feature. It is not thread-safe.
 */
public class FeatureBuilder {

    /** The id of a feature without one. The feature builder writes no negative id. */
    static final long NO_ID = -1;

    private final VectorTileBuilder tileBuilder;
    private final OpenLayer openLayer;
    private final LayerBuilder layerBuilder;

    // The arrays of the tile builder, shared by the feature builders of its layers. The tags are the indexes of the
    // keys and values of the feature in the tables of the layer.
    private final CommandEncoder encoder;
    private final IntArray tags;

    // Created once: a method reference in build() would allocate an object per call
    private final Consumer<Geometry> featureWriter = this::writeFeature;

    // The feature being built
    private long givenId;
    private Geometry geometry;

    /** The id written with each geometry of the feature, decided at build(), or NO_ID. */
    private long featureId;

    FeatureBuilder(
            VectorTileBuilder tileBuilder,
            OpenLayer openLayer,
            LayerBuilder layerBuilder,
            CommandEncoder encoder,
            IntArray tags) {
        this.tileBuilder = tileBuilder;
        this.openLayer = openLayer;
        this.layerBuilder = layerBuilder;
        this.encoder = encoder;
        this.tags = tags;
    }

    /** Starts a feature of the layer: no id, no attributes, no geometry. */
    FeatureBuilder reset() {
        givenId = NO_ID;
        tags.clear();
        geometry = null;
        return this;
    }

    /**
     * Sets the feature identifier. A negative id means no id: the feature gets the next automatic id when automatic ids
     * are on, and is written without an id otherwise.
     *
     * @param id the feature ID
     * @return this FeatureBuilder for method chaining
     * @throws IllegalStateException if the layer of this feature is built, or the next layer started
     * @see VectorTileBuilder#setAutoIncrementIds(boolean)
     */
    public FeatureBuilder id(long id) {
        layerBuilder.requireOpen();
        givenId = id;
        return this;
    }

    /**
     * Sets the geometry of the feature, in tile coordinates, or in units of the input when the tile builder has an
     * {@link VectorTileBuilder#setInputScale(double) input scale}. The geometry goes through the pipeline as given at
     * {@link #build()}, and a {@code GeometryCollection} gives one feature per member. The tile builder multiplies the
     * coordinates by the scale after the pipeline, when it rounds them.
     *
     * @param geometry a {@link Geometry} for the vector tile feature.
     * @return this FeatureBuilder for method chaining
     * @throws IllegalStateException if the layer of this feature is built, or the next layer started
     */
    public FeatureBuilder geometry(Geometry geometry) {
        layerBuilder.requireOpen();
        this.geometry = geometry;
        return this;
    }

    /**
     * Adds an attribute to this feature. The key and the value join the tables of the layer, shared by its features. A
     * value failing to join them, for example when its {@code toString()} throws, leaves the feature as it was.
     *
     * @param name the attribute name
     * @param value the attribute value (null values are ignored)
     * @return this FeatureBuilder for method chaining
     * @throws NullPointerException if the name is null
     * @throws IllegalStateException if the layer of this feature is built, or the next layer started
     */
    public FeatureBuilder attribute(String name, Object value) {
        Objects.requireNonNull(name, "The name of an attribute cannot be null");
        layerBuilder.requireOpen();
        openLayer.addTag(tags, name, value);
        return this;
    }

    /**
     * Adds multiple attributes to this feature from a map.
     *
     * @param attributes map of attribute names to values
     * @return this FeatureBuilder for method chaining
     * @throws IllegalStateException if the layer of this feature is built, or the next layer started
     */
    public FeatureBuilder attributes(Map<String, Object> attributes) {
        layerBuilder.requireOpen();
        attributes.forEach(this::attribute);
        return this;
    }

    /**
     * Runs the geometry through the pipeline and writes the result into the layer, with the id and the attributes of
     * this builder. A feature without an id gets the next automatic id here, and a given id moves the automatic ids
     * past it, also when the pipeline drops the feature.
     *
     * <p>A feature is written whole or not at all. When a step or the encoding fails, for example on a NaN ordinate,
     * the members of a collection written before the failure are removed from the layer, and the automatic ids stay as
     * they were. The exception goes to the caller. The keys and values added by the feature stay in the tables of the
     * layer: the layer stays valid, with these entries unused.
     *
     * @return the layer builder to either build it or continue adding features
     * @throws IllegalStateException if the layer of this feature is built, or the next layer started, or if the feature
     *     has no id and no automatic id is left: see {@link VectorTileBuilder#setAutoIncrementIds(boolean)}. Also when
     *     a step of the pipeline calls a builder of the tile to change it.
     */
    public LayerBuilder build() {
        layerBuilder.requireOpen();
        featureId = decideId();
        runPipeline();
        tileBuilder.noteFeatureId(featureId);
        return layerBuilder;
    }

    /** Returns the given id, or the next automatic id when none is given. */
    private long decideId() {
        if (givenId < 0) {
            return tileBuilder.nextFeatureId();
        }
        return givenId;
    }

    /** Runs the pipeline on the geometry, and removes what it wrote into the layer when it fails, also on an Error. */
    private void runPipeline() {
        WireWriter features = openLayer.features();
        int sizeBefore = features.size();
        boolean completed = false;
        try {
            tileBuilder.runPipeline(geometry, featureWriter);
            completed = true;
        } finally {
            if (!completed) {
                features.truncate(sizeBefore);
            }
        }
    }

    /** Writes a geometry returned by the pipeline as a feature of the layer, unless it has no commands once rounded. */
    private void writeFeature(Geometry part) {
        if (encoder.encode(part, tileBuilder.inputScale())) {
            openLayer.writeFeature(hasId(), featureId, tags, encoder);
        }
    }

    private boolean hasId() {
        return featureId >= 0;
    }
}
