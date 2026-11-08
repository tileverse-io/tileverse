/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.pipeline;

import java.util.List;
import java.util.function.Consumer;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;

/**
 * An immutable sequence of {@link GeometryStep}s, run in order on the geometry of each feature before the feature is
 * written.
 *
 * <p>A null or empty geometry drops the feature: the remaining steps and the sink never see it. A
 * {@link GeometryCollection} of that exact class is split, whether it is given to {@link #run(Geometry, Consumer)} or
 * returned by a step. Its members continue one by one through the remaining steps.
 *
 * <p>A pipeline is safe to share between threads, as its steps must be.
 */
public final class GeometryPipeline {

    private static final GeometryPipeline EMPTY = new GeometryPipeline(List.of());

    private final List<GeometryStep> steps;

    private GeometryPipeline(List<GeometryStep> steps) {
        this.steps = steps;
    }

    /**
     * Returns a pipeline running the steps in the given order.
     *
     * @throws NullPointerException if a step is null
     */
    public static GeometryPipeline of(GeometryStep... steps) {
        return new GeometryPipeline(List.of(steps));
    }

    /**
     * Returns the pipeline without steps. It hands the geometries to the sink as they are, once null and empty
     * geometries are dropped and collections are split.
     */
    public static GeometryPipeline empty() {
        return EMPTY;
    }

    /** The steps in order of execution, as an unmodifiable list. */
    public List<GeometryStep> steps() {
        return steps;
    }

    /**
     * Runs the steps on the geometry of a feature and hands the results to the sink. The geometry is in the coordinates
     * given to the writer: tile coordinates, or coordinates in units of the input when the writer has an input scale.
     * The steps work in these units, and the writer multiplies the coordinates by the scale after them.
     *
     * @param geometry the geometry of the feature in the coordinates given to the writer, or null
     * @param sink receives the geometries to write, in order: none when the feature is dropped, one for each member of
     *     a split collection
     */
    public void run(Geometry geometry, Consumer<? super Geometry> sink) {
        runFrom(0, geometry, sink);
    }

    private void runFrom(int stepIndex, Geometry geometry, Consumer<? super Geometry> sink) {
        if (geometry == null || geometry.isEmpty()) {
            return;
        }
        if (isCollectionOfExactClass(geometry)) {
            runMembersFrom(stepIndex, (GeometryCollection) geometry, sink);
        } else if (stepIndex == steps.size()) {
            sink.accept(geometry);
        } else {
            Geometry result = steps.get(stepIndex).apply(geometry);
            runFrom(stepIndex + 1, result, sink);
        }
    }

    private void runMembersFrom(int stepIndex, GeometryCollection collection, Consumer<? super Geometry> sink) {
        int memberCount = collection.getNumGeometries();
        for (int i = 0; i < memberCount; i++) {
            runFrom(stepIndex, collection.getGeometryN(i), sink);
        }
    }

    /** Whether the geometry is a plain collection: a MultiPoint, a MultiLineString or a MultiPolygon is not. */
    private static boolean isCollectionOfExactClass(Geometry geometry) {
        return geometry.getClass() == GeometryCollection.class;
    }

    @Override
    public String toString() {
        return "GeometryPipeline" + steps;
    }
}
