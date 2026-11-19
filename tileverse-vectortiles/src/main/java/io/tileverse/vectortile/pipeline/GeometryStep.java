/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.pipeline;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;

/**
 * A change to the geometry of a feature before the feature is written, such as clipping or simplification.
 *
 * <p>A step receives a non-empty geometry in the coordinates given to the writer, never a {@link GeometryCollection} of
 * that exact class. These are tile coordinates, or coordinates in units of the input when the writer has an input
 * scale: the writer multiplies them by the scale after the steps, when it rounds them. The parameters of a step, such
 * as a tolerance or a minimum size, are in the units of its geometries. The steps working on the integer grid of the
 * tile take the scale of the geometries.
 *
 * <p>The result of a step goes through the remaining steps of the {@link GeometryPipeline}, and the result of the last
 * step is written. A null or empty result drops the feature. A {@code GeometryCollection} result is split, and its
 * members continue one by one through the remaining steps.
 *
 * <p>Implementations must be safe to share between threads: one pipeline can serve several tiles built at the same
 * time. A step cannot change the tile under construction: the writer fails when a step calls it to change the tile, the
 * pipeline or a setting.
 */
@FunctionalInterface
public interface GeometryStep {

    /**
     * Changes the geometry of a feature, or drops the feature.
     *
     * @param geometry a non-empty geometry in the coordinates given to the writer
     * @return the geometry for the remaining steps, or null to drop the feature
     */
    Geometry apply(Geometry geometry);
}
