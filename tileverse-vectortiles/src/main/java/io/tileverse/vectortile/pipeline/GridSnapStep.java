/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.pipeline;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.precision.GeometryPrecisionReducer;

/**
 * Snaps the coordinates to the integer grid of the tile with the precision reducer of JTS, and removes the parts
 * collapsed by the snapping.
 *
 * <p>The grid has a node at each tile unit. For geometries in coordinates multiplied by a scale when they are rounded,
 * the nodes are at the multiples of {@code 1 / scale}.
 *
 * <p>Polygons are snap-rounded: a valid polygon stays valid, and polygons closer than one tile unit can merge. Lines
 * lose their repeated points. A line left with fewer than two points, or a ring with fewer than four, is removed. A
 * geometry collapsed entirely comes back empty, and the pipeline drops the feature. A geometry rejected by the reducer,
 * such as a self-crossing polygon, drops the feature too.
 */
public final class GridSnapStep implements GeometryStep {

    private final double scale;

    /** Shared by the threads running the step: each call to {@code reduce} creates its own transformer. */
    private final GeometryPrecisionReducer reducer;

    /** Creates the step snapping geometries in tile coordinates to the grid of one tile unit. */
    public GridSnapStep() {
        this(1);
    }

    /**
     * Creates the step snapping to the grid of one tile unit, for geometries in coordinates multiplied by the scale
     * when they are rounded.
     *
     * @param scale the number of tile units in one unit of the geometries
     * @throws IllegalArgumentException if the scale is not positive and finite
     */
    public GridSnapStep(double scale) {
        // The precision model of JTS takes a negative scale as a grid size
        this.scale = Parameters.requirePositiveAndFinite(scale, "scale");
        reducer = new GeometryPrecisionReducer(new PrecisionModel(scale));
        reducer.setRemoveCollapsedComponents(true);
    }

    @Override
    public Geometry apply(Geometry geometry) {
        try {
            return reducer.reduce(geometry);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    @Override
    public String toString() {
        return "GridSnapStep[scale=" + scale + "]";
    }
}
