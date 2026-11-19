/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.pipeline;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.geom.util.GeometryFixer;
import org.locationtech.jts.precision.GeometryPrecisionReducer;

/**
 * Repairs the polygons invalid once their coordinates are rounded to integers, as a vector tile writer rounds them.
 *
 * <p>A {@code Polygon} or a {@code MultiPolygon} valid once rounded is returned as is, as are other geometries. The
 * step snaps an invalid one to the integer grid with the precision reducer of JTS. A polygon valid before rounding is
 * valid once snapped. If the snapped polygon is still invalid once rounded, the fixer of JTS repairs it. When the
 * reducer rejects a polygon invalid before rounding, such as a self-crossing one, the fixer repairs the polygon as
 * given. If the repair returns an empty or non-polygonal geometry, the step drops the feature.
 *
 * <p>For polygons in coordinates multiplied by a scale when they are rounded, the step checks them at that scale, as
 * {@link RoundedValidity} does, and snaps them to the grid of one tile unit, at the multiples of {@code 1 / scale}.
 *
 * <p>A call borrows its {@link RoundedValidity} from a pool held by the step: the pool grows to the number of calls
 * running at the same time, and no state is bound to the threads.
 */
public final class PolygonRepairStep implements GeometryStep {

    private final double scale;

    /** The grid of one tile unit at the scale of the polygons. */
    private final PrecisionModel grid;

    /** The checks not in use. A check keeps the coordinates grown by its earlier polygons for the next ones. */
    private final Queue<RoundedValidity> checks;

    /** Creates the step for polygons in tile coordinates. */
    public PolygonRepairStep() {
        this(1);
    }

    /**
     * Creates the step for polygons in coordinates multiplied by the scale when they are rounded.
     *
     * @param scale the number of tile units in one unit of the polygons
     * @throws IllegalArgumentException if the scale is not positive and finite
     */
    public PolygonRepairStep(double scale) {
        // The precision model of JTS takes a negative scale as a grid size
        this.scale = Parameters.requirePositiveAndFinite(scale, "scale");
        grid = new PrecisionModel(scale);
        checks = new ConcurrentLinkedQueue<>();
    }

    @Override
    public Geometry apply(Geometry geometry) {
        if (!(geometry instanceof Polygonal)) {
            return geometry;
        }
        RoundedValidity validity = takeCheck();
        try {
            return repairIfInvalid(geometry, validity);
        } finally {
            checks.offer(validity);
        }
    }

    /** Takes a check from the pool, or creates one when the calls running at the same time hold them all. */
    private RoundedValidity takeCheck() {
        RoundedValidity validity = checks.poll();
        if (validity == null) {
            return new RoundedValidity(scale);
        }
        return validity;
    }

    /** Returns the polygon if valid once rounded, else its repair, or null if the repair is empty or not polygonal. */
    private Geometry repairIfInvalid(Geometry polygon, RoundedValidity validity) {
        if (validity.isValid((Polygonal) polygon)) {
            return polygon;
        }
        Geometry repaired = repair(polygon, validity);
        if (repaired instanceof Polygonal && !repaired.isEmpty()) {
            return repaired;
        }
        return null;
    }

    /**
     * Snaps the polygon to the integer grid, and fixes the snapped polygon if it is still invalid once rounded. Fixes
     * the polygon as given if the reducer rejects it.
     */
    private Geometry repair(Geometry polygon, RoundedValidity validity) {
        Geometry snapped = snapToGrid(polygon);
        if (snapped == null) {
            return GeometryFixer.fix(polygon);
        }
        if (validity.isValid((Polygonal) snapped)) {
            return snapped;
        }
        return GeometryFixer.fix(snapped);
    }

    /**
     * Returns the polygon snapped to the integer grid: a polygon or a multipolygon, empty when it collapses. Returns
     * null if the reducer rejects the polygon.
     */
    private Geometry snapToGrid(Geometry polygon) {
        try {
            return GeometryPrecisionReducer.reduce(polygon, grid);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    @Override
    public String toString() {
        return "PolygonRepairStep[scale=" + scale + "]";
    }
}
