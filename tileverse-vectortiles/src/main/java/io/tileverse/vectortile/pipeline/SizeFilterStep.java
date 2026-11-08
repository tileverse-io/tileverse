/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.pipeline;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Polygonal;

/**
 * Drops the features with a small polygon or a short line.
 *
 * <p>A {@code Polygon} or a {@code MultiPolygon} is dropped when its area is below the minimum area. A
 * {@code LineString}, a {@code LinearRing} included, is dropped when its length is below the minimum length. Other
 * geometries pass, {@code MultiLineString}s included, as in java-vector-tile. Areas and lengths are in the units of the
 * geometries.
 */
public final class SizeFilterStep implements GeometryStep {

    private final double minimumLength;
    private final double minimumArea;

    /**
     * Creates a filter with the given minimums, in the units of the geometries.
     *
     * @param minimumLength the shortest length kept for a {@code LineString}
     * @param minimumArea the smallest area kept for a {@code Polygon} or a {@code MultiPolygon}
     */
    public SizeFilterStep(double minimumLength, double minimumArea) {
        this.minimumLength = minimumLength;
        this.minimumArea = minimumArea;
    }

    @Override
    public Geometry apply(Geometry geometry) {
        if (isSmallPolygon(geometry) || isShortLine(geometry)) {
            return null;
        }
        return geometry;
    }

    private boolean isSmallPolygon(Geometry geometry) {
        return geometry instanceof Polygonal && geometry.getArea() < minimumArea;
    }

    private boolean isShortLine(Geometry geometry) {
        return geometry instanceof LineString && geometry.getLength() < minimumLength;
    }

    @Override
    public String toString() {
        return "SizeFilterStep[minimumLength=" + minimumLength + ", minimumArea=" + minimumArea + "]";
    }
}
