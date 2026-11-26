/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateArrays;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.util.GeometryEditor;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

/** Geometry helpers for the tests of the writer and of its pipeline. */
public final class Geometries {

    private Geometries() {}

    /**
     * Reads the geometry of a WKT text.
     *
     * @throws IllegalArgumentException if the text is not valid WKT
     */
    public static Geometry wkt(String wkt) {
        try {
            return new WKTReader().read(wkt);
        } catch (ParseException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Returns a copy of the geometry without repeated consecutive points in its lines and rings. The writer leaves such
     * points out, and a geometry compares with its written form once both are without them. The points of a
     * {@code MultiPoint} are kept: a MoveTo writes repeated points.
     */
    public static Geometry withoutRepeatedPoints(Geometry geometry) {
        GeometryEditor editor = new GeometryEditor(geometry.getFactory());
        return editor.edit(geometry, new RepeatedPointRemoval());
    }

    private static final class RepeatedPointRemoval extends GeometryEditor.CoordinateOperation {
        @Override
        public Coordinate[] edit(Coordinate[] coordinates, Geometry geometry) {
            return CoordinateArrays.removeRepeatedPoints(coordinates);
        }
    }
}
