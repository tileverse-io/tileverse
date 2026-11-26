/*
 * Copyright 2015 Electronic Chart Centre
 * Copyright 2026 Multiversio LLC. All rights reserved.
 *
 * Modifications: the clipping of VectorTileEncoder as a pipeline step.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tileverse.vectortile.pipeline;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.TopologyException;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

/**
 * Clips the geometries to the tile and a buffer around it, and drops the features outside.
 *
 * <p>The clip area spans from {@code -buffer} to {@code extent + buffer} on both axes, edges included. The extent and
 * the buffer are in the units of the geometries. A {@link Point} outside the clip area drops the feature. A geometry
 * within the clip area is returned as is. Other geometries are replaced by their intersection with the clip area, and
 * an empty intersection drops the feature.
 *
 * <p>The step makes the same overlay calls as java-vector-tile and returns the same clipped geometries.
 */
public final class ClipStep implements GeometryStep {

    private final double extent;
    private final double buffer;
    private final Polygon clipPolygon;
    private final Envelope clipEnvelope;
    private final PreparedGeometry preparedClip;

    /**
     * Creates the clip step of a tile.
     *
     * @param extent the size of the tile on both axes, in the units of the geometries
     * @param buffer the width of the band kept around the tile, in the units of the geometries
     * @throws IllegalArgumentException if the extent is not positive and finite, or the buffer is negative or not
     *     finite
     */
    public ClipStep(double extent, double buffer) {
        this.extent = Parameters.requirePositiveAndFinite(extent, "extent");
        this.buffer = Parameters.requireNotNegativeAndFinite(buffer, "buffer");
        this.clipPolygon = createClipPolygon(extent, buffer);
        this.clipEnvelope = clipPolygon.getEnvelopeInternal();
        this.preparedClip = PreparedGeometryFactory.prepare(clipPolygon);
    }

    /**
     * Creates the clip polygon with the vertices of java-vector-tile, in the same order: the clipped rings may start at
     * another vertex otherwise.
     */
    private static Polygon createClipPolygon(double extent, double buffer) {
        // -buffer would give -0.0 for a buffer of 0, where java-vector-tile computes 0 - buffer on ints and gets 0.0
        double min = 0 - buffer;
        double max = extent + buffer;
        Coordinate[] ring = new Coordinate[5];
        ring[0] = new Coordinate(min, max);
        ring[1] = new Coordinate(max, max);
        ring[2] = new Coordinate(max, min);
        ring[3] = new Coordinate(min, min);
        ring[4] = ring[0];
        Polygon polygon = new GeometryFactory().createPolygon(ring);
        cacheEnvelopes(polygon);
        return polygon;
    }

    /**
     * Fills the envelope caches of the polygon and of its ring. JTS fills them on first use, without synchronization,
     * and the overlay reads both. Filled in the constructor, they are visible to the threads sharing the step.
     */
    private static void cacheEnvelopes(Polygon polygon) {
        polygon.getEnvelopeInternal();
        polygon.getExteriorRing().getEnvelopeInternal();
    }

    /** The size of the tile on both axes, in the units of the geometries. */
    public double extent() {
        return extent;
    }

    /** The width of the band kept around the tile, in the units of the geometries. */
    public double buffer() {
        return buffer;
    }

    @Override
    public Geometry apply(Geometry geometry) {
        if (geometry instanceof Point point) {
            return clipPoint(point);
        }
        if (isWithinClipEnvelope(geometry)) {
            return geometry;
        }
        return intersect(geometry);
    }

    private Point clipPoint(Point point) {
        if (clipEnvelope.covers(point.getX(), point.getY())) {
            return point;
        }
        return null;
    }

    private boolean isWithinClipEnvelope(Geometry geometry) {
        return clipEnvelope.contains(geometry.getEnvelopeInternal());
    }

    /**
     * Intersects the geometry with the clip polygon. If the overlay fails, returns the geometry held at that point, as
     * java-vector-tile does: the input when the first intersection fails, the empty first result otherwise.
     */
    private Geometry intersect(Geometry geometry) {
        Geometry clipped = geometry;
        try {
            clipped = clipPolygon.intersection(geometry);
            if (clipped.isEmpty() && preparedClip.intersects(geometry)) {
                clipped = intersectWktCopy(geometry);
            }
            return clipped;
        } catch (TopologyException | ParseException e) {
            return clipped;
        }
    }

    /**
     * Intersects the clip polygon with a copy of the geometry read back from its WKT. The overlay sometimes returns an
     * empty intersection for a geometry crossing the clip polygon, and the copy clips as expected.
     */
    private Geometry intersectWktCopy(Geometry geometry) throws ParseException {
        Geometry copy = new WKTReader().read(geometry.toText());
        return clipPolygon.intersection(copy);
    }

    @Override
    public String toString() {
        return "ClipStep[extent=" + extent + ", buffer=" + buffer + "]";
    }
}
