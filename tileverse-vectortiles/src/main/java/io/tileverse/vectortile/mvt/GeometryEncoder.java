/*
 * Copyright 2015 Electronic Chart Centre
 * Copyright 2025 Multiversio LLC. All rights reserved.
 *
 * Modifications: Modernized and integrated into Tileverse PMTiles project.
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
package io.tileverse.vectortile.mvt;

import static com.google.protobuf.CodedOutputStream.encodeZigZag32;

import io.tileverse.vectortile.mvt.VectorTileBuilder.BuildParams;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.IntConsumer;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateSequenceFilter;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryComponentFilter;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Lineal;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.geom.Puntal;
import org.locationtech.jts.geom.TopologyException;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.precision.GeometryPrecisionReducer;
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier;
import org.locationtech.jts.simplify.TopologyPreservingSimplifier;

class GeometryEncoder {

    private final BuildParams params;

    private static final GeometryPrecisionReducer precisionReducer;

    static {
        PrecisionModel integerPrecision = new PrecisionModel(1.0); // Scale 1.0 = snap to integers
        precisionReducer = new GeometryPrecisionReducer(integerPrecision);
        precisionReducer.setRemoveCollapsedComponents(true); // Remove degenerate components
    }

    /**
     * @param buildParams
     */
    GeometryEncoder(BuildParams buildParams) {
        this.params = buildParams;
    }

    /**
     * Prepares the final geometries to encode, may result in more geometries than
     * the original, which requires duplicating the features.
     * <p>
     * The Geometry must be in "pixel" {@literal 0..<extent - 1>} in both directions.
     * <p>
     * For optimization, geometries will be clipped and simplified. Features with
     * geometries outside of the tile will be skipped.
     *
     * @param layerName  a {@link String} with the vector tile layer name.
     * @param attributes a {@link Map} with the vector tile feature attributes.
     * @param geometry   a {@link Geometry} for the vector tile feature.
     * @param id         a long with the vector tile feature id field.
     */
    public List<Geometry> prepareGeometries(final Geometry input) {
        Geometry geometry = input;
        // skip small Polygon/LineString.
        if (geometry instanceof Polygonal && geometry.getArea() < params.getMinimumArea()) {
            return List.of();
        } else if (geometry instanceof Lineal && geometry.getLength() < params.getMinimumLength()) {
            return List.of();
        } else if (isGenericGeometryCollection(geometry)) {
            // special handling of GeometryCollection. subclasses are not handled here.
            return prepareSubGeometries((GeometryCollection) geometry);
        }

        // About to simplify and clip. Looks like simplification before clipping is
        // faster than clipping before simplification
        geometry = simplify(geometry);

        // Step 1: Transform coordinates to extent space
        // geometry = scaleToExtentSpace(geometry);

        if (geometry == null || geometry.isEmpty()) {
            return List.of();
        }

        // clip geometry
        if (geometry instanceof Point) {
            if (!clipCovers(geometry)) {
                return List.of();
            }
        } else {
            geometry = clipGeometry(geometry);
        }

        // no need to add empty geometry
        if (geometry == null || geometry.isEmpty()) {
            return List.of();
        }

        // extra check for GeometryCollection after clipping as it can cause
        // GeometryCollection. Subclasses not handled here.
        if (isGenericGeometryCollection(geometry)) {
            // revisit: subgeometries are already simplified and clipped, maybe just recursively filter out generic
            // collections
            return prepareSubGeometries((GeometryCollection) geometry);
        }
        geometry = applyPrecisionModelSnapping(geometry);
        if (geometry == null) {
            return List.of();
        }
        return List.of(geometry);
    }

    /**
     * @param preparedGeometry must have been returned from {@link #prepareGeometries(Geometry)}
     */
    public List<Integer> commands(Geometry preparedGeometry) {
        return pack(preparedGeometry);
    }

    private Geometry simplify(final Geometry geometry) {
        // simplify non-points
        double simplificationDistance = params.getSimplificationDistanceTolerance();
        if (simplificationDistance <= 0.0 || geometry instanceof Point) {
            return geometry;
        }
        Geometry simplified = geometry;
        if (geometry instanceof Lineal) {
            simplified = DouglasPeuckerSimplifier.simplify(geometry, simplificationDistance);
        } else if (geometry instanceof Polygonal) {
            simplified = DouglasPeuckerSimplifier.simplify(geometry, simplificationDistance);
            // extra check to prevent polygon converted to line
            if (!(simplified instanceof Polygonal)) {
                simplified = TopologyPreservingSimplifier.simplify(geometry, simplificationDistance);
            }
        } else {
            simplified = TopologyPreservingSimplifier.simplify(geometry, simplificationDistance);
        }
        if (!simplified.isValid()) {
            simplified = DouglasPeuckerSimplifier.simplify(geometry, simplificationDistance * 2.0);
        }
        return simplified;
    }

    /**
     * Apply coordinate transformation and JTS GeometryPrecisionReducer to ensure integer coordinates.
     * This is more robust than Math.round() as it maintains topological validity.
     *
     * Steps:
     * 1. Transform coordinates to tile extent space (apply scaling)
     * 2. Apply GeometryPrecisionReducer with PrecisionModel(1.0) to snap to integers
     *
     * Benefits of GeometryPrecisionReducer over Math.round():
     * - Maintains topological validity where possible
     * - Prevents polygon collapse and self-intersections
     * - Handles degenerate geometries robustly
     * - Removes collapsed components automatically
     */
    private Geometry applyPrecisionModelSnapping(Geometry geometry) {
        try {
            Geometry preciseGeometry = precisionReducer.reduce(geometry);
            if (isValidForEncoding(preciseGeometry)) {
                return preciseGeometry;
            }
        } catch (IllegalArgumentException invalidGeom) {
            return null;
        }
        return null;
    }

    //    private Geometry scaleToExtentSpace(Geometry geometry) {
    //        double scale = params.isAutoScale() ? (params.getExtent() / 256.0) : 1.0;
    //        if (scale != 1.0) {
    //            AffineTransformation scaleTransform = AffineTransformation.scaleInstance(scale, scale);
    //            geometry = scaleTransform.transform(geometry);
    //        }
    //        return geometry;
    //    }

    /**
     * Check if geometry is valid for encoding after precision snapping.
     * Note: We skip the expensive geometry.isValid() call since GeometryPrecisionReducer
     * with removeCollapsedComponents=true already ensures reasonable validity.
     */
    private boolean isValidForEncoding(Geometry geometry) {
        if (geometry == null || geometry.isEmpty() || geometry.getNumPoints() == 0) {
            return false;
        }

        int minPoints;
        if (geometry instanceof Puntal) {
            minPoints = 1;
        } else if (geometry instanceof Lineal) {
            minPoints = 2;
        } else if (geometry instanceof Polygonal) {
            minPoints = 4;
        } else {
            return false; // Unknown geometry type
        }
        // Skip geometry.isValid() - trust GeometryPrecisionReducer to maintain validity
        return geometry.getNumPoints() >= minPoints;
    }

    private List<Geometry> prepareSubGeometries(GeometryCollection geometry) {
        int numGeometries = geometry.getNumGeometries();
        List<Geometry> prepared = new ArrayList<>(numGeometries);
        for (int i = 0; i < numGeometries; i++) {
            Geometry subGeometry = geometry.getGeometryN(i);
            prepared.addAll(prepareGeometries(subGeometry));
        }
        return prepared;
    }

    private static boolean isGenericGeometryCollection(Geometry g) {
        return g instanceof GeometryCollection
                && !(g instanceof Puntal)
                && !(g instanceof Lineal)
                && !(g instanceof Polygonal);
    }

    /**
     * A short circuit clip to the tile extent (tile boundary + buffer) for points
     * to improve performance. This method can be overridden to change clipping
     * behavior. See also {@link #clipGeometry(Geometry)}.
     *
     * @param geom a {@link Geometry} to check for "covers"
     * @return a boolean true when the current clip geometry covers the given geom.
     */
    protected boolean clipCovers(Geometry geom) {
        Envelope clipEnvelope = params.getClipEnvelope();
        if (geom instanceof Point p) {
            return clipEnvelope.covers(p.getX(), p.getY());
        }
        return clipEnvelope.covers(geom.getEnvelopeInternal());
    }

    /**
     * Clip geometry according to buffer given at construct time. This method can be
     * overridden to change clipping behavior. See also
     * {@link #clipCovers(Geometry)}.
     *
     * @param geometry a {@link Geometry} to check for intersection with the current
     *                 clip geometry
     * @return a boolean true when current clip geometry intersects with the given
     *         geometry.
     */
    protected Geometry clipGeometry(Geometry geometry) {
        Envelope clipEnvelope = params.getClipEnvelope();
        try {
            if (clipEnvelope.contains(geometry.getEnvelopeInternal())) {
                return geometry;
            }

            final Geometry clipGeometry = params.getClipGeometry();
            final PreparedGeometry clipGeometryPrepared = params.getClipGeometryPrepared();

            Geometry original = geometry;
            geometry = clipGeometry.intersection(original);

            // some times a intersection is returned as an empty geometry.
            // going via wkt fixes the problem.
            if (geometry.isEmpty() && clipGeometryPrepared.intersects(original)) {
                Geometry originalViaWkt = new WKTReader().read(original.toText());
                geometry = clipGeometry.intersection(originalViaWkt);
            }

            return geometry;
        } catch (TopologyException e) {
            // could not intersect. original geometry will be used instead.
            return geometry;
        } catch (ParseException e1) {
            // could not encode/decode WKT. original geometry will be used
            // instead.
            return geometry;
        }
    }

    private List<Integer> pack(Geometry geometry) {
        List<Integer> commands = new ArrayList<>(geometry.getNumPoints());
        IntConsumer sink = commands::add;
        GeometryPacker.pack(geometry, sink);
        return commands;
    }

    private static record GeometryPacker(CoordinateCursor cursor, IntConsumer commandsSink)
            implements GeometryComponentFilter, CoordinateSequenceFilter {

        @SuppressWarnings("serial")
        private static final class CoordinateCursor extends CoordinateXY {
            public int startAtIndex = -1;
            public int stopAtIndex = Integer.MAX_VALUE;
            public boolean processingMultiPoint = false;
        }

        public static void pack(Geometry geom, IntConsumer commandSink) {
            checkPreconditions(geom);
            CoordinateCursor cursor = new CoordinateCursor();
            cursor.processingMultiPoint = geom instanceof MultiPoint;
            geom.apply(((GeometryComponentFilter) new GeometryPacker(cursor, commandSink)));
            cursor.processingMultiPoint = false;
        }

        private static void checkPreconditions(Geometry geom) {
            if (isGenericGeometryCollection(geom)) {
                throw new IllegalArgumentException(
                        "Generic GeometryCollections should have been filtered out by prepareGeometries(), got "
                                + geom);
            } else if (geom.isEmpty()) {
                throw new IllegalArgumentException(
                        "Empty Geometry should have been filtered out by prepareGeometries(), got " + geom);
            }
        }

        @Override
        public void filter(Geometry geom) {
            switch (geom.getDimension()) {
                case 0 -> encodePuntal((Puntal) geom);
                case 1 -> encodeLineal((Lineal) geom);
                default -> {
                    // no-op, traversal of inner rings handled by Polygon.apply(GeometryComponentFilter)
                }
            }
        }

        /**
         * Only called for {@link LineString}, and {@link LinearRing}
         */
        @Override
        public void filter(final CoordinateSequence sequence, final int coordIndex) {
            if (coordIndex >= cursor.startAtIndex && coordIndex <= cursor.stopAtIndex) {
                double x = sequence.getOrdinate(coordIndex, Coordinate.X);
                double y = sequence.getOrdinate(coordIndex, Coordinate.Y);
                coordinate(x, y);
            }
        }

        private void encodePuntal(Puntal puntal) {
            if (puntal instanceof Point point) {
                if (!cursor.processingMultiPoint) {
                    moveTo(1);
                }
                coordinate(point.getX(), point.getY());
            } else if (puntal instanceof MultiPoint mp) {
                moveTo(mp.getNumGeometries());
            }
        }

        private void encodeLineal(Lineal geom) {
            if (geom instanceof LineString line) {
                cursor.startAtIndex = 1;
                int coordCount = line.getNumPoints();
                if (geom instanceof LinearRing) {
                    // discard last coordinate, well add a ClosePath after encoding the CoordinateSequence
                    coordCount--;
                    cursor.stopAtIndex = coordCount - 1; // zero based index of last coordinate - 1
                }
                // start of line and initial coordinate
                moveTo(1);
                Point start = line.getPointN(0);
                coordinate(start.getX(), start.getY());
                // LineTo command with remaining count
                lineTo(coordCount - 1);

                // apply coord seq filter to encode coordinates within range 1..stopAtIndex
                line.apply((CoordinateSequenceFilter) this);

                // reset coord index bounds
                cursor.startAtIndex = -1;
                cursor.stopAtIndex = Integer.MAX_VALUE;

                // ClosePath if needed
                if (geom instanceof LinearRing) {
                    closePath();
                }
            }
        }

        private void moveTo(int count) {
            commandsSink.accept(GeometryCommand.packCommandAndLength(GeometryCommand.MoveTo, count));
        }

        private void lineTo(int count) {
            commandsSink.accept(GeometryCommand.packCommandAndLength(GeometryCommand.LineTo, count));
        }

        private void closePath() {
            commandsSink.accept(GeometryCommand.packCommandAndLength(GeometryCommand.ClosePath, 1));
        }

        private void coordinate(double x, double y) {
            int dx = (int) Math.round(x - cursor.getX());
            int dy = (int) Math.round(y - cursor.getY());
            commandsSink.accept(encodeZigZag32(dx));
            commandsSink.accept(encodeZigZag32(dy));
            // Update cursor position after encoding deltas
            cursor.setX(x);
            cursor.setY(y);
        }

        @Override
        public boolean isDone() {
            return false;
        }

        @Override
        public boolean isGeometryChanged() {
            return false;
        }
    }
}
