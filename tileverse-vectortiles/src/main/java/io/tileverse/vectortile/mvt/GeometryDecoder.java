/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import static io.tileverse.vectortile.mvt.GeometryPacker.decodeZigZag;

import io.tileverse.vectortile.model.GeometryReader;
import io.tileverse.vectortile.model.VectorTile;
import java.util.function.UnaryOperator;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateSequenceFactory;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.impl.PackedCoordinateSequenceFactory;
import org.locationtech.jts.geom.util.AffineTransformation;

/**
 * Builds the JTS geometry of a feature of a decoded tile from the geometries of its layer, decoded and packed when the
 * tile was decoded (see {@link GeometryPacker}). Coordinates are preserved exactly as stored in the tile.
 *
 * <p>Each geometry gets coordinate sequences of its own, created with the factory of the reader: callers may modify
 * them, as a transformation applied in place does.
 *
 * @param gf the JTS GeometryFactory used to create geometries and coordinate sequences
 * @param transform the transformation applied to the decoded geometries
 */
record GeometryDecoder(GeometryFactory gf, UnaryOperator<Geometry> transform) implements GeometryReader {

    public static final GeometryFactory DEFAULT_GEOMETRY_FACTORY =
            new GeometryFactory(new PackedCoordinateSequenceFactory());

    public GeometryDecoder() {
        this(DEFAULT_GEOMETRY_FACTORY, UnaryOperator.identity());
    }

    /**
     * Returns an instance with the same geometry factory and an in-place coordinate transformation that converts
     * coordinates from one extent to the other.
     *
     * <p>For example, to convert geometries in a tile's internal coordinate extent of {@code 4096x4096} to a standard
     * tile size of {@code 256x256}, use {@code withExtentScaling(4096, 256)}
     *
     * @param fromExtent the original coordinate space extent (e.g. {@code 4096})
     * @param toExtent the target coordinate space extent (e.g. {@code 256})
     * @return a scaling geometry decoder using an affine transformation to scale from the source extent to the target
     *     extent
     * @see #withAffineTransformation
     */
    public GeometryReader withExtentScaling(int fromExtent, int toExtent) {
        double scaleFactor = (double) toExtent / fromExtent;
        AffineTransformation scaleInstance = AffineTransformation.scaleInstance(scaleFactor, scaleFactor);
        return withAffineTransformation(scaleInstance);
    }

    @Override
    public GeometryReader withGeometryFactory(GeometryFactory geometryFactory) {
        return new GeometryDecoder(geometryFactory, transform());
    }

    @Override
    public GeometryReader withGeometryTransformation(UnaryOperator<Geometry> transform) {
        return new GeometryDecoder(gf(), transform);
    }

    public GeometryReader withAffineTransformation(AffineTransformation transform) {
        return withGeometryTransformation(GeometryReader.toFunction(transform));
    }

    @Override
    public Geometry decode(VectorTile.Layer.Feature feature) {
        MvtFeature mvtFeature = (MvtFeature) feature;
        return decode(mvtFeature.geometryType(), mvtFeature.layerGeometries(), mvtFeature.index(), gf());
    }

    /**
     * Builds the geometry of a feature from the packed geometries of its layer, then applies the transformation of this
     * decoder. A feature without parts gets the empty geometry of its type, and a feature of unknown type an empty
     * geometry collection.
     *
     * @param type the geometry type of the feature
     * @param geometries the packed geometries of the layer of the feature
     * @param feature the index of the feature in its layer
     * @param gf the factory of the geometry and of its coordinate sequences
     * @return the decoded geometry
     */
    Geometry decode(GeomType type, PackedGeometries geometries, int feature, GeometryFactory gf) {
        int firstPart = geometries.firstPart(feature);
        int partCount = geometries.partCount(feature);
        final Geometry decoded =
                switch (type) {
                    case POINT -> decodePoint(geometries, firstPart, partCount, gf);
                    case LINESTRING -> decodeLineString(geometries, firstPart, partCount, gf);
                    case POLYGON -> decodePolygon(geometries, firstPart, partCount, gf);
                    default -> gf.createGeometryCollection();
                };

        return transform.apply(decoded);
    }

    /**
     * Creates Point or MultiPoint geometry from the part holding the points of a feature.
     *
     * @return Point for single coordinate, MultiPoint for multiple coordinates, or empty Point
     */
    private static Geometry decodePoint(PackedGeometries geometries, int firstPart, int partCount, GeometryFactory gf) {
        if (partCount == 0) {
            return gf.createPoint();
        }
        CoordinateSequence points = partSequence(geometries, firstPart, gf);
        return points.size() == 1 ? gf.createPoint(points) : gf.createMultiPoint(points);
    }

    /**
     * Creates LineString or MultiLineString geometry from parts.
     *
     * @return LineString, MultiLineString, or empty LineString
     */
    private static Geometry decodeLineString(
            PackedGeometries geometries, int firstPart, int partCount, GeometryFactory gf) {
        return switch (partCount) {
            case 0 -> gf.createLineString();
            case 1 -> gf.createLineString(partSequence(geometries, firstPart, gf));
            default -> createMultiLineString(partSequences(geometries, firstPart, partCount, gf), gf);
        };
    }

    /**
     * Creates Polygon or MultiPolygon geometry from ring parts.
     *
     * @return Polygon, MultiPolygon, or empty Polygon
     */
    private static Geometry decodePolygon(
            PackedGeometries geometries, int firstPart, int partCount, GeometryFactory gf) {
        return switch (partCount) {
            case 0 -> gf.createPolygon();
            case 1 -> gf.createPolygon(partSequence(geometries, firstPart, gf)); // simple case, single outer shell
            default -> buildComplexPolygon(geometries, firstPart, partCount, gf);
        };
    }

    /**
     * Creates MultiLineString from multiple line parts.
     *
     * @param parts the coordinate sequences of the lines, > 1
     */
    private static Geometry createMultiLineString(CoordinateSequence[] parts, GeometryFactory gf) {
        LineString[] lines = new LineString[parts.length];
        for (int i = 0; i < parts.length; i++) {
            lines[i] = gf.createLineString(parts[i]);
        }
        return gf.createMultiLineString(lines);
    }

    /**
     * Constructs complex polygons or multipolygons with holes from multiple rings.
     *
     * <p>Groups rings into polygons with the marks of the packed geometries: a ring starting a polygon is its shell,
     * and the rings up to the next shell are its holes.
     *
     * @param partCount the number of rings, at least 2
     * @return MultiPolygon or single Polygon
     */
    private static Geometry buildComplexPolygon(
            PackedGeometries geometries, int firstPart, int partCount, GeometryFactory gf) {
        CoordinateSequence[] parts = partSequences(geometries, firstPart, partCount, gf);
        LinearRing[] allRings = new LinearRing[partCount];
        for (int partIndex = 0; partIndex < partCount; partIndex++) {
            allRings[partIndex] = gf.createLinearRing(parts[partIndex]);
        }

        // Group rings into polygons
        Polygon[] polygonArray = new Polygon[partCount]; // Max possible polygons
        int polygonCount = 0;
        int currentPolygonStart = 0;

        for (int i = 1; i <= partCount; i++) {
            boolean isEndOfPolygon = (i == partCount) || geometries.startsPolygon(firstPart + i);

            if (isEndOfPolygon) {
                LinearRing shell = allRings[currentPolygonStart];
                int holeCount = i - currentPolygonStart - 1;

                if (holeCount == 0) {
                    polygonArray[polygonCount++] = gf.createPolygon(shell);
                } else if (holeCount == 1) {
                    // Avoid array creation for single hole - use direct array
                    polygonArray[polygonCount++] =
                            gf.createPolygon(shell, new LinearRing[] {allRings[currentPolygonStart + 1]});
                } else {
                    LinearRing[] holes = new LinearRing[holeCount];
                    System.arraycopy(allRings, currentPolygonStart + 1, holes, 0, holeCount);
                    polygonArray[polygonCount++] = gf.createPolygon(shell, holes);
                }
                currentPolygonStart = i;
            }
        }

        if (polygonCount == 1) {
            return polygonArray[0];
        }
        // Avoid array copy if already exact size
        if (polygonCount == polygonArray.length) {
            return gf.createMultiPolygon(polygonArray);
        }
        // Trim array to actual size
        Polygon[] trimmedPolygons = new Polygon[polygonCount];
        System.arraycopy(polygonArray, 0, trimmedPolygons, 0, polygonCount);
        return gf.createMultiPolygon(trimmedPolygons);
    }

    /** Returns a new sequence holding the coordinates of one part. */
    private static CoordinateSequence partSequence(PackedGeometries geometries, int part, GeometryFactory gf) {
        int start = geometries.partStart(part);
        int end = geometries.partEnd(part);
        return newSequence(geometries, start, end, gf.getCoordinateSequenceFactory());
    }

    /**
     * Returns the sequences of the parts of a feature. With the default factory, the parts are views of one sequence
     * holding the coordinates of the feature, as in earlier releases. With another factory, each part gets a sequence
     * of its own from the factory.
     */
    private static CoordinateSequence[] partSequences(
            PackedGeometries geometries, int firstPart, int partCount, GeometryFactory gf) {
        if (gf == DEFAULT_GEOMETRY_FACTORY) {
            return viewsOfFeatureSequence(geometries, firstPart, partCount, gf.getCoordinateSequenceFactory());
        }
        CoordinateSequence[] parts = new CoordinateSequence[partCount];
        for (int i = 0; i < partCount; i++) {
            parts[i] = partSequence(geometries, firstPart + i, gf);
        }
        return parts;
    }

    // The parts of a feature follow one another in the packed coordinates
    private static CoordinateSequence[] viewsOfFeatureSequence(
            PackedGeometries geometries, int firstPart, int partCount, CoordinateSequenceFactory factory) {
        int featureStart = geometries.partStart(firstPart);
        int featureEnd = geometries.partEnd(firstPart + partCount - 1);
        CoordinateSequence feature = newSequence(geometries, featureStart, featureEnd, factory);
        CoordinateSequence[] parts = new CoordinateSequence[partCount];
        for (int i = 0; i < partCount; i++) {
            int start = geometries.partStart(firstPart + i);
            int length = geometries.partEnd(firstPart + i) - start;
            parts[i] = new SubCoordinateSequence(feature, start - featureStart, length, factory);
        }
        return parts;
    }

    /**
     * Returns a new sequence from the factory, holding the packed coordinates from {@code start} inclusive to
     * {@code end} exclusive. A packed factory of doubles gets the coordinates in one array, in the kind of sequence
     * returned by its {@code create(size, 2)}. Another factory gets them one coordinate at a time.
     */
    private static CoordinateSequence newSequence(
            PackedGeometries geometries, int start, int end, CoordinateSequenceFactory factory) {
        if (factory instanceof PackedCoordinateSequenceFactory packed
                && packed.getType() == PackedCoordinateSequenceFactory.DOUBLE) {
            double[] xy = new double[2 * (end - start)];
            geometries.copyCoordinates(start, end, xy);
            return packed.create(xy, 2);
        }
        CoordinateSequence sequence = factory.create(end - start, 2);
        for (int i = start; i < end; i++) {
            sequence.setOrdinate(i - start, Coordinate.X, geometries.x(i));
            sequence.setOrdinate(i - start, Coordinate.Y, geometries.y(i));
        }
        return sequence;
    }

    /** Lists the remaining commands of the cursor and their points, for debugging. It reads the cursor to its end. */
    static String toString(VarintCursor commands) {
        StringBuilder sb = new StringBuilder("[\n");

        int cursorX = 0;
        int cursorY = 0;
        int pointIndex = 0;
        while (commands.hasNext()) {
            int cmd = commands.next();
            int commandId = GeometryCommand.extractCommand(cmd);
            int count = GeometryCommand.extractCount(cmd);
            String commandName =
                    switch (commandId) {
                        case 1 -> "MoveTo";
                        case 2 -> "LineTo";
                        case 7 -> "ClosePath";
                        default -> "Unknown(" + commandId + ")";
                    };
            sb.append("  Command: ")
                    .append(commandName)
                    .append(", Count: ")
                    .append(count)
                    .append("\n");
            if (commandId == GeometryCommand.MoveTo || commandId == GeometryCommand.LineTo) {
                // a malformed command may announce more points than the cursor holds
                for (int k = 0; k < count && commands.remaining() >= 2; k++) {
                    int dx = decodeZigZag(commands.next());
                    int dy = decodeZigZag(commands.next());
                    cursorX += dx;
                    cursorY += dy;
                    sb.append("    dx: ").append(dx).append(", dy: ").append(dy);
                    sb.append("    Point ")
                            .append(pointIndex++)
                            .append(": (")
                            .append(cursorX)
                            .append(", ")
                            .append(cursorY)
                            .append(")\n");
                }
            }
        }
        sb.append("]");
        return sb.toString();
    }
}
