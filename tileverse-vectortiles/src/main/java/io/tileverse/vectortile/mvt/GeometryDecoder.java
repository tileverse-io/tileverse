/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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

import static com.google.protobuf.CodedInputStream.decodeZigZag32;

import com.google.protobuf.Internal.IntList;
import io.tileverse.vectortile.mvt.VectorTileProto.Tile.GeomType;
import java.util.ArrayList;
import java.util.List;
import org.locationtech.jts.algorithm.Area;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;

/**
 * Optimized MVT geometry decoder that minimizes memory allocations and coordinate access.
 * <p>
 * Uses pre-computed orientations and direct coordinate sequence operations for better performance.
 * Coordinates are preserved exactly as stored in the MVT protobuf data.
 *
 * @param gf the JTS GeometryFactory used to create geometries and coordinate sequences
 */
public record GeometryDecoder(GeometryFactory gf) {

    /**
     * Ring orientation for polygon construction.
     */
    private enum Orientation {
        /** Clockwise orientation */
        CW,
        /** Counter-clockwise orientation */
        CCW,
        /** Degenerate ring (invalid/zero area) */
        DEGENERATE
    }

    /**
     * Represents a geometry part with coordinate range and orientation.
     * @param start starting index in the coordinate sequence
     * @param length number of coordinates in this part
     * @param orientation ring orientation (only meaningful for polygon rings)
     */
    private static record Part(int start, int length, Orientation orientation) {}

    /**
     * Decodes the MVT feature into a JTS Geometry.
     * @return the decoded geometry
     */
    public Geometry decode(VectorTileProto.Tile.Feature feature) {

        // empyric sizing with profiler's aid
        List<Part> parts = new ArrayList<>(feature.getGeometryCount() / 3);
        CoordinateSequence allCoords = extractAllCoordinates(feature, parts);

        final GeomType type = feature.getType();
        return switch (type) {
            case POINT -> decodePoint(allCoords);
            case LINESTRING -> decodeLineString(parts, allCoords);
            case POLYGON -> decodePolygon(parts, allCoords);
            default -> gf.createGeometryCollection();
        };
    }

    /**
     * Extracts all coordinates from MVT commands and populates geometry parts.
     * For POINT geometries, creates a single part with all coordinates.
     * For POLYGON geometries, pre-computes polygon structure with holes.
     * For other geometries, creates separate parts for each ring/line.
     * @param feature
     * @param parts output list to store geometry parts with pre-computed orientations
     * @return coordinate sequence containing all decoded coordinates
     */
    private CoordinateSequence extractAllCoordinates(VectorTileProto.Tile.Feature feature, List<Part> parts) {
        final GeomType geomType = feature.getType();
        final IntList commands = (IntList) feature.getGeometryList();
        final int cmdCount = commands.size();
        final int maxPossibleSize = calculateCoordinateCount(commands);
        CoordinateSequence allCoords = gf.getCoordinateSequenceFactory().create(maxPossibleSize, 2);

        int coordIndex = 0;
        // unscaled cursor position to compute each deltified ordinate
        int cursorX = 0;
        int cursorY = 0;

        // track part start coordinate (unscaled) for ClosePath
        int partStartX = 0;
        int partStartY = 0;

        // For polygon structure pre-computation
        Orientation firstRingOrientation = null;

        int cmdIndex = 0;
        while (cmdIndex < cmdCount) {
            int cmd = commands.getInt(cmdIndex++);
            int command = GeometryCommand.extractCommand(cmd);
            int count = GeometryCommand.extractCount(cmd);

            if (command != GeometryCommand.MoveTo) {
                throw new IllegalArgumentException("Geometry part must start with MoveTo");
            }

            final int partStart = coordIndex;

            // Process MoveTo coordinates
            for (int k = 0; k < count; k++) {
                int dx = decodeZigZag32(commands.getInt(cmdIndex++));
                int dy = decodeZigZag32(commands.getInt(cmdIndex++));
                cursorX += dx;
                cursorY += dy;
                partStartX = cursorX;
                partStartY = cursorY;
                allCoords.setOrdinate(coordIndex, 0, cursorX);
                allCoords.setOrdinate(coordIndex, 1, cursorY);
                coordIndex++;
            }

            while (cmdIndex < cmdCount) {
                final int nextCmd = commands.getInt(cmdIndex);
                final int nextCommand = GeometryCommand.extractCommand(nextCmd);
                if (nextCommand == GeometryCommand.MoveTo) {
                    break; // Next part
                } else if (nextCommand == GeometryCommand.LineTo) {
                    final int nextCount = GeometryCommand.extractCount(nextCmd);
                    cmdIndex++; // Skip command
                    for (int k = 0; k < nextCount; k++) {
                        int dx = decodeZigZag32(commands.getInt(cmdIndex++));
                        int dy = decodeZigZag32(commands.getInt(cmdIndex++));
                        cursorX += dx;
                        cursorY += dy;
                        allCoords.setOrdinate(coordIndex, 0, cursorX);
                        allCoords.setOrdinate(coordIndex, 1, cursorY);
                        coordIndex++;
                    }
                } else if (nextCommand == GeometryCommand.ClosePath) {
                    cmdIndex++;
                    allCoords.setOrdinate(coordIndex, 0, partStartX);
                    allCoords.setOrdinate(coordIndex, 1, partStartY);
                    coordIndex++;
                } else {
                    throw new IllegalArgumentException("Invalid command: " + nextCommand);
                }
            }

            int partLength = coordIndex - partStart;

            if (geomType == GeomType.POLYGON) {
                Orientation orientation = areaOfRingSigned(allCoords, partStart, partLength);
                // Skip degenerate rings entirely
                if (orientation == Orientation.DEGENERATE) {
                    continue;
                }
                // Set first ring orientation for reference
                if (firstRingOrientation == null) {
                    firstRingOrientation = orientation;
                }
                parts.add(new Part(partStart, partLength, orientation));
            } else if (geomType == GeomType.LINESTRING) {
                if (partLength < 2) {
                    continue; // Skip invalid linestrings
                }
                parts.add(new Part(partStart, partLength, Orientation.DEGENERATE));
            }
            // For POINT geometries, don't create parts here - handled after loop
        }

        // For POINT geometries, create a single part containing all coordinates
        if (geomType == GeomType.POINT && coordIndex > 0) {
            parts.add(new Part(0, coordIndex, Orientation.DEGENERATE));
        }

        // Return original sequence if we used exactly the allocated space, otherwise create subsequence
        if (coordIndex == maxPossibleSize) {
            return allCoords;
        }
        return new SubCoordinateSequence(allCoords, 0, coordIndex, gf.getCoordinateSequenceFactory());
    }

    /**
     * Creates Point or MultiPoint geometry from coordinate sequence.
     * @param coords the coordinate sequence
     * @return Point for single coordinate, MultiPoint for multiple coordinates
     */
    private Geometry decodePoint(CoordinateSequence coords) {
        return switch (coords.size()) {
            case 0 -> gf.createPoint();
            case 1 -> gf.createPoint(coords);
            default -> gf.createMultiPoint(coords);
        };
    }

    /**
     * Creates LineString or MultiLineString geometry from parts.
     * @param parts geometry parts defining line segments (all valid)
     * @param allCoords coordinate sequence containing all coordinates
     * @return LineString, MultiLineString, or empty LineString
     */
    private Geometry decodeLineString(List<Part> parts, CoordinateSequence allCoords) {
        return switch (parts.size()) {
            case 0 -> gf.createLineString();
            case 1 -> gf.createLineString(allCoords);
            default -> createMultiLineString(parts, allCoords);
        };
    }

    /**
     * Creates Polygon or MultiPolygon geometry from ring parts.
     * @param parts geometry parts defining polygon rings
     * @param allCoords coordinate sequence containing all coordinates
     * @return Polygon, MultiPolygon, or empty Polygon
     */
    private Geometry decodePolygon(List<Part> parts, CoordinateSequence allCoords) {
        return switch (parts.size()) {
            case 0 -> gf.createPolygon();
            case 1 -> gf.createPolygon(allCoords); // simple case, single outer shell
            default -> buildComplexPolygon(parts, allCoords);
        };
    }

    /**
     * Creates MultiLineString from multiple line parts, all valid.
     * @param parts geometry parts defining line segments, > 1
     * @param allCoords coordinate sequence containing all coordinates
     * @return MultiLineString or simplified geometry if possible
     */
    private Geometry createMultiLineString(List<Part> parts, CoordinateSequence allCoords) {
        LineString[] lines = parts.stream()
                .map(p -> createSubSequence(p, allCoords))
                .map(gf::createLineString)
                .toArray(LineString[]::new);
        return gf.createMultiLineString(lines);
    }
    /**
     * Constructs complex polygons or multipolygons with holes from multiple rings.
     * <p>
     * All parts are guaranteed to be valid (no degenerate orientations).
     * Groups rings into polygons based on pre-computed orientations.
     *
     * @param parts polygon ring parts with orientation information (all valid, size >= 2)
     * @param allCoords coordinate sequence containing all coordinates
     * @return MultiPolygon or single Polygon
     */
    private Geometry buildComplexPolygon(List<Part> parts, CoordinateSequence allCoords) {
        final int partCount = parts.size();

        // Create all rings - all parts are valid
        LinearRing[] allRings = new LinearRing[partCount];
        Orientation firstRingOrientation = parts.get(0).orientation();

        for (int partIndex = 0; partIndex < partCount; partIndex++) {
            final CoordinateSequence ringSeq = createSubSequence(parts.get(partIndex), allCoords);
            allRings[partIndex] = gf.createLinearRing(ringSeq);
        }

        // Group rings into polygons
        Polygon[] polygonArray = new Polygon[partCount]; // Max possible polygons
        int polygonCount = 0;
        int currentPolygonStart = 0;

        for (int i = 1; i <= partCount; i++) {
            boolean isEndOfPolygon = (i == partCount);

            if (!isEndOfPolygon) {
                // Rings with same orientation as first ring are exterior rings (start new polygons)
                boolean isExteriorRing = (parts.get(i).orientation() == firstRingOrientation);
                isEndOfPolygon = isExteriorRing;
            }

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

    /**
     * Calculates the exact number of coordinates that will be generated from MVT commands.
     * @param commands the MVT command list
     * @return exact number of coordinates needed
     */
    private static int calculateCoordinateCount(IntList commands) {
        final int size = commands.size();
        int closePathCount = 0;
        int commandCount = 0;

        for (int i = 0; i < size; ) {
            int cmd = commands.getInt(i++);
            int command = GeometryCommand.extractCommand(cmd);
            int count = GeometryCommand.extractCount(cmd);
            commandCount++;

            if (command == GeometryCommand.ClosePath) {
                closePathCount++;
            } else if (command == GeometryCommand.MoveTo || command == GeometryCommand.LineTo) {
                i += count * 2; // Skip coordinate parameters
            }
        }

        // Accurate size: (total - command_words) / 2 + ClosePath coordinates
        int ordinateCount = size - commandCount;
        int coordinateCount = (ordinateCount / 2) + closePathCount;
        return coordinateCount;
    }

    /**
     * Creates a coordinate subsequence for a specific geometry part.
     * @param part the geometry part with bounds from the full coord sequence
     * @param allCoords full coordinate sequence
     * @return coordinate subsequence for the specified part
     */
    private CoordinateSequence createSubSequence(Part part, CoordinateSequence allCoords) {
        return new SubCoordinateSequence(allCoords, part.start(), part.length(), gf.getCoordinateSequenceFactory());
    }

    /**
     * {@link Area#ofRing(CoordinateSequence)} modified to use a subsequence and {@link CoordinateSequence#getOrdinate(int, int)}
     * instead of {@link CoordinateSequence#createCoordinate()} + {@link CoordinateSequence#getCoordinate(int, Coordinate)}
     * <p>
     * Profiler shows a 10% performance improvement on {@link #buildPolygon buildPolygon()}
     * <p>
     * This code is copied under JTS EDL 1.0 license (BSD-3)
     */
    private static Orientation areaOfRingSigned(CoordinateSequence ring, final int start, final int length) {
        final int n = length;
        if (n < 3) return Orientation.DEGENERATE;
        /*
         * Based on the Shoelace formula. http://en.wikipedia.org/wiki/Shoelace_formula
         */
        double p0Y;
        double p1x = ring.getOrdinate(start, Coordinate.X);
        double p1y = ring.getOrdinate(start, Coordinate.Y);
        double p2x = ring.getOrdinate(start + 1, Coordinate.X);
        double p2y = ring.getOrdinate(start + 1, Coordinate.Y);

        double x0 = p1x;
        p2x -= x0;
        double sum = 0.0;
        for (int i = 1; i < n - 1; i++) {
            p0Y = p1y;
            p1x = p2x;
            p1y = p2y;
            p2x = ring.getOrdinate(start + i + 1, Coordinate.X);
            p2y = ring.getOrdinate(start + i + 1, Coordinate.Y);
            p2x -= x0;
            sum += p1x * (p0Y - p2y);
        }
        return (sum / 2.0) > 0 ? Orientation.CW : Orientation.CCW;
    }

    public static String toString(List<Integer> commands) {
        StringBuilder sb = new StringBuilder("[\n");

        int cursorX = 0;
        int cursorY = 0;
        int index = 0;
        int pointIndex = 0;
        while (index < commands.size()) {
            int cmd = commands.get(index++);
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
                for (int k = 0; k < count; k++) {
                    int dx = decodeZigZag32(commands.get(index++));
                    int dy = decodeZigZag32(commands.get(index++));
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
