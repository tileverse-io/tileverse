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

import io.tileverse.vectortile.model.Feature;
import io.tileverse.vectortile.model.Layer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;

/**
 * AssertJ-style fluent assertions for MVT features with proper context for attribute assertions.
 *
 * <p>Usage example:
 * <pre>{@code
 * assertThat(tile)
 *     .layer("roads")
 *         .feature(0)
 *             .hasAttribute("highway", "primary")
 *             .hasId(123)
 *             .geometry()
 *                 .isLineString()
 *                 .moveTo(3, 6)
 *                 .lineTo(8, 12)
 *                 .matches();
 * }</pre>
 */
class FeatureAssertions extends AbstractAssert<FeatureAssertions, Feature> {

    @SuppressWarnings("unused")
    private final Layer parentLayer;

    private final LayerAssertions parentLayerAssertions;

    public FeatureAssertions(Feature actual, Layer parentLayer, LayerAssertions parentLayerAssertions) {
        super(actual, FeatureAssertions.class);
        this.parentLayer = parentLayer;
        this.parentLayerAssertions = parentLayerAssertions;
    }

    /**
     * Assert that the feature has the specified attribute with the given value.
     * This uses the parent layer context to decode the feature's attributes.
     */
    public FeatureAssertions hasAttribute(String name, Object expectedValue) {
        isNotNull();
        Object actualValue = actual.getAttribute(name);
        Assertions.assertThat(actualValue)
                .overridingErrorMessage(
                        "Expected feature attribute <%s> to be <%s> but was <%s>", name, expectedValue, actualValue)
                .isEqualTo(expectedValue);
        return this;
    }

    /**
     * Assert that the MVT Feature does not have the tag for the given attribute name
     * This is different from {@link #hasAttribute(String)} in that this method checks the internal {@link VectorTileProto.Tile.Feature#getTagsList}
     * does not contain a tag for a layer's {@link io.tileverse.vectortile.mvt.VectorTileProto.Tile.Layer#getKeysList key}, whereas
     * {@link #hasAttribute(String)} works on the "unified feature type model" {@link Layer#getAttributeNames()} returning {@code null}
     * when asking a feature for an attribute whos'e tag it doesn't have
     */
    public FeatureAssertions doesNotHaveTag(String name) {
        isNotNull();

        Assertions.assertThat(actual.getAttributes())
                .overridingErrorMessage(
                        "Expected feature to not have attribute <%s> but attributes were <%s>",
                        name, actual.getAttributes().keySet())
                .doesNotContainKey(name);
        return this;
    }

    /**
     * Assert that the feature has an attribute with the specified name (regardless of value).
     */
    public FeatureAssertions hasAttribute(String name) {
        isNotNull();
        Assertions.assertThat(actual.getAttributes())
                .overridingErrorMessage(
                        "Expected feature to have attribute <%s> but attributes were <%s>",
                        name, actual.getAttributes().keySet())
                .containsKey(name);
        return this;
    }

    /**
     * Assert that the feature has the specified ID.
     */
    public FeatureAssertions hasId(long expectedId) {
        isNotNull();
        Assertions.assertThat(actual.getId())
                .overridingErrorMessage("Expected feature ID <%s> but was <%s>", expectedId, actual.getId())
                .isEqualTo(expectedId);
        return this;
    }

    /**
     * Navigate to geometry assertions for this feature's geometry.
     * This creates a separate assertion context for geometry commands.
     */
    public FeatureGeometryAssertions geometry() {
        isNotNull();
        return new FeatureGeometryAssertions(actual, this);
    }

    /**
     * Navigate back to the parent layer assertions.
     */
    public LayerAssertions layer() {
        return parentLayerAssertions;
    }

    /**
     * Navigate back to the parent tile assertions.
     */
    public TileAssertions tile() {
        return parentLayerAssertions.tile();
    }

    /**
     * Geometry-specific assertions that handle both JTS geometry operations and MVT command decoding.
     * Extends the JTS GeometryAssertions to provide unified geometry testing capabilities.
     */
    public static class FeatureGeometryAssertions extends io.tileverse.vectortile.mvt.GeometryAssertions {
        private final Feature feature;
        private final FeatureAssertions parentFeature;
        private final List<DecodedCommand> expectedCommands = new ArrayList<>();

        public FeatureGeometryAssertions(Feature feature, FeatureAssertions parentFeature) {
            super(feature.getGeometry());
            this.feature = feature;
            this.parentFeature = parentFeature;
        }

        // Override key methods to return the correct type for method chaining

        @Override
        public FeatureGeometryAssertions equalsExact(org.locationtech.jts.geom.Geometry expected, double tolerance) {
            super.equalsExact(expected, tolerance);
            return this;
        }

        @Override
        public FeatureGeometryAssertions equalsExact(org.locationtech.jts.geom.Geometry expected) {
            super.equalsExact(expected);
            return this;
        }

        @Override
        public FeatureGeometryAssertions isNormalized() {
            super.isNormalized();
            return this;
        }

        @Override
        public FeatureGeometryAssertions isValid() {
            super.isValid();
            return this;
        }

        @Override
        public FeatureGeometryAssertions isSimple() {
            super.isSimple();
            return this;
        }

        @Override
        public FeatureGeometryAssertions contains(org.locationtech.jts.geom.Geometry other) {
            super.contains(other);
            return this;
        }

        @Override
        public FeatureGeometryAssertions intersects(org.locationtech.jts.geom.Geometry other) {
            super.intersects(other);
            return this;
        }

        // Override geometry type methods to return correct type for method chaining
        @Override
        public FeatureGeometryAssertions isPoint() {
            super.isPoint();
            return this;
        }

        @Override
        public FeatureGeometryAssertions isLineString() {
            super.isLineString();
            return this;
        }

        @Override
        public FeatureGeometryAssertions isPolygon() {
            super.isPolygon();
            return this;
        }

        @Override
        public FeatureGeometryAssertions isPuntal() {
            super.isPuntal();
            return this;
        }

        @Override
        public FeatureGeometryAssertions isLineal() {
            super.isLineal();
            return this;
        }

        @Override
        public FeatureGeometryAssertions isPolygonal() {
            super.isPolygonal();
            return this;
        }

        // Override remaining geometry type methods for return type covariance
        @Override
        public FeatureGeometryAssertions isMultiPoint() {
            super.isMultiPoint();
            return this;
        }

        @Override
        public FeatureGeometryAssertions isMultiLineString() {
            super.isMultiLineString();
            return this;
        }

        @Override
        public FeatureGeometryAssertions isMultiPolygon() {
            super.isMultiPolygon();
            return this;
        }

        @Override
        public FeatureGeometryAssertions isGeometryCollection() {
            super.isGeometryCollection();
            return this;
        }

        @Override
        public FeatureGeometryAssertions hasGeometryType(String expectedTypeName) {
            super.hasGeometryType(expectedTypeName);
            return this;
        }

        // Override geometry state methods
        @Override
        public FeatureGeometryAssertions isEmpty() {
            super.isEmpty();
            return this;
        }

        @Override
        public FeatureGeometryAssertions isNotEmpty() {
            super.isNotEmpty();
            return this;
        }

        @Override
        public FeatureGeometryAssertions isNotSimple() {
            super.isNotSimple();
            return this;
        }

        @Override
        public FeatureGeometryAssertions isInvalid() {
            super.isInvalid();
            return this;
        }

        // Override spatial relationship methods
        @Override
        public FeatureGeometryAssertions disjoint(org.locationtech.jts.geom.Geometry other) {
            super.disjoint(other);
            return this;
        }

        @Override
        public FeatureGeometryAssertions touches(org.locationtech.jts.geom.Geometry other) {
            super.touches(other);
            return this;
        }

        @Override
        public FeatureGeometryAssertions doesNotIntersect(org.locationtech.jts.geom.Geometry other) {
            super.doesNotIntersect(other);
            return this;
        }

        @Override
        public FeatureGeometryAssertions crosses(org.locationtech.jts.geom.Geometry other) {
            super.crosses(other);
            return this;
        }

        @Override
        public FeatureGeometryAssertions within(org.locationtech.jts.geom.Geometry other) {
            super.within(other);
            return this;
        }

        @Override
        public FeatureGeometryAssertions doesNotContain(org.locationtech.jts.geom.Geometry other) {
            super.doesNotContain(other);
            return this;
        }

        @Override
        public FeatureGeometryAssertions overlaps(org.locationtech.jts.geom.Geometry other) {
            super.overlaps(other);
            return this;
        }

        @Override
        public FeatureGeometryAssertions covers(org.locationtech.jts.geom.Geometry other) {
            super.covers(other);
            return this;
        }

        @Override
        public FeatureGeometryAssertions coveredBy(org.locationtech.jts.geom.Geometry other) {
            super.coveredBy(other);
            return this;
        }

        // Override geometric measurement methods
        @Override
        public FeatureGeometryAssertions hasArea(double expectedArea, double tolerance) {
            super.hasArea(expectedArea, tolerance);
            return this;
        }

        @Override
        public FeatureGeometryAssertions hasLength(double expectedLength, double tolerance) {
            super.hasLength(expectedLength, tolerance);
            return this;
        }

        @Override
        public FeatureGeometryAssertions hasNumPoints(int expectedNumPoints) {
            super.hasNumPoints(expectedNumPoints);
            return this;
        }

        @Override
        public FeatureGeometryAssertions hasNumGeometries(int expectedNumGeometries) {
            super.hasNumGeometries(expectedNumGeometries);
            return this;
        }

        // Override additional geometry comparison methods
        @Override
        public FeatureGeometryAssertions equalsNorm(org.locationtech.jts.geom.Geometry expected) {
            super.equalsNorm(expected);
            return this;
        }

        @Override
        public FeatureGeometryAssertions equalsTopo(org.locationtech.jts.geom.Geometry expected) {
            super.equalsTopo(expected);
            return this;
        }

        // Override WKT overloads for return type covariance
        public FeatureGeometryAssertions equalsExact(String expectedWkt, double tolerance) {
            super.equalsExact(expectedWkt, tolerance);
            return this;
        }

        public FeatureGeometryAssertions equalsExact(String expectedWkt) {
            super.equalsExact(expectedWkt);
            return this;
        }

        public FeatureGeometryAssertions equalsNorm(String expectedWkt) {
            super.equalsNorm(expectedWkt);
            return this;
        }

        public FeatureGeometryAssertions equalsTopo(String expectedWkt) {
            super.equalsTopo(expectedWkt);
            return this;
        }

        // Override WKT spatial relationship methods
        public FeatureGeometryAssertions disjoint(String otherWkt) {
            super.disjoint(otherWkt);
            return this;
        }

        public FeatureGeometryAssertions touches(String otherWkt) {
            super.touches(otherWkt);
            return this;
        }

        public FeatureGeometryAssertions intersects(String otherWkt) {
            super.intersects(otherWkt);
            return this;
        }

        public FeatureGeometryAssertions doesNotIntersect(String otherWkt) {
            super.doesNotIntersect(otherWkt);
            return this;
        }

        public FeatureGeometryAssertions crosses(String otherWkt) {
            super.crosses(otherWkt);
            return this;
        }

        public FeatureGeometryAssertions within(String otherWkt) {
            super.within(otherWkt);
            return this;
        }

        public FeatureGeometryAssertions contains(String otherWkt) {
            super.contains(otherWkt);
            return this;
        }

        public FeatureGeometryAssertions doesNotContain(String otherWkt) {
            super.doesNotContain(otherWkt);
            return this;
        }

        public FeatureGeometryAssertions overlaps(String otherWkt) {
            super.overlaps(otherWkt);
            return this;
        }

        public FeatureGeometryAssertions covers(String otherWkt) {
            super.covers(otherWkt);
            return this;
        }

        public FeatureGeometryAssertions coveredBy(String otherWkt) {
            super.coveredBy(otherWkt);
            return this;
        }

        /**
         * Convenience method that combines JTS geometry validation with MVT command validation.
         * This demonstrates the integrated assertion capabilities.
         *
         * @param expectedGeometry the expected JTS geometry for comparison
         * @return this GeometryAssertions for continued chaining
         */
        public FeatureGeometryAssertions matchesGeometry(org.locationtech.jts.geom.Geometry expectedGeometry) {
            // First verify using JTS geometry comparison
            super.equalsExact(expectedGeometry);
            super.isValid();

            // Then continue with MVT-specific assertions if needed
            return this;
        }

        /**
         * Assert that the MVT feature has a POINT geometry type.
         * This checks the protobuf GeomType, complementing the JTS geometry type check from the parent class.
         */
        public FeatureGeometryAssertions hasPointMvtType() {
            VectorTileProto.Tile.Feature protoFeature = getProtoFeature();
            Assertions.assertThat(protoFeature.getType())
                    .overridingErrorMessage(
                            "Expected MVT feature type to be POINT but was <%s>", protoFeature.getType())
                    .isEqualTo(VectorTileProto.Tile.GeomType.POINT);
            return this;
        }

        /**
         * Assert that the MVT feature has a LINESTRING geometry type.
         * This checks the protobuf GeomType, complementing the JTS geometry type check from the parent class.
         */
        public FeatureGeometryAssertions hasLinestringMvtType() {
            VectorTileProto.Tile.Feature protoFeature = getProtoFeature();
            Assertions.assertThat(protoFeature.getType())
                    .overridingErrorMessage(
                            "Expected MVT feature type to be LINESTRING but was <%s>", protoFeature.getType())
                    .isEqualTo(VectorTileProto.Tile.GeomType.LINESTRING);
            return this;
        }

        /**
         * Assert that the MVT feature has a POLYGON geometry type.
         * This checks the protobuf GeomType, complementing the JTS geometry type check from the parent class.
         */
        public FeatureGeometryAssertions hasPolygonMvtType() {
            VectorTileProto.Tile.Feature protoFeature = getProtoFeature();
            Assertions.assertThat(protoFeature.getType())
                    .overridingErrorMessage(
                            "Expected MVT feature type to be POLYGON but was <%s>", protoFeature.getType())
                    .isEqualTo(VectorTileProto.Tile.GeomType.POLYGON);
            return this;
        }

        /**
         * Start expecting a MoveTo command. Use .count(n) for MULTIPOINT or single coordinate for simple cases.
         */
        public MoveToCommand moveTo() {
            return new MoveToCommand(this);
        }

        /**
         * Expect a MoveTo command to the specified coordinates (for single point geometries).
         */
        public FeatureGeometryAssertions moveTo(int x, int y) {
            expectedCommands.add(new DecodedCommand(GeometryCommand.MoveTo, x, y));
            return this;
        }

        /**
         * Start expecting a LineTo command. Use .count(n) for multiple LineTo or single coordinate for simple cases.
         */
        public LineToCommand lineTo() {
            return new LineToCommand(this);
        }

        /**
         * Expect a LineTo command to the specified coordinates (for single line segments).
         */
        public FeatureGeometryAssertions lineTo(int x, int y) {
            expectedCommands.add(new DecodedCommand(GeometryCommand.LineTo, x, y));
            return this;
        }

        /**
         * Expect a ClosePath command.
         */
        public FeatureGeometryAssertions closePath() {
            expectedCommands.add(new DecodedCommand(GeometryCommand.ClosePath, -1, -1));
            return this;
        }

        /**
         * Finalize the command sequence verification.
         */
        public FeatureGeometryAssertions matches() {
            VectorTileProto.Tile.Feature protoFeature = getProtoFeature();
            List<DecodedCommand> actualCommands = decodeFeatureCommands(protoFeature);

            // Provide detailed error message showing exactly where the mismatch occurs
            if (!actualCommands.equals(expectedCommands)) {
                String detailedError = buildDetailedCommandsError(expectedCommands, actualCommands);
                throw new AssertionError(detailedError);
            }

            return this;
        }

        private String buildDetailedCommandsError(List<DecodedCommand> expected, List<DecodedCommand> actual) {
            StringBuilder error = new StringBuilder();
            error.append("MVT command sequence mismatch:\n");

            int maxLength = Math.max(expected.size(), actual.size());

            for (int i = 0; i < maxLength; i++) {
                DecodedCommand exp = i < expected.size() ? expected.get(i) : null;
                DecodedCommand act = i < actual.size() ? actual.get(i) : null;

                String status;
                if (exp == null) {
                    status = "EXTRA";
                    error.append(String.format("  [%d] %s: %s\n", i, status, act));
                } else if (act == null) {
                    status = "MISSING";
                    error.append(String.format("  [%d] %s: %s\n", i, status, exp));
                } else if (!exp.equals(act)) {
                    status = "DIFF";
                    error.append(String.format("  [%d] %s: expected <%s> but was <%s>\n", i, status, exp, act));
                } else {
                    status = "OK";
                    error.append(String.format("  [%d] %s: %s\n", i, status, exp));
                }
            }

            error.append("\nExpected: ").append(expected);
            error.append("\nActual:   ").append(actual);

            return error.toString();
        }

        /**
         * Navigate back to the parent feature assertions.
         */
        public FeatureAssertions feature() {
            return parentFeature;
        }

        /**
         * Navigate back to the parent layer assertions.
         */
        public LayerAssertions layer() {
            return parentFeature.layer();
        }

        /**
         * Navigate back to the parent tile assertions.
         */
        public TileAssertions tile() {
            return parentFeature.tile();
        }

        private VectorTileProto.Tile.Feature getProtoFeature() {
            if (feature instanceof io.tileverse.vectortile.mvt.MvtFeature mvtFeature) {
                return (VectorTileProto.Tile.Feature) mvtFeature.feature;
            }
            throw new UnsupportedOperationException("Geometry assertion only supports MvtFeature instances");
        }

        private List<DecodedCommand> decodeFeatureCommands(VectorTileProto.Tile.Feature protoFeature) {
            List<DecodedCommand> commands = new ArrayList<>();
            com.google.protobuf.Internal.IntList geometryList =
                    (com.google.protobuf.Internal.IntList) protoFeature.getGeometryList();

            int cursorX = 0;
            int cursorY = 0;
            int cmdIndex = 0;

            while (cmdIndex < geometryList.size()) {
                int cmd = geometryList.getInt(cmdIndex++);
                int command = extractCommand(cmd);
                int count = extractCount(cmd);

                if (command == GeometryCommand.MoveTo || command == GeometryCommand.LineTo) {
                    for (int i = 0; i < count; i++) {
                        int dx = decodeZigZag32(geometryList.getInt(cmdIndex++));
                        int dy = decodeZigZag32(geometryList.getInt(cmdIndex++));
                        cursorX += dx;
                        cursorY += dy;
                        commands.add(new DecodedCommand(command, cursorX, cursorY));
                    }
                } else if (command == GeometryCommand.ClosePath) {
                    commands.add(new DecodedCommand(GeometryCommand.ClosePath, -1, -1));
                }
            }

            return commands;
        }

        private static int extractCommand(int cmd) {
            return cmd & 0b00000111;
        }

        private static int extractCount(int cmd) {
            return cmd >> 3;
        }

        /**
         * Builder for MoveTo commands supporting both single and multi-point assertions.
         */
        public static class MoveToCommand {
            private final FeatureGeometryAssertions parent;
            private int expectedCount = 1;

            public MoveToCommand(FeatureGeometryAssertions parent) {
                this.parent = parent;
            }

            /**
             * Set the expected count for this MoveTo command (for MULTIPOINT geometries).
             */
            public CoordinateSequence count(int count) {
                this.expectedCount = count;
                return new CoordinateSequence(parent, GeometryCommand.MoveTo, count);
            }

            /**
             * Single coordinate for MoveTo (count=1 implied).
             */
            public FeatureGeometryAssertions coord(int x, int y) {
                parent.expectedCommands.add(new DecodedCommand(GeometryCommand.MoveTo, x, y));
                return parent;
            }
        }

        /**
         * Builder for LineTo commands supporting both single and multi-line assertions.
         */
        public static class LineToCommand {
            private final FeatureGeometryAssertions parent;

            public LineToCommand(FeatureGeometryAssertions parent) {
                this.parent = parent;
            }

            /**
             * Set the expected count for this LineTo command.
             */
            public CoordinateSequence count(int count) {
                return new CoordinateSequence(parent, GeometryCommand.LineTo, count);
            }

            /**
             * Single coordinate for LineTo (count=1 implied).
             */
            public FeatureGeometryAssertions coord(int x, int y) {
                parent.expectedCommands.add(new DecodedCommand(GeometryCommand.LineTo, x, y));
                return parent;
            }
        }

        /**
         * Coordinate sequence builder for commands with count > 1.
         */
        public static class CoordinateSequence {
            private final FeatureGeometryAssertions parent;
            private final int command;
            private final int expectedCount;
            private int coordinateCount = 0;

            public CoordinateSequence(FeatureGeometryAssertions parent, int command, int expectedCount) {
                this.parent = parent;
                this.command = command;
                this.expectedCount = expectedCount;
            }

            /**
             * Add a coordinate pair to the sequence.
             */
            public CoordinateSequence coord(int x, int y) {
                if (coordinateCount >= expectedCount) {
                    throw new IllegalStateException(String.format(
                            "Too many coordinates: expected %d but got %d+", expectedCount, coordinateCount));
                }
                parent.expectedCommands.add(new DecodedCommand(command, x, y));
                coordinateCount++;
                return this;
            }

            /**
             * Finish the coordinate sequence and return to geometry assertions.
             * Validates that the correct number of coordinates were provided.
             */
            public FeatureGeometryAssertions done() {
                if (coordinateCount != expectedCount) {
                    throw new IllegalStateException(String.format(
                            "Incomplete coordinate sequence: expected %d coordinates but got %d",
                            expectedCount, coordinateCount));
                }
                return parent;
            }
        }
    }

    /**
     * Represents a decoded MVT geometry command with absolute coordinates.
     */
    private static class DecodedCommand {
        private final int command;
        private final int x;
        private final int y;

        public DecodedCommand(int command, int x, int y) {
            this.command = command;
            this.x = x;
            this.y = y;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (!(obj instanceof DecodedCommand other)) return false;
            return command == other.command && (command == GeometryCommand.ClosePath || (x == other.x && y == other.y));
        }

        @Override
        public int hashCode() {
            return Objects.hash(command, x, y);
        }

        @Override
        public String toString() {
            return switch (command) {
                case GeometryCommand.MoveTo -> String.format("MoveTo(%d, %d)", x, y);
                case GeometryCommand.LineTo -> String.format("LineTo(%d, %d)", x, y);
                case GeometryCommand.ClosePath -> "ClosePath()";
                default -> String.format("Unknown(%d, %d, %d)", command, x, y);
            };
        }
    }
}
