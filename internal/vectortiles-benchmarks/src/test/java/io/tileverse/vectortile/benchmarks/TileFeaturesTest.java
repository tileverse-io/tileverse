/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.benchmarks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import io.tileverse.vectortile.benchmarks.TileFeatures.InputFeature;
import io.tileverse.vectortile.benchmarks.TileFeatures.LayerFeatures;
import io.tileverse.vectortile.mvt.VectorTileBuilder;
import io.tileverse.vectortile.mvt.VectorTileCodec;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.impl.PackedCoordinateSequence;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

class TileFeaturesTest {

    private static final GeometryFactory FACTORY = new GeometryFactory();

    private static final String POLYGON = "POLYGON ((0 0, 20 0, 20 20, 0 20, 0 0), (5 5, 5 10, 10 10, 5 5))";

    @Test
    void numbersFeaturesFromOne() throws IOException {
        VectorTileBuilder builder = new VectorTileBuilder().setAutoIncrementIds(false);
        builder.layer()
                .name("roads")
                .feature(Map.of("name", "main"), geom("LINESTRING (0 0, 10 10)"), 50)
                .feature(Map.of(), geom("LINESTRING (0 10, 10 0)"), 60)
                .build();
        builder.layer().name("pois").feature(Map.of(), geom("POINT (5 5)"), 70).build();

        TileFeatures features = TileFeatures.decode(builder.toByteArray());

        List<LayerFeatures> layers = features.layers();
        assertThat(layers).extracting(LayerFeatures::name).containsExactly("roads", "pois");
        assertThat(layers.get(0).features()).extracting(InputFeature::id).containsExactly(1L, 2L);
        assertThat(layers.get(0).features().get(0).attributes()).isEqualTo(Map.of("name", "main"));
        assertThat(layers.get(1).features()).extracting(InputFeature::id).containsExactly(1L);
        assertThat(features.featureCount()).isEqualTo(3);
    }

    @Test
    void jitterKeepsRoundedPoints() {
        Geometry polygon = geom(POLYGON);
        Geometry line = geom("LINESTRING (3 4, 7 9, 12 1)");
        TileFeatures features = tileOf(polygon, line);

        TileFeatures jittered = features.jittered(42);

        Polygon jitteredPolygon = (Polygon) geometryOf(jittered, 0);
        assertMovedBelowHalf(polygon, jitteredPolygon);
        assertThat(jitteredPolygon.getExteriorRing().isClosed()).isTrue();
        assertThat(jitteredPolygon.getInteriorRingN(0).isClosed()).isTrue();
        assertMovedBelowHalf(line, geometryOf(jittered, 1));
        assertThat(features.jittered(42)).isEqualTo(jittered);
        assertThat(polygon).isEqualTo(geom(POLYGON));
    }

    // The validity benchmark measures the checks on the polygons of the default reader, jittered
    @Test
    void jitterKeepsPackedSequences() throws IOException {
        VectorTileBuilder builder = new VectorTileBuilder();
        builder.layer().name("land").feature(Map.of(), geom(POLYGON)).build();

        TileFeatures decoded = TileFeatures.decode(builder.toByteArray(), VectorTileCodec.newGeometryReader());

        Polygon polygon = (Polygon) geometryOf(decoded.jittered(42), 0);
        assertThat(polygon.getExteriorRing().getCoordinateSequence()).isInstanceOf(PackedCoordinateSequence.class);
        assertThat(polygon.getInteriorRingN(0).getCoordinateSequence()).isInstanceOf(PackedCoordinateSequence.class);
    }

    @Test
    void packedCopiesSequences() {
        Geometry polygon = geom(POLYGON);
        Geometry line = geom("LINESTRING (3 4, 7 9, 12 1)");

        TileFeatures packed = tileOf(polygon, line).packed();

        Polygon packedPolygon = (Polygon) geometryOf(packed, 0);
        LineString packedLine = (LineString) geometryOf(packed, 1);
        assertThat(packedPolygon.getExteriorRing().getCoordinateSequence())
                .isExactlyInstanceOf(PackedCoordinateSequence.Double.class);
        assertThat(packedPolygon.getInteriorRingN(0).getCoordinateSequence())
                .isExactlyInstanceOf(PackedCoordinateSequence.Double.class);
        assertThat(packedLine.getCoordinateSequence()).isExactlyInstanceOf(PackedCoordinateSequence.Double.class);
        assertThat(packedPolygon).isEqualTo(polygon);
        assertThat(packedLine).isEqualTo(line);
    }

    @Test
    void scalesCoordinates() {
        TileFeatures features = tileOf(geom("POLYGON ((0 0, 32 0, 32 16, 0 0))"), geom("POINT (64 8)"));

        TileFeatures scaled = features.scaled(1.0 / 16);

        assertThat(geometryOf(scaled, 0)).isEqualTo(geom("POLYGON ((0 0, 2 0, 2 1, 0 0))"));
        assertThat(geometryOf(scaled, 1)).isEqualTo(geom("POINT (4 0.5)"));
    }

    @Test
    void scalesLayersToExtent() {
        Geometry line = geom("LINESTRING (2 4, 6 8)");
        LayerFeatures small = new LayerFeatures("small", 512, List.of(new InputFeature(1, Map.of(), line)));
        LayerFeatures large = new LayerFeatures("large", 4096, List.of(new InputFeature(1, Map.of(), line)));
        TileFeatures features = new TileFeatures(List.of(small, large));

        TileFeatures inExtent = features.toExtent(4096);

        assertThat(inExtent.layers()).extracting(LayerFeatures::extent).containsExactly(4096, 4096);
        assertThat(geometryOf(inExtent, 0)).isEqualTo(geom("LINESTRING (16 32, 48 64)"));
        assertThat(inExtent.layers().get(1)).isEqualTo(large);
    }

    /** Checks the points moved off their places by at most 0.49 on both axes: they round back to their places. */
    private static void assertMovedBelowHalf(Geometry original, Geometry moved) {
        Coordinate[] from = original.getCoordinates();
        Coordinate[] to = moved.getCoordinates();
        assertThat(to).hasSameSizeAs(from);
        for (int i = 0; i < from.length; i++) {
            assertThat(to[i].x).isCloseTo(from[i].x, within(0.49)).isNotEqualTo(from[i].x);
            assertThat(to[i].y).isCloseTo(from[i].y, within(0.49)).isNotEqualTo(from[i].y);
        }
    }

    /** A tile of one layer with a feature per geometry, numbered from 1. */
    private static TileFeatures tileOf(Geometry... geometries) {
        List<InputFeature> features = new ArrayList<>();
        for (Geometry geometry : geometries) {
            features.add(new InputFeature(features.size() + 1L, Map.of(), geometry));
        }
        return new TileFeatures(List.of(new LayerFeatures("layer", 4096, features)));
    }

    private static Geometry geometryOf(TileFeatures features, int index) {
        return features.layers().get(0).features().get(index).geometry();
    }

    private static Geometry geom(String wkt) {
        try {
            return new WKTReader(FACTORY).read(wkt);
        } catch (ParseException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
