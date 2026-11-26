/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.benchmarks;

import static org.assertj.core.api.Assertions.assertThat;

import io.tileverse.vectortile.benchmarks.TileFeatures.InputFeature;
import io.tileverse.vectortile.benchmarks.TileFeatures.LayerFeatures;
import io.tileverse.vectortile.model.GeometryReader;
import io.tileverse.vectortile.model.VectorTile;
import io.tileverse.vectortile.model.VectorTile.Layer;
import io.tileverse.vectortile.model.VectorTile.Layer.Feature;
import io.tileverse.vectortile.mvt.Geometries;
import io.tileverse.vectortile.mvt.VectorTileCodec;
import io.tileverse.vectortile.pipeline.GeometryPipeline;
import io.tileverse.vectortile.pipeline.GeometryStep;
import io.tileverse.vectortile.pipeline.PolygonRepairStep;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.locationtech.jts.algorithm.Area;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;

/**
 * Compares the tiles written by tileverse and by java-vector-tile from the same features, feature by feature.
 *
 * <p>The tile of java-vector-tile is decoded by the lenient codec of tileverse: it decodes a malformed geometry as an
 * empty one. The tile of tileverse must have well-formed geometries, and the layers of both tiles the extent of the
 * configurations: the comparison fails otherwise. A written feature is matched with its input feature by layer name and
 * id: the input features of a layer have ids of their own. The features written for an input feature must be the same
 * in both tiles, in order: the same geometry once its repeated consecutive points are removed, compared with
 * {@code equalsExact}, and the same attributes. The repeated points are removed because java-vector-tile writes the
 * last points of a ring when they round to its first point, and tileverse leaves them out.
 *
 * <p>The input features matching an {@link Exclusion} are left out of the comparison. An input feature written by
 * neither writer counts as dropped. A feature written with a layer name and an id of no input feature is a mismatch,
 * whatever the exclusions. The exclusions on rings and on splits check the geometries handed to the encoders: the input
 * geometry once filtered, simplified and clipped, as set by the configuration, in the units of the input. tileverse
 * runs these steps in its pipeline, and java-vector-tile runs the same JTS operations. The rules multiply the
 * coordinates by the input scale of the configuration before rounding them, as both encoders do.
 */
final class TileComparison {

    // A malformed geometry written by tileverse fails the decoding, before an exclusion can leave its feature out
    private static final VectorTileCodec TILEVERSE_CODEC = VectorTileCodec.failingOnMalformedGeometry();
    private static final VectorTileCodec JAVA_VECTOR_TILE_CODEC = new VectorTileCodec();

    /** Decodes the geometries with the coordinate sequences of the input features. */
    private static final GeometryReader GEOMETRY_READER =
            VectorTileCodec.newGeometryReader().withGeometryFactory(new GeometryFactory());

    private final Map<FeatureKey, List<WrittenFeature>> tileverse;
    private final Map<FeatureKey, List<WrittenFeature>> javaVectorTile;
    private final WriterConfiguration configuration;
    private final Set<FeatureKey> repaired;
    private final GeometryPipeline stepsBeforeEncoding;

    /** The number of tile units in one unit of the input: the encoders multiply the coordinates by it, then round. */
    private final double inputScale;

    private final List<Mismatch> mismatches = new ArrayList<>();
    private final Map<Exclusion, Integer> exclusions = new EnumMap<>(Exclusion.class);
    private int comparedFeatures;
    private int droppedFeatures;

    private TileComparison(
            Map<FeatureKey, List<WrittenFeature>> tileverse,
            Map<FeatureKey, List<WrittenFeature>> javaVectorTile,
            WriterConfiguration configuration,
            Set<FeatureKey> repaired) {
        this.tileverse = tileverse;
        this.javaVectorTile = javaVectorTile;
        this.configuration = configuration;
        this.repaired = repaired;
        this.stepsBeforeEncoding = stepsBeforeEncoding(configuration);
        this.inputScale = configuration.inputScale();
        for (Exclusion exclusion : Exclusion.values()) {
            exclusions.put(exclusion, 0);
        }
    }

    /**
     * Compares the tiles written by both writers from the input features.
     *
     * @param tileverse the tile written by tileverse
     * @param javaVectorTile the tile written by java-vector-tile
     * @param input the features given to both writers
     * @param configuration the configuration of both writers
     * @param repaired the features repaired by java-vector-tile once encoded, by layer name and id
     * @throws IOException if a tile cannot be decoded, as the tile of tileverse with a malformed geometry
     */
    static TileComparison compare(
            byte[] tileverse,
            byte[] javaVectorTile,
            TileFeatures input,
            WriterConfiguration configuration,
            Set<FeatureKey> repaired)
            throws IOException {
        VectorTile decodedTileverse = TILEVERSE_CODEC.decode(tileverse);
        VectorTile decodedJavaVectorTile = JAVA_VECTOR_TILE_CODEC.decode(javaVectorTile);
        requireConfiguredExtent(decodedTileverse, "tileverse");
        requireConfiguredExtent(decodedJavaVectorTile, "java-vector-tile");
        TileComparison comparison = new TileComparison(
                writtenFeatures(decodedTileverse), writtenFeatures(decodedJavaVectorTile), configuration, repaired);
        Set<FeatureKey> inputKeys = new HashSet<>();
        for (LayerFeatures layer : input.layers()) {
            for (InputFeature feature : layer.features()) {
                inputKeys.add(new FeatureKey(layer.name(), feature.id()));
                comparison.compareFeature(layer.name(), feature);
            }
        }
        comparison.addFeaturesWithoutInput(inputKeys);
        return comparison;
    }

    /**
     * The input features written differently by the two writers, in input order, then the features written without an
     * input feature.
     */
    List<Mismatch> mismatches() {
        return Collections.unmodifiableList(mismatches);
    }

    /** The number of input features written by at least one writer, and not excluded. */
    int comparedFeatures() {
        return comparedFeatures;
    }

    /** The number of input features left out of the comparison, the exclusions together. */
    int excludedFeatures() {
        return exclusions.values().stream().mapToInt(Integer::intValue).sum();
    }

    /**
     * The counts of the comparison in one line: features compared, dropped and excluded, with the count per exclusion.
     */
    String summary() {
        int inputFeatures = comparedFeatures + droppedFeatures + excludedFeatures();
        String excludedByRule = exclusions.entrySet().stream()
                .map(entry -> entry.getKey().description + ": " + entry.getValue())
                .collect(Collectors.joining(", "));
        return inputFeatures + " features, " + comparedFeatures + " compared, " + droppedFeatures
                + " dropped by both writers, " + excludedFeatures() + " excluded (" + excludedByRule + ")";
    }

    /**
     * The steps run by both writers before they encode a geometry: the pipeline of the tileverse builder of the
     * configuration, without its {@link PolygonRepairStep}. java-vector-tile repairs a polygon once it has encoded it.
     */
    private static GeometryPipeline stepsBeforeEncoding(WriterConfiguration configuration) {
        GeometryPipeline tileversePipeline = configuration.newTileverse().pipeline();
        List<GeometryStep> steps = new ArrayList<>(tileversePipeline.steps());
        steps.removeIf(PolygonRepairStep.class::isInstance);
        return GeometryPipeline.of(steps.toArray(GeometryStep[]::new));
    }

    /**
     * Fails unless each layer of the tile has the extent of the configurations. The coordinates of layers of other
     * extents are positions in other grids: equal features would be at different places.
     */
    private static void requireConfiguredExtent(VectorTile tile, String writer) {
        for (Layer layer : tile.getLayers()) {
            assertThat(layer.getExtent())
                    .as("the extent of layer %s written by %s", layer.getName(), writer)
                    .isEqualTo(WriterConfiguration.EXTENT);
        }
    }

    /** Groups the features of a written tile by layer name and id, in tile order. */
    private static Map<FeatureKey, List<WrittenFeature>> writtenFeatures(VectorTile decoded) {
        Map<FeatureKey, List<WrittenFeature>> features = new HashMap<>();
        for (Layer layer : decoded.getLayers()) {
            List<Feature> layerFeatures =
                    layer.getFeatures(feature -> true, GEOMETRY_READER).toList();
            for (Feature feature : layerFeatures) {
                FeatureKey key = new FeatureKey(layer.getName(), feature.getId());
                features.computeIfAbsent(key, k -> new ArrayList<>()).add(WrittenFeature.of(feature));
            }
        }
        return features;
    }

    /**
     * Adds a mismatch for each layer name and id written by either writer without an input feature: a writer wrote a
     * feature of its own, or changed an id. The exclusions do not apply.
     */
    private void addFeaturesWithoutInput(Set<FeatureKey> inputKeys) {
        Set<FeatureKey> writtenKeys = new LinkedHashSet<>(tileverse.keySet());
        writtenKeys.addAll(javaVectorTile.keySet());
        writtenKeys.removeAll(inputKeys);
        for (FeatureKey key : writtenKeys) {
            List<WrittenFeature> byTileverse = tileverse.getOrDefault(key, List.of());
            List<WrittenFeature> byJavaVectorTile = javaVectorTile.getOrDefault(key, List.of());
            mismatches.add(new Mismatch(key.layer(), key.id(), byTileverse, byJavaVectorTile));
        }
    }

    private void compareFeature(String layer, InputFeature input) {
        FeatureKey key = new FeatureKey(layer, input.id());
        List<WrittenFeature> byTileverse = tileverse.getOrDefault(key, List.of());
        List<WrittenFeature> byJavaVectorTile = javaVectorTile.getOrDefault(key, List.of());
        List<Geometry> encoded = encodedGeometries(input.geometry());
        Exclusion exclusion = exclusionOf(key, encoded, byTileverse, byJavaVectorTile);
        if (exclusion != null) {
            exclusions.merge(exclusion, 1, Integer::sum);
        } else if (byTileverse.isEmpty() && byJavaVectorTile.isEmpty()) {
            droppedFeatures++;
        } else {
            compareWritten(key, byTileverse, byJavaVectorTile);
        }
    }

    /** The records compare their geometries with {@code equalsExact}, as {@code Geometry.equals} does. */
    private void compareWritten(
            FeatureKey key, List<WrittenFeature> byTileverse, List<WrittenFeature> byJavaVectorTile) {
        comparedFeatures++;
        if (!byTileverse.equals(byJavaVectorTile)) {
            mismatches.add(new Mismatch(key.layer(), key.id(), byTileverse, byJavaVectorTile));
        }
    }

    /**
     * The geometries handed to the encoders for the input geometry, in the units of the input: none when the steps drop
     * the feature, one per member when they split a collection.
     */
    private List<Geometry> encodedGeometries(Geometry input) {
        List<Geometry> encoded = new ArrayList<>();
        stepsBeforeEncoding.run(input, encoded::add);
        return encoded;
    }

    /** Returns the first exclusion matching the input feature, in the order of {@link Exclusion}, or null. */
    private Exclusion exclusionOf(
            FeatureKey key,
            List<Geometry> encoded,
            List<WrittenFeature> byTileverse,
            List<WrittenFeature> byJavaVectorTile) {
        if (repaired.contains(key)) {
            return Exclusion.REPAIRED;
        }
        if (anyRing(encoded, this::hasZeroAreaOnceRounded)) {
            return Exclusion.ZERO_AREA_RING;
        }
        if (anyRing(encoded, this::changesOrientationWithRounding)) {
            return Exclusion.ORIENTATION_CHANGES_WITH_ROUNDING;
        }
        if (splitByClip(encoded) && (byTileverse.size() > 1 || byJavaVectorTile.size() > 1)) {
            return Exclusion.SPLIT_BY_CLIP;
        }
        if (byJavaVectorTile.stream().anyMatch(WrittenFeature::isEmpty)) {
            return Exclusion.EMPTY_IN_JAVA_VECTOR_TILE;
        }
        return null;
    }

    /** Whether a ring of the polygons of the geometries meets the condition: never for points and lines. */
    private static boolean anyRing(List<Geometry> geometries, Predicate<LinearRing> condition) {
        for (Geometry geometry : geometries) {
            if (polygonRings(geometry).stream().anyMatch(condition)) {
                return true;
            }
        }
        return false;
    }

    /** The rings of the polygons of the geometry. */
    private static List<LinearRing> polygonRings(Geometry geometry) {
        List<LinearRing> rings = new ArrayList<>();
        for (int i = 0; i < geometry.getNumGeometries(); i++) {
            if (geometry.getGeometryN(i) instanceof Polygon polygon && !polygon.isEmpty()) {
                addRings(polygon, rings);
            }
        }
        return rings;
    }

    private static void addRings(Polygon polygon, List<LinearRing> rings) {
        rings.add(polygon.getExteriorRing());
        for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
            rings.add(polygon.getInteriorRingN(i));
        }
    }

    /** Whether the ring collapsed to a line or a point once the encoders rounded its points. */
    private boolean hasZeroAreaOnceRounded(LinearRing ring) {
        return roundedSurveyorSum(ring) == 0;
    }

    /**
     * Whether the surveyor's sums of the ring over its rounded and over its unrounded coordinates differ in sign. A
     * zero on either side counts as a change.
     */
    private boolean changesOrientationWithRounding(LinearRing ring) {
        int roundedSign = Long.signum(roundedSurveyorSum(ring));
        int unroundedSign = (int) Math.signum(unroundedSurveyorSum(ring));
        return roundedSign == 0 || roundedSign != unroundedSign;
    }

    /**
     * The surveyor's sum of the ring over its unrounded coordinates. java-vector-tile orients the ring with the signed
     * area of JTS: half of this sum, with the opposite sign. It computes the area in the units of its input, and the
     * scale of a configuration with scaled input keeps the sign.
     */
    private static double unroundedSurveyorSum(LinearRing ring) {
        return -2 * Area.ofRingSigned(ring.getCoordinates());
    }

    /**
     * The surveyor's sum of the ring over the coordinates written by the encoders: twice its signed area, in tile
     * units.
     */
    private long roundedSurveyorSum(LinearRing ring) {
        CoordinateSequence points = ring.getCoordinateSequence();
        long sum = 0;
        for (int i = 1; i < points.size(); i++) {
            long previousX = written(points.getX(i - 1));
            long previousY = written(points.getY(i - 1));
            sum += previousX * written(points.getY(i)) - written(points.getX(i)) * previousY;
        }
        return sum;
    }

    /** The ordinate as written by the encoders: multiplied by the input scale, then rounded. */
    private long written(double ordinate) {
        return Math.round(ordinate * inputScale);
    }

    /**
     * Whether the clip split the input feature: it returned a collection, and the steps handed its members to the
     * encoders. The other steps of the configurations return no collection.
     */
    private boolean splitByClip(List<Geometry> encoded) {
        return configuration.clips() && encoded.size() > 1;
    }

    /**
     * The differences between the writers which leave an input feature out of the comparison, checked in this order.
     */
    enum Exclusion {
        /** java-vector-tile repaired the encoded polygon, and tileverse repairs polygons another way. */
        REPAIRED("repaired by java-vector-tile"),

        /**
         * A ring of an encoded polygon has zero area once rounded, such as a sliver left by the clip along its edge.
         * tileverse leaves the ring out, and java-vector-tile writes it: the decoder skips the ring, and groups the
         * other rings into other polygons. With simplification, tileverse also repairs the polygon, as invalid once
         * rounded, and the rings of the repaired polygon start at another vertex: the outputs are equal once
         * normalized. java-vector-tile keeps the polygon, because its check decodes the polygon without its rings of
         * zero area.
         */
        ZERO_AREA_RING("ring of zero area once rounded"),

        /**
         * The orientation of a ring of an encoded polygon changes with rounding: the surveyor's sums over its rounded
         * and over its unrounded coordinates differ in sign, and a zero on either side counts as a change.
         * java-vector-tile orients a ring on its unrounded coordinates, the specification applies the winding rule to
         * the written ones, and such a ring has an area of zero or close to it.
         */
        ORIENTATION_CHANGES_WITH_ROUNDING("ring orientation changes with rounding"),

        /**
         * A writer wrote several features for the input feature, from a collection returned by the clip.
         * java-vector-tile runs its steps before the clip again on the members, and tileverse does not.
         */
        SPLIT_BY_CLIP("split by the clip"),

        /**
         * The feature written by java-vector-tile decodes to an empty geometry: a line collapsed to a lone MoveTo, or a
         * MultiPoint with repeated points, written by java-vector-tile with a wrong MoveTo count.
         */
        EMPTY_IN_JAVA_VECTOR_TILE("empty in java-vector-tile");

        private final String description;

        Exclusion(String description) {
            this.description = description;
        }
    }

    /**
     * An input feature written differently by the two writers, or a layer name and an id written without an input
     * feature, with the features written for it by tileverse and by java-vector-tile: none when a writer left it out.
     *
     * @param layer the name of the layer of the feature
     * @param id the id of the feature
     * @param tileverse the features written by tileverse with this layer name and id
     * @param javaVectorTile the features written by java-vector-tile with this layer name and id
     */
    record Mismatch(String layer, long id, List<WrittenFeature> tileverse, List<WrittenFeature> javaVectorTile) {

        @Override
        public String toString() {
            return "layer " + layer + ", id " + id + "\n  tileverse:        " + describe(tileverse)
                    + "\n  java-vector-tile: " + describe(javaVectorTile);
        }

        private static String describe(List<WrittenFeature> features) {
            if (features.isEmpty()) {
                return "no feature";
            }
            return features.stream().map(WrittenFeature::toString).collect(Collectors.joining(", "));
        }
    }

    /**
     * A feature of a written tile, as compared: its geometry without repeated consecutive points, and its attributes.
     *
     * @param geometry the decoded geometry, without repeated consecutive points
     * @param attributes the decoded attributes
     */
    record WrittenFeature(Geometry geometry, Map<String, Object> attributes) {

        /**
         * The length of the text of a geometry shown in a mismatch: a polygon of the benchmark tiles has 35,402 points.
         */
        private static final int SHOWN_TEXT_LENGTH = 500;

        static WrittenFeature of(Feature feature) {
            Geometry geometry = Geometries.withoutRepeatedPoints(feature.getGeometry());
            return new WrittenFeature(geometry, feature.getAttributes());
        }

        boolean isEmpty() {
            return geometry.isEmpty();
        }

        @Override
        public String toString() {
            return shownText(geometry) + " " + attributes;
        }

        private static String shownText(Geometry geometry) {
            String text = geometry.toText();
            if (text.length() <= SHOWN_TEXT_LENGTH) {
                return text;
            }
            return text.substring(0, SHOWN_TEXT_LENGTH) + "... (" + geometry.getNumPoints() + " points)";
        }
    }

    /**
     * The key of a written feature: the name of its layer, and its id.
     *
     * @param layer the name of the layer of the feature
     * @param id the id of the feature
     */
    record FeatureKey(String layer, long id) {}
}
