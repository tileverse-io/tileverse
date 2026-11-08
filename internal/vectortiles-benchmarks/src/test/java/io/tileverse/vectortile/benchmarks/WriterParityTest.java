/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.benchmarks;

import static io.tileverse.vectortile.benchmarks.WriterConfiguration.CLIP_BUFFER;
import static io.tileverse.vectortile.benchmarks.WriterConfiguration.EXTENT;
import static io.tileverse.vectortile.benchmarks.WriterConfiguration.SIMPLIFICATION_TOLERANCE;
import static org.assertj.core.api.Assertions.assertThat;

import io.tileverse.vectortile.benchmarks.TileComparison.FeatureKey;
import io.tileverse.vectortile.benchmarks.TileComparison.Mismatch;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import no.ecc.vectortile.VectorTileEncoder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Checks the tiles written by tileverse against the tiles written by java-vector-tile 1.4.2 from the same features, in
 * the {@link WriterConfiguration}s. {@link TileComparison} states the rules of the comparison.
 *
 * <p>The features come from the four tiles committed in tileverse-vectortiles, and from the tiles of
 * {@code run-benchmarks.sh} once the script downloaded them. A layer of another extent than the writers' is scaled to
 * their extent first. A tile is written with its features as decoded, then jittered: the coordinates moved by less than
 * half a tile unit, off the integers, to the same rounded values when they were integers.
 */
class WriterParityTest {

    /** The system property naming the directory of the tiles committed in tileverse-vectortiles. */
    private static final String COMMITTED_TILES_DIRECTORY = "tileverse.vectortiles.testResources";

    /** The system property naming the directory of the tiles downloaded by {@code run-benchmarks.sh}. */
    private static final String BENCHMARK_TILES_DIRECTORY = "tileverse.benchmarks.tiles";

    private static final List<String> COMMITTED_TILES =
            List.of("14-8801-5371.vector.pbf", "bigtile.vector.pbf", "cells-11-1058-568.mvt", "cells-11-1065-567.mvt");

    /** The tiles of {@code run-benchmarks.sh}, in its order. */
    private static final List<String> BENCHMARK_TILES = List.of(
            "osm-qa-montevideo/12-1408-2471.mvt",
            "chicago/13-2101-3044.mvt",
            "bing/4-12-6.mvt",
            "amazon/5_6_21.mvt",
            "amazon_here/4_9_4.mvt",
            "omt/4_8_10.mvt",
            "omt/14_8298_10748.mvt");

    private static final long JITTER_SEED = 42;

    private static final int MISMATCHES_SHOWN = 10;

    @ParameterizedTest(name = "{0}, {1}, jittered: {2}")
    @MethodSource("tilesAndConfigurations")
    void writesSameFeatures(String tile, WriterConfiguration configuration, boolean jittered) throws IOException {
        TileFeatures input = inputFeatures(tile, configuration, jittered);

        byte[] tileverse = TileWriters.writeTileverse(input, configuration.newTileverse());
        VectorTileEncoder encoder = javaVectorTileEncoder(configuration);
        byte[] javaVectorTile = TileWriters.writeJavaVectorTile(input, encoder);
        Set<FeatureKey> repaired = repairedFeatures(encoder, javaVectorTile);
        TileComparison comparison = TileComparison.compare(tileverse, javaVectorTile, input, configuration, repaired);

        String testCase = configuration + " on " + tile + (jittered ? ", jittered" : ", as decoded");
        System.out.println(testCase + ": " + comparison.summary());
        List<Mismatch> mismatches = comparison.mismatches();
        assertThat(mismatches)
                .withFailMessage(() -> mismatchMessage(testCase, mismatches))
                .isEmpty();
        assertThat(comparison.comparedFeatures()).as(testCase).isPositive();
        assertThat(comparison.excludedFeatures()).as(testCase).isLessThanOrEqualTo(input.featureCount() / 10);
    }

    /** The committed tiles, then the benchmark tiles present, with the configurations, as decoded and jittered. */
    static List<Arguments> tilesAndConfigurations() {
        List<Arguments> arguments = new ArrayList<>();
        for (String tile : tiles()) {
            for (WriterConfiguration configuration : WriterConfiguration.values()) {
                arguments.add(Arguments.of(tile, configuration, false));
                arguments.add(Arguments.of(tile, configuration, true));
            }
        }
        return arguments;
    }

    private static List<String> tiles() {
        List<String> tiles = new ArrayList<>(COMMITTED_TILES);
        for (String tile : BENCHMARK_TILES) {
            if (Files.exists(tileFile(tile))) {
                tiles.add(tile);
            }
        }
        return tiles;
    }

    private static Path tileFile(String tile) {
        String property = COMMITTED_TILES.contains(tile) ? COMMITTED_TILES_DIRECTORY : BENCHMARK_TILES_DIRECTORY;
        String directory = Objects.requireNonNull(
                System.getProperty(property), () -> "The system property " + property + " is not set");
        return Path.of(directory, tile);
    }

    /**
     * The features given to both writers. The layers are brought to the extent of the writers first. The jitter moves
     * the coordinates in tile units, before the scaling of a configuration with scaled input: the writers multiply them
     * by the input scale again when they round them.
     */
    private static TileFeatures inputFeatures(String tile, WriterConfiguration configuration, boolean jittered)
            throws IOException {
        TileFeatures decoded = TileFeatures.decode(Files.readAllBytes(tileFile(tile)));
        TileFeatures features = decoded.toExtent(EXTENT);
        if (jittered) {
            features = features.jittered(JITTER_SEED);
        }
        if (configuration.scalesInput()) {
            features = features.scaled(1 / configuration.inputScale());
        }
        return features;
    }

    /**
     * The java-vector-tile encoder of the configuration. java-vector-tile repairs polygons only when it simplifies
     * them: the encoder of {@link WriterConfiguration#CLIP_SIMPLIFY} records the repaired features.
     */
    private static VectorTileEncoder javaVectorTileEncoder(WriterConfiguration configuration) {
        if (configuration == WriterConfiguration.CLIP_SIMPLIFY) {
            return new RepairRecordingEncoder(EXTENT, CLIP_BUFFER, false, false, SIMPLIFICATION_TOLERANCE);
        }
        return configuration.newJavaVectorTile();
    }

    /** The features repaired by the encoder in its tile, by layer name and id: none without simplification. */
    private static Set<FeatureKey> repairedFeatures(VectorTileEncoder encoder, byte[] javaVectorTile)
            throws IOException {
        if (encoder instanceof RepairRecordingEncoder recorder) {
            return recorder.repairedFeatures(javaVectorTile);
        }
        return Set.of();
    }

    private static String mismatchMessage(String testCase, List<Mismatch> mismatches) {
        String shown = mismatches.stream()
                .limit(MISMATCHES_SHOWN)
                .map(Mismatch::toString)
                .collect(Collectors.joining("\n"));
        return mismatches.size() + " features of " + testCase + " are written differently, the first ones:\n" + shown;
    }
}
