/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.benchmarks;

import io.tileverse.vectortile.benchmarks.TileFeatures.InputFeature;
import io.tileverse.vectortile.benchmarks.TileFeatures.LayerFeatures;
import io.tileverse.vectortile.model.VectorTile;
import io.tileverse.vectortile.mvt.VectorTileCodec;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryComponentFilter;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.impl.PackedCoordinateSequence;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Compares the writing of vector tiles by tileverse and by java-vector-tile.
 *
 * <p>{@link #write()} writes a tile from its features with a new writer of the {@link WriterConfiguration}, up to the
 * bytes of the tile. The features come from the tile file, decoded once in the setup of the trial, with their layers
 * brought to the extent of the writers, and with the coordinate sequences of the {@link Sequence}. The README of this
 * module gives the commands and the results.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class VectorTileWriteBenchmark {

    /** The library under test. */
    public enum Writer {
        TILEVERSE,
        JAVA_VECTOR_TILE
    }

    /** The coordinate sequences of the geometries given to the writers. */
    public enum Sequence {
        /**
         * {@code CoordinateArraySequence}, the sequence of a default JTS geometry factory: its
         * {@code getCoordinates()}, called by java-vector-tile, returns its own array.
         */
        ARRAY,

        /**
         * {@code PackedCoordinateSequence.Double}, the ordinates in one array of doubles: its {@code getCoordinates()}
         * creates a {@code Coordinate} per point, and keeps them until an ordinate is set.
         */
        PACKED
    }

    /**
     * The path of the tile file. There is no default tile: {@code run-benchmarks.sh} downloads the tiles of the
     * benchmarks and passes them with {@code -p tile=<file>,<file>}.
     */
    @Param({})
    private String tile;

    @Param
    private Writer writer;

    @Param
    private WriterConfiguration configuration;

    @Param
    private Sequence sequence;

    private final VectorTileCodec codec = new VectorTileCodec();

    /** The features given to the writers, shared by the operations: the writers only read them. */
    private TileFeatures features;

    /** The packed sequences of the features, emptied of their {@code Coordinate} objects before each operation. */
    private List<PackedCoordinateSequence> packedSequences;

    @Setup(Level.Trial)
    public void readFeatures() throws IOException {
        TileFeatures decoded = TileFeatures.decode(Files.readAllBytes(Path.of(tile)));
        features = inputOf(decoded);
        packedSequences = packedSequencesOf(features);
        // A writer writing no feature would measure nothing
        if (!writesFeatures()) {
            throw new IllegalStateException(writer + " wrote no feature of " + tile + " in " + configuration);
        }
    }

    /**
     * The features as given to the writers: in the extent of the writers, scaled down when the writers of the
     * configuration scale their input, then copied to packed sequences for {@link Sequence#PACKED}.
     */
    private TileFeatures inputOf(TileFeatures decoded) {
        TileFeatures inExtent = decoded.toExtent(WriterConfiguration.EXTENT);
        TileFeatures inInputUnits = inInputUnits(inExtent);
        if (sequence == Sequence.PACKED) {
            return inInputUnits.packed();
        }
        return inInputUnits;
    }

    private TileFeatures inInputUnits(TileFeatures inExtent) {
        if (configuration.scalesInput()) {
            return inExtent.scaled(1 / configuration.inputScale());
        }
        return inExtent;
    }

    /** The packed coordinate sequences of the geometries of the features, without the empty ones. */
    private static List<PackedCoordinateSequence> packedSequencesOf(TileFeatures features) {
        List<PackedCoordinateSequence> sequences = new ArrayList<>();
        GeometryComponentFilter collector = component -> addPackedSequence(component, sequences);
        for (LayerFeatures layer : features.layers()) {
            for (InputFeature feature : layer.features()) {
                feature.geometry().apply(collector);
            }
        }
        return sequences;
    }

    /**
     * Adds the sequence of a point or a line, a ring included. A polygon or a collection has no sequence of its own:
     * the filter visits its parts.
     */
    private static void addPackedSequence(Geometry component, List<PackedCoordinateSequence> sequences) {
        if (component instanceof Point point) {
            addIfPacked(point.getCoordinateSequence(), sequences);
        } else if (component instanceof LineString line) {
            addIfPacked(line.getCoordinateSequence(), sequences);
        }
    }

    // An empty sequence has no ordinate to set
    private static void addIfPacked(CoordinateSequence sequence, List<PackedCoordinateSequence> sequences) {
        if (sequence instanceof PackedCoordinateSequence packed && packed.size() > 0) {
            sequences.add(packed);
        }
    }

    private boolean writesFeatures() throws IOException {
        VectorTile written = codec.decode(write());
        return written.getFeatures().findAny().isPresent();
    }

    /**
     * Drops the {@code Coordinate} objects kept by the packed sequences since their first {@code getCoordinates()}:
     * setting an ordinate drops them. Each operation pays for their creation, as a writer of fresh geometries does.
     * There is nothing to drop for {@link Sequence#ARRAY}.
     */
    @Setup(Level.Invocation)
    public void clearCoordinateCaches() {
        for (PackedCoordinateSequence packed : packedSequences) {
            double x = packed.getOrdinate(0, CoordinateSequence.X);
            packed.setOrdinate(0, CoordinateSequence.X, x);
        }
    }

    @Benchmark
    public byte[] write() {
        if (writer == Writer.TILEVERSE) {
            return TileWriters.writeTileverse(features, configuration.newTileverse());
        }
        return TileWriters.writeJavaVectorTile(features, configuration.newJavaVectorTile());
    }
}
