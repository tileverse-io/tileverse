/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.benchmarks;

import io.tileverse.vectortile.benchmarks.TileFeatures.InputFeature;
import io.tileverse.vectortile.benchmarks.TileFeatures.LayerFeatures;
import io.tileverse.vectortile.mvt.VectorTileCodec;
import io.tileverse.vectortile.pipeline.RoundedValidity;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateSequenceFilter;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.precision.GeometryPrecisionReducer;
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
import org.openjdk.jmh.infra.Blackhole;

/**
 * Compares three checks of the validity of polygons once their coordinates are rounded to integers, as a writer checks
 * a polygon before writing it. The three checks answer the same question in different ways.
 *
 * <p>{@link #checkPolygons(Blackhole)} runs the {@link Check} on a fresh copy of each polygon and multipolygon of the
 * tile, and {@link Check#COPY_ONLY} measures the copies alone. The setup of the trial decodes the polygons once from
 * the tile file, with the packed coordinate sequences of the default geometry reader of tileverse, and brings their
 * layers to the extent of the writers. It moves their coordinates by less than half a tile unit, as the parity test
 * does. The README of this module gives the commands and the results.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class PolygonValidityBenchmark {

    /** The validity check under test, or the baseline of the copies. */
    public enum Check {
        /** The check of tileverse: {@link RoundedValidity#isValid}, over coordinates rounded into reused objects. */
        ROUNDED_VALIDITY,

        /**
         * The check of java-vector-tile without its encoding and decoding: {@code isValid()} on a copy of the polygon
         * rounded point by point by the precision reducer of JTS.
         */
        PRECISION_REDUCER_IS_VALID,

        /**
         * {@code isValid()} on the fresh copy once its coordinates are rounded in place with
         * {@link Math#round(double)}: the validity check of JTS on the rounded polygon, without another copy.
         */
        IS_VALID,

        /** The copy of the polygon alone, without a check: the baseline, included in the time of the other checks. */
        COPY_ONLY
    }

    /** The seed of the offsets of the coordinates, as in the parity test. */
    private static final long JITTER_SEED = 42;

    private static final PrecisionModel INTEGER_GRID = new PrecisionModel(1.0);

    private static final CoordinateSequenceFilter ROUNDING = new RoundingFilter();

    /**
     * The path of the tile file. There is no default tile: {@code run-benchmarks.sh} downloads the tiles of the
     * benchmarks and passes them with {@code -p tile=<file>,<file>}.
     */
    @Param({})
    private String tile;

    @Param
    private Check check;

    /** The check of tileverse, one per thread state: a check keeps its coordinates and is not thread-safe. */
    private final RoundedValidity roundedValidity = new RoundedValidity();

    /** The polygons and multipolygons of the tile, in the extent of the writers, with their coordinates moved. */
    private List<Geometry> polygons;

    @Setup(Level.Trial)
    public void readPolygons() throws IOException {
        byte[] tileBytes = Files.readAllBytes(Path.of(tile));
        TileFeatures decoded = TileFeatures.decode(tileBytes, VectorTileCodec.newGeometryReader());
        TileFeatures inExtent = decoded.toExtent(WriterConfiguration.EXTENT);
        polygons = polygonsOf(inExtent.jittered(JITTER_SEED));
        // A tile without polygons would measure nothing
        if (polygons.isEmpty()) {
            throw new IllegalStateException(tile + " has no polygon");
        }
    }

    private static List<Geometry> polygonsOf(TileFeatures features) {
        List<Geometry> polygons = new ArrayList<>();
        for (LayerFeatures layer : features.layers()) {
            for (InputFeature feature : layer.features()) {
                addIfPolygonal(feature.geometry(), polygons);
            }
        }
        return polygons;
    }

    private static void addIfPolygonal(Geometry geometry, List<Geometry> polygons) {
        if (geometry instanceof Polygonal) {
            polygons.add(geometry);
        }
    }

    /**
     * Runs the check on a fresh copy of each polygon. A writer checks a polygon once, and a packed coordinate sequence
     * keeps the {@code Coordinate} objects created by its first {@code getCoordinates()}, as called by the validity
     * check of JTS: a second check of the same polygon would skip their creation. The copy of a packed sequence copies
     * its ordinates into a new array, and creates no {@code Coordinate} object.
     */
    @Benchmark
    public void checkPolygons(Blackhole blackhole) {
        for (Geometry polygon : polygons) {
            Geometry copy = polygon.copy();
            consumeCheck(copy, blackhole);
        }
    }

    /** Runs the check on the copy and hands its answer to the blackhole, or the copy itself for {@code COPY_ONLY}. */
    private void consumeCheck(Geometry copy, Blackhole blackhole) {
        switch (check) {
            case ROUNDED_VALIDITY -> blackhole.consume(roundedValidity.isValid((Polygonal) copy));
            case PRECISION_REDUCER_IS_VALID -> blackhole.consume(isValidOncePrecisionReduced(copy));
            case IS_VALID -> blackhole.consume(isValidOnceRoundedInPlace(copy));
            case COPY_ONLY -> blackhole.consume(copy);
        }
    }

    private static boolean isValidOncePrecisionReduced(Geometry polygon) {
        Geometry rounded = GeometryPrecisionReducer.reducePointwise(polygon, INTEGER_GRID);
        return rounded.isValid();
    }

    private static boolean isValidOnceRoundedInPlace(Geometry copy) {
        copy.apply(ROUNDING);
        return copy.isValid();
    }

    /**
     * Rounds the coordinates of a geometry in place with {@link Math#round(double)}. The filter reports a change: the
     * geometry then drops its cached envelope, as {@code Geometry.apply} calls {@code geometryChanged()}. The filter
     * keeps no state, and the benchmark threads share it.
     */
    private static final class RoundingFilter implements CoordinateSequenceFilter {

        @Override
        public void filter(CoordinateSequence sequence, int index) {
            double x = sequence.getOrdinate(index, CoordinateSequence.X);
            double y = sequence.getOrdinate(index, CoordinateSequence.Y);
            sequence.setOrdinate(index, CoordinateSequence.X, (double) Math.round(x));
            sequence.setOrdinate(index, CoordinateSequence.Y, (double) Math.round(y));
        }

        @Override
        public boolean isDone() {
            return false;
        }

        @Override
        public boolean isGeometryChanged() {
            return true;
        }
    }
}
