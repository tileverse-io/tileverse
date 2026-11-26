/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.benchmarks;

import io.tileverse.vectortile.model.VectorTile;
import io.tileverse.vectortile.model.VectorTile.Layer.Feature;
import io.tileverse.vectortile.mvt.VectorTileCodec;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import no.ecc.vectortile.VectorTileDecoder;
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
 * Compares the decoding of vector tiles by tileverse and by java-vector-tile.
 *
 * <p>{@link #decode()} measures the decoding of the tile bytes. {@link #readFeatures(Blackhole)} decodes the tile, then
 * reads the id, the geometry and the attributes of each feature. {@link #readDecodedFeatures(DecodedTile, Blackhole)}
 * reads them from a tile decoded once, in the setup of the trial. The README of this module gives the commands and the
 * results.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class VectorTileDecodeBenchmark {

    /** The library under test. */
    public enum Decoder {
        TILEVERSE,
        JAVA_VECTOR_TILE
    }

    /**
     * The path of the tile file. There is no default tile: {@code run-benchmarks.sh} downloads the tiles of the
     * benchmarks and passes them with {@code -p tile=<file>,<file>}.
     */
    @Param({})
    private String tile;

    @Param
    private Decoder decoder;

    private final VectorTileCodec codec = new VectorTileCodec();

    private final VectorTileDecoder javaVectorTileDecoder = new VectorTileDecoder();

    private byte[] tileBytes;

    public VectorTileDecodeBenchmark() {
        // Keeps the coordinates in the extent space of the tile, as tileverse does
        javaVectorTileDecoder.setAutoScale(false);
    }

    @Setup(Level.Trial)
    public void readTile() throws IOException {
        tileBytes = Files.readAllBytes(Path.of(tile));
        // A tile without features would measure nothing
        if (!hasFeatures()) {
            throw new IllegalStateException(decoder + " found no feature in " + tile);
        }
    }

    private boolean hasFeatures() throws IOException {
        if (decoder == Decoder.TILEVERSE) {
            VectorTile vectorTile = codec.decode(tileBytes);
            return vectorTile.getFeatures().findAny().isPresent();
        }
        return javaVectorTileDecoder.decode(tileBytes).iterator().hasNext();
    }

    @Benchmark
    public Object decode() throws IOException {
        if (decoder == Decoder.TILEVERSE) {
            return codec.decode(tileBytes);
        }
        return javaVectorTileDecoder.decode(tileBytes);
    }

    @Benchmark
    public void readFeatures(Blackhole blackhole) throws IOException {
        if (decoder == Decoder.TILEVERSE) {
            consumeFeatures(codec.decode(tileBytes), blackhole);
        } else {
            consumeFeatures(javaVectorTileDecoder.decode(tileBytes), blackhole);
        }
    }

    /** Reads the features of the tile decoded in the setup. java-vector-tile builds them again at each iteration. */
    @Benchmark
    public void readDecodedFeatures(DecodedTile decoded, Blackhole blackhole) {
        if (decoder == Decoder.TILEVERSE) {
            consumeFeatures(decoded.tileverse, blackhole);
        } else {
            consumeFeatures(decoded.javaVectorTile, blackhole);
        }
    }

    private static void consumeFeatures(VectorTile vectorTile, Blackhole blackhole) {
        vectorTile.getFeatures().forEach(feature -> consume(feature, blackhole));
    }

    private static void consume(Feature feature, Blackhole blackhole) {
        blackhole.consume(feature.getId());
        blackhole.consume(feature.getGeometry());
        blackhole.consume(feature.getAttributes());
    }

    private static void consumeFeatures(VectorTileDecoder.FeatureIterable features, Blackhole blackhole) {
        for (VectorTileDecoder.Feature feature : features) {
            consume(feature, blackhole);
        }
    }

    private static void consume(VectorTileDecoder.Feature feature, Blackhole blackhole) {
        blackhole.consume(feature.getId());
        blackhole.consume(feature.getGeometry());
        blackhole.consume(feature.getAttributes());
    }

    /**
     * The tile decoded once by the library under test, for {@link VectorTileDecodeBenchmark#readDecodedFeatures}. The
     * other benchmarks do not keep a decoded tile alive while they run.
     */
    @State(Scope.Benchmark)
    public static class DecodedTile {

        private VectorTile tileverse;
        private VectorTileDecoder.FeatureIterable javaVectorTile;

        @Setup(Level.Trial)
        public void decode(VectorTileDecodeBenchmark benchmark) throws IOException {
            if (benchmark.decoder == Decoder.TILEVERSE) {
                tileverse = benchmark.codec.decode(benchmark.tileBytes);
            } else {
                javaVectorTile = benchmark.javaVectorTileDecoder.decode(benchmark.tileBytes);
            }
        }
    }
}
