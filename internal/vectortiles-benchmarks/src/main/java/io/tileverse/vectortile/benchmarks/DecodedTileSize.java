/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.benchmarks;

import static io.tileverse.vectortile.benchmarks.HeapMeasurement.copiesFor;
import static io.tileverse.vectortile.benchmarks.HeapMeasurement.heapDelta;
import static io.tileverse.vectortile.benchmarks.HeapMeasurement.kib;
import static io.tileverse.vectortile.benchmarks.HeapMeasurement.print;
import static io.tileverse.vectortile.benchmarks.HeapMeasurement.printRuntime;
import static io.tileverse.vectortile.benchmarks.HeapMeasurement.usedHeapAfterGc;

import io.tileverse.vectortile.model.VectorTile;
import io.tileverse.vectortile.model.VectorTile.Layer.Feature;
import io.tileverse.vectortile.mvt.VectorTileCodec;
import java.io.IOException;
import java.lang.ref.Reference;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.ToLongFunction;
import no.ecc.vectortile.VectorTileDecoder;
import org.locationtech.jts.geom.Geometry;

/**
 * Measures the heap held by a tile decoded by each library: the memory paid by a cache of decoded tiles for each tile.
 *
 * <p>For each tile file, the program decodes copies of the tile with each library, reads the id, the geometry and the
 * attributes of each feature of each copy once, and divides the growth of the used heap after a full garbage collection
 * by the number of copies. The read comes first because a library may keep what a read computes: java-vector-tile turns
 * some bytes of its protobuf objects into strings. The program prints a Markdown table, in KiB per tile.
 *
 * <p>Run it with a fixed heap and the G1 collector, as {@code run-benchmarks.sh} does: {@code java -Xms4g -Xmx4g
 * -XX:+UseG1GC -cp benchmarks.jar io.tileverse.vectortile.benchmarks.DecodedTileSize <file>...}
 */
public final class DecodedTileSize {

    private final VectorTileCodec codec = new VectorTileCodec();

    private final VectorTileDecoder javaVectorTileDecoder = new VectorTileDecoder();

    private DecodedTileSize() {
        // Keeps the coordinates in the extent space of the tile, as the benchmarks do
        javaVectorTileDecoder.setAutoScale(false);
    }

    public static void main(String[] args) throws IOException {
        DecodedTileSize measurement = new DecodedTileSize();
        printRuntime();
        print("| Tile | Encoded | java-vector-tile | tileverse | Heap delta |");
        print("|---|---:|---:|---:|---:|");
        for (String tile : args) {
            measurement.measure(tile);
        }
    }

    private void measure(String tile) throws IOException {
        byte[] tileBytes = Files.readAllBytes(Path.of(tile));
        int copies = copiesFor(tileBytes.length);
        Measurement tileverse = measureCopies(copies, () -> codec.decode(tileBytes), DecodedTileSize::readTileverse);
        Measurement javaVectorTile = measureCopies(
                copies, () -> javaVectorTileDecoder.decode(tileBytes), DecodedTileSize::readJavaVectorTile);
        // Both libraries build the same geometries from the benchmark tiles: a difference means a broken measurement
        if (tileverse.coordinates() != javaVectorTile.coordinates()) {
            throw new IllegalStateException("tileverse read " + tileverse.coordinates() + " coordinates in " + tile
                    + ", java-vector-tile " + javaVectorTile.coordinates());
        }
        printRow(tile, tileBytes.length, javaVectorTile.size(), tileverse.size());
    }

    private static <T> Measurement measureCopies(int copies, TileDecoding<T> decoding, ToLongFunction<T> read)
            throws IOException {
        List<T> tiles = new ArrayList<>(copies);
        long emptyHeap = usedHeapAfterGc();
        long coordinates = 0;
        for (int i = 0; i < copies; i++) {
            T tile = decoding.decode();
            coordinates = read.applyAsLong(tile);
            tiles.add(tile);
        }
        long fullHeap = usedHeapAfterGc();
        Reference.reachabilityFence(tiles);
        return new Measurement((fullHeap - emptyHeap) / copies, coordinates);
    }

    /** Reads each feature of a tile, and returns the coordinates of its geometries. */
    private static long readTileverse(VectorTile tile) {
        return tile.getFeatures().mapToLong(DecodedTileSize::readFeature).sum();
    }

    private static long readFeature(Feature feature) {
        feature.getId();
        feature.getAttributes();
        return coordinatesOf(feature.getGeometry());
    }

    /** Reads each feature of a tile, and returns the coordinates of its geometries. */
    private static long readJavaVectorTile(VectorTileDecoder.FeatureIterable tile) {
        long coordinates = 0;
        for (VectorTileDecoder.Feature feature : tile) {
            feature.getId();
            feature.getAttributes();
            coordinates += coordinatesOf(feature.getGeometry());
        }
        return coordinates;
    }

    private static long coordinatesOf(Geometry geometry) {
        return geometry == null ? 0 : geometry.getNumPoints();
    }

    private static void printRow(String tile, int encodedSize, long javaVectorTileSize, long tileverseSize) {
        print("| `" + tile + "` | " + kib(encodedSize) + " | " + kib(javaVectorTileSize) + " | " + kib(tileverseSize)
                + " | " + heapDelta(tileverseSize, javaVectorTileSize) + " |");
    }

    /** Decodes one copy of a tile. */
    @FunctionalInterface
    private interface TileDecoding<T> {
        T decode() throws IOException;
    }

    /** The heap held by one copy of a decoded tile, and the coordinates of the geometries read from it. */
    private record Measurement(long size, long coordinates) {}
}
