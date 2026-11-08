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

import io.tileverse.vectortile.benchmarks.TileFeatures.InputFeature;
import io.tileverse.vectortile.benchmarks.TileFeatures.LayerFeatures;
import io.tileverse.vectortile.mvt.LayerBuilder;
import io.tileverse.vectortile.mvt.VectorTileBuilder;
import java.io.IOException;
import java.lang.ref.Reference;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import no.ecc.vectortile.VectorTileEncoder;
import org.locationtech.jts.geom.Geometry;

/**
 * Measures the heap held by a tile under construction with each writer: the heap held by a writer once the features of
 * a tile are added, before the tile is encoded. A tile server pays this heap for each tile under construction.
 *
 * <p>For each tile file, the program decodes the features of the tile and brings its layers to the extent of the
 * writers. It adds the features to copies of both writers in the {@link WriterConfiguration#ENCODING_ONLY encoding-only
 * configuration}, and divides the growth of the used heap after a full garbage collection by the number of copies. A
 * tileverse builder writes a layer into the tile at the build of the layer: the copies build a layer when the next one
 * starts, and the last layer stays open, as after the last feature of a tile. java-vector-tile keeps the geometries of
 * the features until it encodes the tile: its copies receive copies of the geometries, as the tiles of a server receive
 * geometries of their own. The copies share the keys and values of the attributes with the decoded features, and the
 * sizes leave them out.
 *
 * <p>The program prints a Markdown table, in KiB per tile. The table gives the heap reported by
 * {@link VectorTileBuilder#heapSize()} next to the heap measured for tileverse. Run the program with a fixed heap and
 * the G1 collector, as {@code run-benchmarks.sh} does: {@code java -Xms4g -Xmx4g -XX:+UseG1GC -cp benchmarks.jar
 * io.tileverse.vectortile.benchmarks.TileUnderConstructionSize <file>...}
 */
public final class TileUnderConstructionSize {

    private static final WriterConfiguration CONFIGURATION = WriterConfiguration.ENCODING_ONLY;

    private TileUnderConstructionSize() {}

    public static void main(String[] args) throws IOException {
        printRuntime();
        print("| Tile | java-vector-tile | tileverse measured | tileverse `heapSize()` | Heap delta |");
        print("|---|---:|---:|---:|---:|");
        for (String tile : args) {
            measure(tile);
        }
    }

    private static void measure(String tile) throws IOException {
        byte[] tileBytes = Files.readAllBytes(Path.of(tile));
        TileFeatures decoded = TileFeatures.decode(tileBytes);
        TileFeatures features = decoded.toExtent(WriterConfiguration.EXTENT);
        // A first copy of each writer, left out of the measurement, initializes what the copies share: the classes of
        // the writers, and the entry sets cached by the attribute maps of the features
        long reportedSize = tileverseCopy(features).heapSize();
        javaVectorTileCopy(features);
        int copies = copiesFor(tileBytes.length);
        long tileverseSize = heapPerCopy(copies, () -> tileverseCopy(features));
        long javaVectorTileSize = heapPerCopy(copies, () -> javaVectorTileCopy(features));
        printRow(tile, javaVectorTileSize, tileverseSize, reportedSize);
    }

    /** A tileverse builder holding the features of the tile: its last layer open, the other layers built. */
    private static VectorTileBuilder tileverseCopy(TileFeatures features) {
        VectorTileBuilder builder = CONFIGURATION.newTileverse();
        LayerBuilder openLayer = null;
        for (LayerFeatures layer : features.layers()) {
            if (openLayer != null) {
                openLayer.build();
            }
            openLayer = addLayer(builder, layer);
        }
        return builder;
    }

    /** Starts the layer in the builder and adds its features. The layer stays open. */
    private static LayerBuilder addLayer(VectorTileBuilder builder, LayerFeatures layer) {
        LayerBuilder layerBuilder = builder.layer().name(layer.name());
        for (InputFeature feature : layer.features()) {
            layerBuilder.feature(feature.attributes(), feature.geometry(), feature.id());
        }
        return layerBuilder;
    }

    /** A java-vector-tile encoder holding the features of the tile, each with a copy of its geometry. */
    private static VectorTileEncoder javaVectorTileCopy(TileFeatures features) {
        VectorTileEncoder encoder = CONFIGURATION.newJavaVectorTile();
        for (LayerFeatures layer : features.layers()) {
            for (InputFeature feature : layer.features()) {
                Geometry geometry = feature.geometry().copy();
                encoder.addFeature(layer.name(), feature.attributes(), geometry, feature.id());
            }
        }
        return encoder;
    }

    /** The growth of the used heap per copy while the copies are kept, in bytes. */
    private static <T> long heapPerCopy(int copies, Supplier<T> copy) {
        List<T> tiles = new ArrayList<>(copies);
        long emptyHeap = usedHeapAfterGc();
        for (int i = 0; i < copies; i++) {
            tiles.add(copy.get());
        }
        long fullHeap = usedHeapAfterGc();
        // Both readings count the features held by the supplier, and the second one counts the copies
        Reference.reachabilityFence(copy);
        Reference.reachabilityFence(tiles);
        return (fullHeap - emptyHeap) / copies;
    }

    private static void printRow(String tile, long javaVectorTileSize, long tileverseSize, long reportedSize) {
        print("| `" + tile + "` | " + kib(javaVectorTileSize) + " | " + kib(tileverseSize) + " | " + kib(reportedSize)
                + " | " + heapDelta(tileverseSize, javaVectorTileSize) + " |");
    }
}
