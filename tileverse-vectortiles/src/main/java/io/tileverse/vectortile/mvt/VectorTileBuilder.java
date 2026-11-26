/*
 * Copyright 2015 Electronic Chart Centre
 * Copyright 2025 Multiversio LLC. All rights reserved.
 *
 * Modifications: Modernized and integrated into Tileverse PMTiles project.
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

import io.tileverse.vectortile.model.VectorTile;
import io.tileverse.vectortile.model.VectorTile.Layer;
import io.tileverse.vectortile.pipeline.ClipStep;
import io.tileverse.vectortile.pipeline.GeometryPipeline;
import io.tileverse.vectortile.pipeline.GeometryStep;
import io.tileverse.vectortile.pipeline.GridSnapStep;
import io.tileverse.vectortile.pipeline.PolygonRepairStep;
import io.tileverse.vectortile.pipeline.SimplificationStep;
import io.tileverse.vectortile.pipeline.SizeFilterStep;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Consumer;
import org.locationtech.jts.geom.Geometry;

/**
 * Writes a Mapbox Vector Tile (MVT) from features in tile coordinates, one layer at a time.
 *
 * <h2>Coordinates</h2>
 *
 * <p>The builder expects geometries in the coordinate space of the tile, not in a coordinate reference system such as
 * WGS84 or Web Mercator: the caller transforms them. The tile spans 0 to the {@link #setExtent(int) extent} on both
 * axes, 4096 by default. The builder rounds the coordinates to integers when it writes them.
 *
 * <p>Geometries in another grid, such as a tile of 256 units for a tile of extent 4096, take an
 * {@link #setInputScale(double) input scale}: the builder multiplies their coordinates by it when it rounds them, and
 * makes no scaled copy of the geometries.
 *
 * <h2>Layers</h2>
 *
 * <p>A tile is written one layer at a time. The features of a layer are added between {@link #layer()} and
 * {@link LayerBuilder#build()}, and the build writes the layer into the tile. A name is written once per tile: a layer
 * builder rejects the name of a layer written before. A layer without features is left out, and leaves its name free. A
 * layer builder fails once its layer is built or the next layer started.
 *
 * <pre>{@code
 * VectorTileBuilder builder = new VectorTileBuilder().setExtent(4096);
 * builder.layer().name("roads").feature(Map.of("highway", "primary"), road).build();
 * builder.layer().name("pois").feature(Map.of("name", "cafe"), cafe).build();
 * byte[] tile = builder.toByteArray();
 * }</pre>
 *
 * <h2>Tiles from tiles</h2>
 *
 * <p>{@link LayerBuilder#merge(Layer)} adds the features of a layer of another tile to the open layer, without the
 * steps of the pipeline. {@link #merge(VectorTile)} adds the layers of a tile, and {@link #init(VectorTile)} starts a
 * tile from an existing one, with its extent.
 *
 * <h2>Pipeline</h2>
 *
 * <p>The geometries given to {@link FeatureBuilder#geometry(Geometry)} go through the {@link #pipeline() pipeline} of
 * the builder as given, before they are written. The settings derive it: a {@link SizeFilterStep}, a
 * {@link SimplificationStep} when the {@link #setSimplificationDistanceTolerance(double) tolerance} is positive, a
 * {@link ClipStep} to the extent and the {@link #setClipBuffer(int) clip buffer}, a {@link GridSnapStep} when the
 * {@link #setUsePrecisionModelSnapping(boolean) snapping} is on, then a {@link PolygonRepairStep} when the tolerance is
 * positive. The settings are in tile units, and the derived steps take them in the units of the given geometries:
 * divided by the input scale. {@link #setPipeline(GeometryPipeline)} replaces the pipeline, until a setting derives it
 * again. A setting rejected by a step leaves the settings and the pipeline as they were.
 *
 * <h2>Result</h2>
 *
 * <p>{@link #toByteArray()} and {@link #writeTo(OutputStream)} give the bytes of the tile, and {@link #build()} the
 * tile read from them. They clear the builder for the next tile. {@link #heapSize()} gives the heap held by the tile
 * under construction, for a memory guard.
 *
 * <h2>Callbacks and threads</h2>
 *
 * <p>The code of the caller run by the builder cannot change the tile under construction: a step of the pipeline,
 * another tile and its layers while they are merged, and the stream given to {@link #writeTo(OutputStream)}. A call
 * from such code to this builder, or to a layer or feature builder of the tile, fails with an
 * {@link IllegalStateException} when it would change the tile, the pipeline or a setting.
 *
 * <p>This class is not thread-safe: each thread uses its own builder. A pipeline can be shared by the builders of
 * several threads.
 */
public class VectorTileBuilder {

    private boolean autoincrementIds = true;
    private long firstFeatureId;

    /** The extent of the layers and the settings deriving the pipeline. */
    private Settings settings = Settings.DEFAULTS;

    /** The pipeline given to {@link #setPipeline}, or the one derived from the settings. */
    private GeometryPipeline pipeline;

    /**
     * Names the code of the caller running inside this builder, or null: a step of the pipeline, a tile or a layer
     * being merged, or the output stream. Such code cannot change the tile under construction.
     */
    private String runningCallback;

    /** Whether the pipeline was given to {@link #setPipeline}. {@link #init} keeps a given pipeline. */
    private boolean pipelineGiven;

    /** One past the largest id taken in the tile, automatic or given: 0 while the tile has none. */
    private long afterLargestId;

    /** Whether the tile has the id {@link Long#MAX_VALUE}. No automatic id comes after it. */
    private boolean hasLargestId;

    /** The layers of the tile, each written as a field of the tile when its builder completes it. */
    private final WireWriter tile = new WireWriter();

    /** The layer under construction, open between {@link #layer()} and the build of its layer builder. */
    private final OpenLayer openLayer = new OpenLayer();

    /** The builder of the open layer, or null when no layer is open. */
    private LayerBuilder openLayerBuilder;

    /** The names of the layers written in the tile. A layer left out for having no features leaves its name free. */
    private final Set<String> layerNames = new HashSet<>();

    // Shared by the feature builders of the layers, to keep their capacity from one layer to the next
    private final CommandEncoder featureEncoder = new CommandEncoder();
    private final IntArray featureTags = new IntArray();

    private final LayerMerger layerMerger;

    /** Constructs a new VectorTileBuilder with default settings. */
    public VectorTileBuilder() {
        pipeline = settings.pipeline();
        layerMerger = new LayerMerger(this, openLayer);
    }

    /**
     * Starts a tile from an existing one: takes the extent of the tile, then merges its layers as {@link #merge} does.
     * A builder with no layer written takes the extent when the layers of the tile agree on one. Otherwise the layers
     * of the tile are merged into the extent of this builder. The checks of {@link #merge} come before the extent is
     * taken.
     *
     * <p>Taking the extent derives the pipeline from the settings again, as {@link #setExtent} does, unless the
     * pipeline was given to {@link #setPipeline}. The steps of a given pipeline are the caller's: they stay as given, a
     * {@link ClipStep} with its own extent included.
     *
     * @param tile the tile to start from
     * @return this VectorTileBuilder for method chaining
     * @throws IllegalStateException if the open layer has features and is not built. Also when the tile or its layers
     *     call this builder to change it while they are merged.
     * @throws IllegalArgumentException if a layer of the tile has no name, two layers of the tile have one name, a
     *     layer has the name of a layer written in this tile, or a name with an unpaired surrogate
     */
    public VectorTileBuilder init(VectorTile tile) {
        List<SourceLayer> layers = readLayers(tile);
        requireMergeable(layers);
        if (layerNames.isEmpty()) {
            OptionalInt tileExtent = sharedExtent(layers);
            tileExtent.ifPresent(this::takeExtent);
        }
        mergeLayers(layers);
        return this;
    }

    /** Takes the extent of a tile for {@link #init}, and derives the pipeline for it unless the pipeline was given. */
    private void takeExtent(int tileExtent) {
        requireAtLeast(1, tileExtent, "extent");
        Settings withTileExtent = settings.withExtent(tileExtent);
        if (pipelineGiven) {
            settings = withTileExtent;
        } else {
            takeSettings(withTileExtent);
        }
    }

    /** The extent of the layers when they all have the same one. */
    private static OptionalInt sharedExtent(List<SourceLayer> layers) {
        int[] extents = layers.stream().mapToInt(SourceLayer::extent).distinct().toArray();
        return extents.length == 1 ? OptionalInt.of(extents[0]) : OptionalInt.empty();
    }

    /**
     * Adds the layers of a tile to this tile, in their order. Each layer of the tile is written as a layer of the same
     * name, with its features merged as {@link LayerBuilder#merge(Layer)} merges them, in the extent of this builder.
     * The automatic ids continue after the largest merged id below 2^63. A layer whose features are all left out is not
     * written.
     *
     * <p>The names are read once, and checked before the first layer is merged: the merge is whole or nothing for the
     * names, and a failed check leaves this tile as it was.
     *
     * @param tile the tile whose layers are added
     * @return this VectorTileBuilder for method chaining
     * @throws IllegalStateException if the open layer has features and is not built. Also when the tile or its layers
     *     call this builder to change it while they are merged.
     * @throws IllegalArgumentException if a layer of the tile has no name, two layers of the tile have one name, a
     *     layer has the name of a layer written in this tile, or a name with an unpaired surrogate
     */
    public VectorTileBuilder merge(VectorTile tile) {
        List<SourceLayer> layers = readLayers(tile);
        requireMergeable(layers);
        mergeLayers(layers);
        return this;
    }

    /**
     * Reads the layers of a tile with their names and extents, once: the checks and the merge take these values, and a
     * layer cannot pass the checks under one name and be written under another. The calls changing the tile, the
     * pipeline or a setting fail while the tile runs its code.
     */
    private List<SourceLayer> readLayers(VectorTile tile) {
        requireNoCallback();
        runningCallback = "a tile being merged";
        try {
            List<SourceLayer> layers = new ArrayList<>();
            for (Layer layer : tile.getLayers()) {
                layers.add(new SourceLayer(layer, layer.getName(), layer.getExtent()));
            }
            return layers;
        } finally {
            runningCallback = null;
        }
    }

    /** A layer of a tile being merged, with its name and its extent as read before the checks. */
    private record SourceLayer(Layer layer, String name, int extent) {}

    /**
     * Fails unless the layers can be merged into this tile: the open layer has no features, and each layer has a name
     * of its own, without an unpaired surrogate and free in this tile. An open layer without features is closed once
     * the checks pass: a failed check leaves it open.
     */
    private void requireMergeable(List<SourceLayer> layers) {
        requireEmptyOpenLayer();
        Set<String> names = new HashSet<>();
        for (SourceLayer layer : layers) {
            String name = layer.name();
            requireLayerName(name);
            requireWellFormedName(name);
            requireNameNotWritten(name);
            requireNameNotRepeated(names, name);
        }
        closeEmptyOpenLayer();
    }

    /**
     * Fails on a layer without a name. The build of an unnamed layer would fail once its features are merged: the check
     * comes before them.
     *
     * @throws IllegalArgumentException if the name is null or empty
     */
    static void requireLayerName(String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("The tile has a layer without a name: a layer is written with its name");
        }
    }

    /**
     * Fails on a name with an unpaired surrogate. UTF-8 has no encoding for it, and the name would be written with a
     * question mark in its place: two such names could be written as one.
     *
     * @throws IllegalArgumentException if the name has an unpaired surrogate
     */
    static void requireWellFormedName(String name) {
        if (name != null && hasUnpairedSurrogate(name)) {
            throw new IllegalArgumentException(
                    "The layer name " + name + " has an unpaired surrogate: a layer name is written in UTF-8");
        }
    }

    // An unpaired surrogate is a code point of its own, of type SURROGATE
    private static boolean hasUnpairedSurrogate(String text) {
        return text.codePoints().anyMatch(codePoint -> Character.getType(codePoint) == Character.SURROGATE);
    }

    /**
     * Adds the name to the names of the layers checked so far, and fails if they hold it already. Unlike the names of
     * the written layers, these names include the layers without features: a tile with two layers of one name is
     * rejected even when one of them is empty.
     *
     * @throws IllegalArgumentException if the names hold the name
     */
    static void requireNameNotRepeated(Set<String> names, String name) {
        if (!names.add(name)) {
            throw new IllegalArgumentException(
                    "The tile has two layers named " + name + ": a tile holds one layer per name");
        }
    }

    private void mergeLayers(List<SourceLayer> layers) {
        for (SourceLayer source : layers) {
            layer().name(source.name()).merge(source.layer()).build();
        }
    }

    /**
     * Starts the next layer of the tile. An open layer without features is closed without being written.
     *
     * @return the builder of the new layer
     * @throws IllegalStateException if the open layer has features and is not built
     */
    public LayerBuilder layer() {
        closeEmptyOpenLayer();
        openLayerBuilder = new LayerBuilder(this);
        return openLayerBuilder;
    }

    /**
     * Closes the open layer, if any, without writing it. Its layer builder fails from then on.
     *
     * @throws IllegalStateException if the open layer has features: they would be lost. Also while the builder runs
     *     code of the caller, such as a step of the pipeline.
     */
    private void closeEmptyOpenLayer() {
        requireNoCallback();
        if (openLayerBuilder == null) {
            return;
        }
        requireEmptyOpenLayer();
        openLayerBuilder.close();
        openLayerBuilder = null;
        openLayer.clear();
    }

    /**
     * Fails if a layer is open and has features.
     *
     * @throws IllegalStateException if the open layer has features
     */
    private void requireEmptyOpenLayer() {
        if (openLayerBuilder != null && openLayer.hasFeatures()) {
            throw new IllegalStateException(
                    "The open layer has features and is not built: call build() on the builder of "
                            + openLayerBuilder.description() + " first");
        }
    }

    /**
     * Returns the feature builder of a layer builder. The feature builders of the layers share the arrays of this tile
     * builder.
     */
    FeatureBuilder newFeatureBuilder(LayerBuilder layerBuilder) {
        return new FeatureBuilder(this, openLayer, layerBuilder, featureEncoder, featureTags);
    }

    /**
     * Runs the pipeline on the geometry of a feature, and hands each resulting geometry to the writer. The calls
     * changing the tile, the pipeline or a setting fail until the run ends.
     */
    void runPipeline(Geometry geometry, Consumer<Geometry> featureWriter) {
        runningCallback = "a step of the pipeline";
        try {
            pipeline.run(geometry, featureWriter);
        } finally {
            runningCallback = null;
        }
    }

    /**
     * Merges the features of a layer of another tile into the open layer, for its layer builder. The calls changing the
     * tile, the pipeline or a setting fail until the merge ends: a layer of another implementation runs its own code
     * while it hands out features.
     */
    void mergeLayer(Layer layer) {
        runningCallback = "a layer being merged";
        try {
            layerMerger.merge(layer);
        } finally {
            runningCallback = null;
        }
    }

    /**
     * Fails while the builder runs code of the caller. A step changing the tile would have its feature written into
     * another layer, or with the id and the attributes of another feature. A layer changing the extent while it is
     * merged would have its features written at another scale. A merge that fails would keep the settings changed by
     * its tile or its layers. A stream adding a layer would lose it when the builder is cleared.
     *
     * @throws IllegalStateException if a step of the pipeline, a tile or a layer being merged, or the output stream
     *     calls the builder
     */
    void requireNoCallback() {
        if (runningCallback != null) {
            throw new IllegalStateException("Cannot change the tile under construction from " + runningCallback);
        }
    }

    /**
     * Fails if the tile holds a layer of this name: a tile holds one layer per name.
     *
     * @throws IllegalArgumentException if a layer of this name is written
     */
    void requireNameNotWritten(String name) {
        if (layerNames.contains(name)) {
            throw new IllegalArgumentException("The tile has a layer named " + name
                    + " already: the features of a layer are added before the next layer starts");
        }
    }

    /**
     * Writes the open layer into the tile with its name when it has features, then clears it for the next layer. A
     * layer without features is left out, and leaves its name free. Called by the builder of the open layer.
     */
    void writeLayer(String name) {
        if (openLayer.hasFeatures()) {
            openLayer.writeInto(tile, name, settings.extent());
            layerNames.add(name);
        }
        openLayer.clear();
        openLayerBuilder = null;
    }

    /**
     * Returns the next automatic id of the tile when automatic ids are on, or {@link FeatureBuilder#NO_ID}. Called for
     * the features without an id of their own. The feature builder takes the id with {@link #noteFeatureId} once the
     * feature is built: a failed build leaves the id to the next feature.
     *
     * @throws IllegalStateException if the tile has the id {@link Long#MAX_VALUE}: no automatic id is left
     */
    long nextFeatureId() {
        if (!autoincrementIds) {
            return FeatureBuilder.NO_ID;
        }
        if (hasLargestId) {
            throw new IllegalStateException("No automatic id is left: the automatic ids continue after the largest id,"
                    + " and the tile has the id " + Long.MAX_VALUE);
        }
        return Math.max(firstFeatureId, afterLargestId);
    }

    /**
     * Moves the automatic ids of the tile past the id. Called for the features built and for the layers merged. A
     * negative id moves nothing.
     */
    void noteFeatureId(long id) {
        if (id == Long.MAX_VALUE) {
            hasLargestId = true;
        } else {
            afterLargestId = Math.max(afterLargestId, id + 1);
        }
    }

    /**
     * Returns the tile read from the bytes of {@link #toByteArray()}, and clears this builder for the next tile.
     *
     * @return the built VectorTile, over its encoded bytes
     * @throws IllegalStateException if the open layer has features and is not built
     */
    public VectorTile build() {
        byte[] encoded = toByteArray();
        try {
            return MvtTile.wrap(encoded);
        } catch (IOException e) {
            // Unreachable: the builder writes valid tiles
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Returns the bytes of the tile: the layers built since this builder was created or cleared. The builder is then
     * cleared for the next tile. An open layer without features is closed without being written.
     *
     * @return the encoded tile
     * @throws IllegalStateException if the open layer has features and is not built
     */
    public byte[] toByteArray() {
        closeEmptyOpenLayer();
        byte[] bytes = tile.toByteArray();
        clear();
        return bytes;
    }

    /**
     * Writes the bytes of {@link #toByteArray()} to the stream, without copying them first. The builder is then cleared
     * for the next tile, also when the stream fails.
     *
     * @param out the stream to write the encoded tile to
     * @throws IOException if the stream fails
     * @throws IllegalStateException if the open layer has features and is not built, or if the stream calls this
     *     builder to change the tile
     */
    public void writeTo(OutputStream out) throws IOException {
        closeEmptyOpenLayer();
        runningCallback = "the output stream";
        try {
            tile.writeTo(out);
        } finally {
            runningCallback = null;
            clear();
        }
    }

    /**
     * Removes the layers and their names, and restarts the automatic ids at the first id. The tile buffer keeps its
     * array.
     */
    private void clear() {
        tile.clear();
        layerNames.clear();
        afterLargestId = 0;
        hasLargestId = false;
    }

    /**
     * Returns the heap held by the tile under construction, in bytes. A memory guard can read it instead of estimating
     * the size of the features. The size counts the capacity of the buffers, not their content: the buffer of the tile,
     * the buffers of the open layer, and the arrays kept to write a feature or to merge a layer. It adds an estimate of
     * the keys and values of the open layer, and leaves out the builder objects. The buffers keep their capacity for
     * the next tile. A key or a value is counted when it joins the tables of the layer: a call costs no walk of the
     * tables, and a guard can read the size after each feature.
     *
     * @return the heap held by the buffers and the tables of this builder, in bytes
     */
    public long heapSize() {
        long tileBuffer = HeapSize.array(Byte.BYTES, tile.capacity());
        return tileBuffer + openLayer.heapSize() + featureArraysHeapSize() + layerMerger.heapSize();
    }

    /**
     * Estimates the heap memory held by the arrays of the feature builders, in bytes: the tags and the geometry
     * commands of a feature, by capacity.
     */
    private long featureArraysHeapSize() {
        IntArray commands = featureEncoder.commands();
        return HeapSize.array(Integer.BYTES, featureTags.capacity())
                + HeapSize.array(Integer.BYTES, commands.capacity());
    }

    /**
     * The size of the tile on both axes, in tile units. It is the extent of the layers, and the clip step keeps 0 to
     * {@code extent} on both axes plus the {@link #setClipBuffer(int) buffer}. 4096 is the default.
     *
     * <p>The coordinates of the geometries given to {@link FeatureBuilder#geometry(Geometry)} are in these units, or in
     * units of the {@link #setInputScale(double) input scale}: the builder multiplies them by the scale when it rounds
     * them. The setting derives the pipeline again, in place of the one given to {@link #setPipeline}.
     *
     * <p>A layer is written with one extent, when it is built. The extent cannot change while the open layer has
     * features, because their coordinates are positions in the previous extent.
     *
     * @param extent an int with extent value. 4096 is a good value.
     * @return this VectorTileBuilder for method chaining
     * @throws IllegalArgumentException if the extent is below 1, or if the clip step rejects it once divided by the
     *     input scale
     * @throws IllegalStateException if the extent changes while the open layer has features
     */
    public VectorTileBuilder setExtent(int extent) {
        requireAtLeast(1, extent, "extent");
        if (extent != settings.extent()) {
            requireEmptyOpenLayer();
        }
        return takeSettings(settings.withExtent(extent));
    }

    /** The extent of the layers of the tile, set by {@link #setExtent(int)}. */
    int extent() {
        return settings.extent();
    }

    /**
     * The width of the band kept around the tile by the clip step, in tile units: 0 clips at the edges of the tile. 32
     * is the default. The setting derives the pipeline again, in place of the one given to {@link #setPipeline}.
     *
     * @param clipBuffer an int with clip buffer size for geometries
     * @return this VectorTileBuilder for method chaining
     * @throws IllegalArgumentException if the clip buffer is negative, or if the clip step rejects it once divided by
     *     the input scale
     */
    public VectorTileBuilder setClipBuffer(int clipBuffer) {
        requireAtLeast(0, clipBuffer, "clip buffer");
        return takeSettings(settings.withClipBuffer(clipBuffer));
    }

    private static int requireAtLeast(int minimum, int value, String name) {
        if (value < minimum) {
            throw new IllegalArgumentException("The " + name + " must be at least " + minimum + ": " + value);
        }
        return value;
    }

    /**
     * The number of tile units in one unit of the geometries given to {@link FeatureBuilder#geometry(Geometry)}: 1 by
     * default. The builder multiplies each coordinate by the scale when it rounds it, as java-vector-tile does with
     * {@code autoScale}, and makes no scaled copy of the geometries. A scale of 16 takes geometries from a grid of 256
     * to a tile of extent 4096.
     *
     * <p>The geometries go through the pipeline as given, in their own units. The extent, the clip buffer and the
     * simplification tolerance stay in tile units, and the derived steps take them divided by the scale: at the scale
     * 16, the default clip buffer of 32 tile units is a band of 2 units of the geometries. The layers merged from other
     * tiles are scaled by their extent instead. The setting derives the pipeline again, in place of the one given to
     * {@link #setPipeline}.
     *
     * @param inputScale the number of tile units in one unit of the given geometries
     * @return this VectorTileBuilder for method chaining
     * @throws IllegalArgumentException if the scale is not positive and finite, or if a step rejects a setting divided
     *     by the scale, such as an infinite extent
     */
    public VectorTileBuilder setInputScale(double inputScale) {
        requirePositiveAndFinite(inputScale, "input scale");
        return takeSettings(settings.withInputScale(inputScale));
    }

    /** The input scale of the geometries given to the feature builder, set by {@link #setInputScale(double)}. */
    double inputScale() {
        return settings.inputScale();
    }

    private static double requirePositiveAndFinite(double value, String name) {
        if (value > 0 && Double.isFinite(value)) {
            return value;
        }
        throw new IllegalArgumentException("The " + name + " must be positive and finite: " + value);
    }

    /**
     * Whether the features without an id get automatic ids. True by default. When false, a feature without an id is
     * written without one.
     *
     * <p>The automatic ids of a tile start at the {@link #setFirstFeatureId(long) first id}, and continue after the
     * largest id of the tile, given or automatic: after a feature with id 50, the next automatic id is 51. A feature
     * takes its id before the pipeline runs: a feature dropped by a step consumes its id. No automatic id comes after
     * the id 2^63 - 1, the largest long: once the tile has it, a feature without an id fails with an
     * {@link IllegalStateException} until the next tile.
     *
     * @param autoincrementIds true to enable auto-incrementing feature IDs
     * @return this VectorTileBuilder for method chaining
     */
    public VectorTileBuilder setAutoIncrementIds(boolean autoincrementIds) {
        requireNoCallback();
        this.autoincrementIds = autoincrementIds;
        return this;
    }

    /**
     * The first automatic id of each tile: 0 by default. The automatic ids are not negative, because a negative id
     * means no id.
     *
     * @param firstId the first automatic id of each tile
     * @return this VectorTileBuilder for method chaining
     * @throws IllegalArgumentException if the id is negative
     * @see #setAutoIncrementIds(boolean)
     */
    public VectorTileBuilder setFirstFeatureId(long firstId) {
        requireNoCallback();
        if (firstId < 0) {
            throw new IllegalArgumentException("The first feature id must be at least 0: " + firstId);
        }
        this.firstFeatureId = firstId;
        return this;
    }

    /**
     * The distance tolerance of the simplification, in tile units. A positive tolerance adds a
     * {@link SimplificationStep} after the size filter, and a {@link PolygonRepairStep} at the end of the pipeline for
     * the polygons invalid once rounded. As in java-vector-tile, the polygons are repaired only when they are
     * simplified. A tolerance of 0 or below, such as the default -1, leaves the geometries unsimplified and unrepaired.
     * The setting derives the pipeline again, in place of the one given to {@link #setPipeline}.
     *
     * @param simplificationDistanceTolerance the distance tolerance for geometry simplification, 0 or below for none
     * @return this VectorTileBuilder for method chaining
     * @throws IllegalArgumentException if the tolerance is infinite or NaN, or if the simplification step rejects it
     *     once divided by the input scale
     */
    public VectorTileBuilder setSimplificationDistanceTolerance(double simplificationDistanceTolerance) {
        requireFinite(simplificationDistanceTolerance, "simplification distance tolerance");
        return takeSettings(settings.withSimplificationDistanceTolerance(simplificationDistanceTolerance));
    }

    // A NaN tolerance adds no simplification step: the check rejects it, with the infinite ones, under the setting name
    private static double requireFinite(double value, String name) {
        if (Double.isFinite(value)) {
            return value;
        }
        throw new IllegalArgumentException("The " + name + " must be finite: " + value);
    }

    /**
     * When true, snaps the coordinates to the integer grid with the precision model of JTS, and removes the parts
     * collapsed by the snapping: a {@link GridSnapStep} runs after the clip step, and a valid polygon stays valid. When
     * false, the default, the builder rounds the coordinates when it writes them. Rounding can make a polygon invalid,
     * for example when a hole touches its shell once rounded. A positive simplification tolerance adds a
     * {@link PolygonRepairStep} for such polygons. The setting derives the pipeline again, in place of the one given to
     * {@link #setPipeline}.
     *
     * @param usePrecisionModelSnapping true to snap with the precision model of JTS, false to round
     * @return this VectorTileBuilder for method chaining
     */
    public VectorTileBuilder setUsePrecisionModelSnapping(boolean usePrecisionModelSnapping) {
        return takeSettings(settings.withUsePrecisionModelSnapping(usePrecisionModelSnapping));
    }

    /**
     * Replaces the pipeline derived from the settings. The given pipeline runs in place of the steps of the clip
     * buffer, the simplification tolerance and the snapping, and the extent remains the extent of the layers. Its steps
     * receive the geometries as given, and the builder multiplies the coordinates by the {@link #setInputScale(double)
     * input scale} when it rounds them. A later call to {@link #setExtent}, {@link #setClipBuffer},
     * {@link #setInputScale}, {@link #setSimplificationDistanceTolerance} or {@link #setUsePrecisionModelSnapping}
     * derives the pipeline from the settings again: the last call wins. {@link #init} takes the extent of a tile and
     * keeps the given pipeline.
     *
     * @param pipeline the steps run on the geometries before they are written
     * @return this VectorTileBuilder for method chaining
     * @throws NullPointerException if the pipeline is null
     */
    public VectorTileBuilder setPipeline(GeometryPipeline pipeline) {
        requireNoCallback();
        this.pipeline = Objects.requireNonNull(pipeline, "The pipeline cannot be null");
        pipelineGiven = true;
        return this;
    }

    /**
     * The pipeline run on the geometries before they are written: the one given to {@link #setPipeline}, or the one
     * derived from the settings. The derived pipeline drops the lines shorter than one tile unit and the polygons of
     * area below one square tile unit with a {@link SizeFilterStep}, simplifies with a {@link SimplificationStep} when
     * the tolerance is positive, clips to the extent and the clip buffer with a {@link ClipStep}, snaps to the integer
     * grid of the tile with a {@link GridSnapStep} when the snapping is on, then repairs the polygons invalid once
     * rounded with a {@link PolygonRepairStep} when the tolerance is positive. Its steps work in the units of the given
     * geometries: the settings divided by the {@link #setInputScale(double) input scale}, and the grid of one tile unit
     * at that scale.
     *
     * @return the pipeline of this builder
     */
    public GeometryPipeline pipeline() {
        return pipeline;
    }

    /**
     * Takes the settings and the pipeline derived from them, in place of a given pipeline. The pipeline is derived
     * first: when a step rejects the settings, the builder keeps its settings and its pipeline.
     */
    private VectorTileBuilder takeSettings(Settings newSettings) {
        requireNoCallback();
        GeometryPipeline derived = newSettings.pipeline();
        settings = newSettings;
        pipeline = derived;
        pipelineGiven = false;
        return this;
    }

    /** The extent of the layers and the settings deriving the pipeline. */
    private record Settings(
            int extent,
            int clipBuffer,
            double simplificationDistanceTolerance,
            boolean usePrecisionModelSnapping,
            double inputScale) {

        static final Settings DEFAULTS = new Settings(4096, 32, -1d, false, 1);

        Settings withExtent(int newExtent) {
            return new Settings(
                    newExtent, clipBuffer, simplificationDistanceTolerance, usePrecisionModelSnapping, inputScale);
        }

        Settings withClipBuffer(int newClipBuffer) {
            return new Settings(
                    extent, newClipBuffer, simplificationDistanceTolerance, usePrecisionModelSnapping, inputScale);
        }

        Settings withSimplificationDistanceTolerance(double newTolerance) {
            return new Settings(extent, clipBuffer, newTolerance, usePrecisionModelSnapping, inputScale);
        }

        Settings withUsePrecisionModelSnapping(boolean newSnapping) {
            return new Settings(extent, clipBuffer, simplificationDistanceTolerance, newSnapping, inputScale);
        }

        Settings withInputScale(double newInputScale) {
            return new Settings(
                    extent, clipBuffer, simplificationDistanceTolerance, usePrecisionModelSnapping, newInputScale);
        }

        /**
         * The steps of the settings, in the units of the given geometries.
         *
         * @throws IllegalArgumentException if a step rejects a setting divided by the input scale
         */
        GeometryPipeline pipeline() {
            double tileUnit = 1 / inputScale;
            List<GeometryStep> steps = new ArrayList<>();
            steps.add(new SizeFilterStep(tileUnit, tileUnit * tileUnit));
            if (simplificationDistanceTolerance > 0) {
                steps.add(new SimplificationStep(simplificationDistanceTolerance / inputScale));
            }
            steps.add(new ClipStep(extent / inputScale, clipBuffer / inputScale));
            if (usePrecisionModelSnapping) {
                steps.add(new GridSnapStep(inputScale));
            }
            if (simplificationDistanceTolerance > 0) {
                steps.add(new PolygonRepairStep(inputScale));
            }
            return GeometryPipeline.of(steps.toArray(GeometryStep[]::new));
        }
    }
}
