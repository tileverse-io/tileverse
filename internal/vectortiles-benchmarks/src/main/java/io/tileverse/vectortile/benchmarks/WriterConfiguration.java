/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.benchmarks;

import io.tileverse.vectortile.mvt.VectorTileBuilder;
import io.tileverse.vectortile.pipeline.GeometryPipeline;
import io.tileverse.vectortile.pipeline.SizeFilterStep;
import no.ecc.vectortile.VectorTileEncoder;

/**
 * The settings of two writers, a java-vector-tile encoder and a tileverse builder, set to write the same tile from the
 * same features. The parity test compares the tiles of the two writers, and the write benchmarks measure them.
 *
 * <p>In each configuration, both writers first drop the lines shorter than 1 and the polygons of area below 1, in tile
 * units. The tileverse builders write tiles of extent 4096, without automatic ids: the features have ids of their own.
 */
public enum WriterConfiguration {

    /**
     * Writes the geometries without clipping or simplification. Both writers drop the lines shorter than 1 and the
     * polygons of area below 1, then round and encode the coordinates.
     */
    ENCODING_ONLY {
        @Override
        public VectorTileEncoder newJavaVectorTile() {
            return new NoClipEncoder(EXTENT, 128, false);
        }

        @Override
        public VectorTileBuilder newTileverse() {
            GeometryPipeline sizeFilter = GeometryPipeline.of(new SizeFilterStep(1, 1));
            return tileverseBuilder().setPipeline(sizeFilter);
        }
    },

    /**
     * Clips the geometries to the tile and a buffer of 8 tile units around it, the default buffer of java-vector-tile,
     * without simplification.
     */
    DEFAULTS {
        @Override
        public VectorTileEncoder newJavaVectorTile() {
            return new VectorTileEncoder(EXTENT, CLIP_BUFFER, false, false, NO_SIMPLIFICATION);
        }

        @Override
        public VectorTileBuilder newTileverse() {
            return tileverseBuilder().setClipBuffer(CLIP_BUFFER);
        }
    },

    /** Clips the geometries at the edges of the tile, without a buffer and without simplification. */
    CLIP_NO_BUFFER {
        @Override
        public VectorTileEncoder newJavaVectorTile() {
            return new VectorTileEncoder(EXTENT, 0, false, false, NO_SIMPLIFICATION);
        }

        @Override
        public VectorTileBuilder newTileverse() {
            return tileverseBuilder().setClipBuffer(0);
        }
    },

    /**
     * Simplifies the lines and polygons with a distance tolerance of 2 tile units, then clips them as {@link #DEFAULTS}
     * does. Both writers then repair the polygons invalid once rounded, each in its own way.
     */
    CLIP_SIMPLIFY {
        @Override
        public VectorTileEncoder newJavaVectorTile() {
            return new VectorTileEncoder(EXTENT, CLIP_BUFFER, false, false, SIMPLIFICATION_TOLERANCE);
        }

        @Override
        public VectorTileBuilder newTileverse() {
            VectorTileBuilder builder = tileverseBuilder().setClipBuffer(CLIP_BUFFER);
            return builder.setSimplificationDistanceTolerance(SIMPLIFICATION_TOLERANCE);
        }
    },

    /**
     * Takes the coordinates of a tile of size 256 and scales them to the extent, as java-vector-tile does with
     * {@code autoScale}, without simplification. The clip buffer is 8 units of the input, 128 tile units. Both writers
     * filter and clip the geometries in the units of the input, and multiply the coordinates by 16 when they round
     * them. tileverse runs with the input scale 16.
     */
    AUTO_SCALE {
        @Override
        public VectorTileEncoder newJavaVectorTile() {
            return new VectorTileEncoder(EXTENT, CLIP_BUFFER, true, false, NO_SIMPLIFICATION);
        }

        @Override
        public VectorTileBuilder newTileverse() {
            VectorTileBuilder builder = tileverseBuilder().setInputScale(AUTO_SCALE_FACTOR);
            return builder.setClipBuffer(CLIP_BUFFER * AUTO_SCALE_FACTOR);
        }
    };

    /** The extent of the written tiles. */
    public static final int EXTENT = 4096;

    /**
     * The default clip buffer of java-vector-tile, in tile units, or in units of the input when the writers scale it.
     */
    static final int CLIP_BUFFER = 8;

    /** The distance tolerance of the simplification of {@link #CLIP_SIMPLIFY}, in tile units. */
    static final double SIMPLIFICATION_TOLERANCE = 2.0;

    /** The tolerance of java-vector-tile without simplification: a tolerance of 0 or below simplifies nothing. */
    private static final double NO_SIMPLIFICATION = -1;

    /** The size of the tile in the coordinates given to writers scaling their input. */
    private static final int SCALED_INPUT_SIZE = 256;

    /** The number of tile units in one unit of the coordinates given to writers scaling their input: 16. */
    private static final int AUTO_SCALE_FACTOR = EXTENT / SCALED_INPUT_SIZE;

    /** Returns a new java-vector-tile encoder with the settings of this configuration. */
    public abstract VectorTileEncoder newJavaVectorTile();

    /** Returns a new tileverse builder with the settings of this configuration. */
    public abstract VectorTileBuilder newTileverse();

    /**
     * Whether the writers take coordinates from 0 to 256 and scale them to the extent, as java-vector-tile does with
     * {@code autoScale}: in {@link #AUTO_SCALE} alone.
     */
    public boolean scalesInput() {
        return this == AUTO_SCALE;
    }

    /**
     * Whether the writers clip the geometries to the tile and a buffer around it: in the configurations other than
     * {@link #ENCODING_ONLY}.
     */
    public boolean clips() {
        return this != ENCODING_ONLY;
    }

    /**
     * The number of tile units in one unit of the coordinates given to the writers: 16 when the writers scale their
     * input, 1 otherwise. The features of a decoded tile are scaled by {@code 1 / inputScale()} for the writers.
     */
    public double inputScale() {
        if (scalesInput()) {
            return AUTO_SCALE_FACTOR;
        }
        return 1.0;
    }

    private static VectorTileBuilder tileverseBuilder() {
        return new VectorTileBuilder().setExtent(EXTENT).setAutoIncrementIds(false);
    }
}
