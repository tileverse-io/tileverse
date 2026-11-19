/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.benchmarks;

import no.ecc.vectortile.VectorTileEncoder;
import org.locationtech.jts.geom.Geometry;

/**
 * A java-vector-tile encoder without clipping: it writes the geometries as given, and keeps the points outside the
 * tile. Its size filter and its encoding are those of java-vector-tile.
 */
public final class NoClipEncoder extends VectorTileEncoder {

    /**
     * Creates an encoder without automatic ids and without simplification, as the constructor of
     * {@link VectorTileEncoder} with the same parameters does.
     *
     * @param extent the extent of the written tiles
     * @param clipBuffer the buffer of the clip polygon built by java-vector-tile, unused by this encoder
     * @param autoScale whether the encoder scales its input from 0..256 to the extent
     */
    public NoClipEncoder(int extent, int clipBuffer, boolean autoScale) {
        super(extent, clipBuffer, autoScale);
    }

    /** Keeps the points: java-vector-tile drops the points outside the clip polygon. */
    @Override
    protected boolean clipCovers(Geometry geom) {
        return true;
    }

    /** Returns the geometry as given. */
    @Override
    protected Geometry clipGeometry(Geometry geometry) {
        return geometry;
    }
}
