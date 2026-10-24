/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.tiling.matrix;

import io.tileverse.geom.Coordinate;
import io.tileverse.tiling.pyramid.TileRange;

/** Builder for creating TileMatrix instances. */
public class TileMatrixBuilder {
    private TileRange tileRange;
    private double resolution;
    private Coordinate pointOfOrigin;
    private String crsId;
    private int tileWidth = 256;
    private int tileHeight = 256;

    public TileMatrixBuilder tileRange(TileRange tileRange) {
        this.tileRange = tileRange;
        return this;
    }

    public TileMatrixBuilder resolution(double resolution) {
        this.resolution = resolution;
        return this;
    }

    public TileMatrixBuilder pointOfOrigin(Coordinate pointOfOrigin) {
        this.pointOfOrigin = pointOfOrigin;
        return this;
    }

    public TileMatrixBuilder crs(String crsId) {
        this.crsId = crsId;
        return this;
    }

    public TileMatrixBuilder tileSize(int width, int height) {
        this.tileWidth = width;
        this.tileHeight = height;
        return this;
    }

    public TileMatrix build() {
        return new TileMatrix(tileRange, resolution, pointOfOrigin, crsId, tileWidth, tileHeight);
    }
}
