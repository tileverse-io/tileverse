/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.tiling.matrix;

import io.tileverse.geom.BoundingBox2D;
import io.tileverse.tiling.pyramid.TileIndex;

/** Builder for creating Tile instances. */
public class TileBuilder {
    private TileIndex tileIndex;
    private BoundingBox2D extent;
    private int width = 256;
    private int height = 256;
    private double resolution;
    private String crsId;

    public TileBuilder tileIndex(TileIndex tileIndex) {
        this.tileIndex = tileIndex;
        return this;
    }

    public TileBuilder tileIndex(long x, long y, int z) {
        this.tileIndex = TileIndex.xyz(x, y, z);
        return this;
    }

    public TileBuilder extent(BoundingBox2D extent) {
        this.extent = extent;
        return this;
    }

    public TileBuilder extent(double minX, double minY, double maxX, double maxY) {
        this.extent = BoundingBox2D.extent(minX, minY, maxX, maxY);
        return this;
    }

    public TileBuilder size(int width, int height) {
        this.width = width;
        this.height = height;
        return this;
    }

    public TileBuilder resolution(double resolution) {
        this.resolution = resolution;
        return this;
    }

    public TileBuilder crs(String crsId) {
        this.crsId = crsId;
        return this;
    }

    public Tile build() {
        return new Tile(tileIndex, extent, width, height, resolution, crsId);
    }
}
