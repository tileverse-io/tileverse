/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.tiling.matrix;

import io.tileverse.tiling.common.BoundingBox2D;
import io.tileverse.tiling.pyramid.TileIndex;

/**
 * Builder for creating Tile instances.
 */
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
