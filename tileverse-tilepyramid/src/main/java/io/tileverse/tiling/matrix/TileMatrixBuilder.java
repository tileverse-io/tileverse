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

import io.tileverse.tiling.common.Coordinate;
import io.tileverse.tiling.pyramid.TileRange;

/**
 * Builder for creating TileMatrix instances.
 */
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
