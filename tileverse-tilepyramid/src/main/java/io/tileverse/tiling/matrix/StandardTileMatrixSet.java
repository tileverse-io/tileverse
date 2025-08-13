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

import io.tileverse.tiling.pyramid.TileIndex;
import io.tileverse.tiling.pyramid.TilePyramid;
import io.tileverse.tiling.pyramid.TileRange;
import java.util.List;
import java.util.Optional;

/**
 * Standard implementation of TileMatrixSet that provides coordinate
 * transformation between tile space and map space using composition with a
 * TilePyramid.
 *
 * <p>
 * This implementation supports common tiling schemes such as:
 * <ul>
 * <li>Web Mercator (EPSG:3857) - Google/OSM style tiling</li>
 * <li>Geographic (EPSG:4326) - WGS84 longitude/latitude</li>
 * <li>Custom projected coordinate systems</li>
 * </ul>
 *
 * @since 1.0
 */
public record StandardTileMatrixSet(
        TilePyramid tilePyramid,
        String crsId,
        int tileWidth,
        int tileHeight,
        Extent extent,
        double[] resolutions,
        Coordinate[] origins)
        implements TileMatrixSet {

    @Override
    public TilePyramid tilePyramid() {
        return tilePyramid;
    }

    @Override
    public String crsId() {
        return crsId;
    }

    @Override
    public int tileWidth() {
        return tileWidth;
    }

    @Override
    public int tileHeight() {
        return tileHeight;
    }

    @Override
    public Extent extent() {
        return extent;
    }

    @Override
    public double resolution(int zoomLevel) {
        validateZoomLevel(zoomLevel);
        return resolutions[zoomLevel];
    }

    @Override
    public Coordinate origin(int zoomLevel) {
        validateZoomLevel(zoomLevel);
        return origins[zoomLevel];
    }

    @Override
    public TileIndex coordinateToTile(Coordinate coordinate, int zoomLevel) {
        validateZoomLevel(zoomLevel);

        double resolution = resolution(zoomLevel);
        Coordinate origin = origin(zoomLevel);

        // Calculate tile size in map units
        double tileMapWidth = tileWidth * resolution;
        double tileMapHeight = tileHeight * resolution;

        long tileX, tileY;

        // Transform map coordinates to tile coordinates based on axis origin
        switch (tilePyramid.axisOrigin()) {
            case LOWER_LEFT -> {
                tileX = (long) Math.floor((coordinate.x() - origin.x()) / tileMapWidth);
                tileY = (long) Math.floor((coordinate.y() - origin.y()) / tileMapHeight);
            }
            case UPPER_LEFT -> {
                tileX = (long) Math.floor((coordinate.x() - origin.x()) / tileMapWidth);
                tileY = (long) Math.floor((origin.y() - coordinate.y()) / tileMapHeight);
            }
            case LOWER_RIGHT -> {
                tileX = (long) Math.floor((origin.x() - coordinate.x()) / tileMapWidth);
                tileY = (long) Math.floor((coordinate.y() - origin.y()) / tileMapHeight);
            }
            case UPPER_RIGHT -> {
                tileX = (long) Math.floor((origin.x() - coordinate.x()) / tileMapWidth);
                tileY = (long) Math.floor((origin.y() - coordinate.y()) / tileMapHeight);
            }
            default -> throw new IllegalStateException("Unsupported axis origin: " + tilePyramid.axisOrigin());
        }

        TileIndex tile = TileIndex.of(tileX, tileY, zoomLevel);

        // Clamp to pyramid bounds if outside
        TileRange levelRange = tilePyramid.tileRange(zoomLevel);
        if (!levelRange.contains(tile)) {
            tileX = Math.max(levelRange.minx(), Math.min(levelRange.maxx(), tileX));
            tileY = Math.max(levelRange.miny(), Math.min(levelRange.maxy(), tileY));
            tile = TileIndex.of(tileX, tileY, zoomLevel);
        }

        return tile;
    }

    @Override
    public TileRange extentToRange(Extent extent, int zoomLevel) {
        validateZoomLevel(zoomLevel);

        // Find tiles at the corners of the extent
        TileIndex minTile = coordinateToTile(extent.min(), zoomLevel);
        TileIndex maxTile = coordinateToTile(extent.max(), zoomLevel);

        // Create range covering all tiles that intersect the extent
        return TileRange.of(
                Math.min(minTile.x(), maxTile.x()),
                Math.min(minTile.y(), maxTile.y()),
                Math.max(minTile.x(), maxTile.x()),
                Math.max(minTile.y(), maxTile.y()),
                zoomLevel,
                tilePyramid.axisOrigin());
    }

    private void validateZoomLevel(int zoomLevel) {
        if (!tilePyramid.hasZoom(zoomLevel)) {
            throw new IllegalArgumentException("Zoom level " + zoomLevel + " is not supported by this tile matrix set");
        }
    }

    // TileMatrix-based API implementation

    @Override
    public List<TileMatrix> tileMatrices() {
        return tilePyramid.levels().stream().map(this::createTileMatrix).toList();
    }

    @Override
    public Optional<TileMatrix> tileMatrix(int zoomLevel) {
        return tilePyramid.level(zoomLevel).map(this::createTileMatrix);
    }

    @Override
    public Optional<TileMatrixSet> intersection(Extent mapExtent) {
        return TileMatrixSetView.intersection(this, mapExtent);
    }

    @Override
    public TileMatrixSet subset(int minZoomLevel, int maxZoomLevel) {
        return TileMatrixSetView.subset(this, minZoomLevel, maxZoomLevel);
    }

    /**
     * Creates a TileMatrix from a TileRange and this matrix set's spatial
     * properties.
     */
    private TileMatrix createTileMatrix(TileRange tileRange) {
        int z = tileRange.zoomLevel();

        // Calculate the extent covered by this tile range
        Extent matrixExtent = tileRange.extent(origin(z), resolution(z), tileWidth, tileHeight);

        return new TileMatrix(tileRange, resolutions[z], origins[z], matrixExtent, crsId, tileWidth, tileHeight);
    }

    static TileMatrixSet.Builder toBuilder(TileMatrixSet orig) {
        TileMatrixSet.Builder builder = new TileMatrixSet.Builder()
                .tilePyramid(orig.tilePyramid())
                .crs(orig.crsId())
                .tileSize(orig.tileWidth(), orig.tileHeight())
                .extent(orig.extent());

        // For StandardTileMatrixSet records, we can access the arrays directly
        if (orig instanceof StandardTileMatrixSet std) {
            return builder.resolutions(std.resolutions()).origins(std.origins());
        }

        // For other implementations, build arrays from zoom level data
        int minZoom = orig.minZoomLevel();
        int maxZoom = orig.maxZoomLevel();
        double[] resolutions = new double[maxZoom + 1];
        Coordinate[] origins = new Coordinate[maxZoom + 1];

        for (int z = minZoom; z <= maxZoom; z++) {
            resolutions[z] = orig.resolution(z);
            origins[z] = orig.origin(z);
        }

        return builder.resolutions(resolutions).origins(origins);
    }
}
