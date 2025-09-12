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
import io.tileverse.tiling.common.Coordinate;
import io.tileverse.tiling.common.CornerOfOrigin;
import io.tileverse.tiling.pyramid.TileIndex;
import io.tileverse.tiling.pyramid.TilePyramid;
import io.tileverse.tiling.pyramid.TileRange;
import java.util.ArrayList;
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
        BoundingBox2D extent,
        double[] resolutions)
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
    public BoundingBox2D boundingBox() {
        return extent;
    }

    @Override
    public Coordinate origin() {
        return tilePyramid.cornerOfOrigin().pointOfOrigin(boundingBox());
    }

    static Coordinate origin(BoundingBox2D extent, CornerOfOrigin cornerOfOrigin) {
        return cornerOfOrigin.pointOfOrigin(extent);
    }

    @Override
    public double resolution(int zoomLevel) {
        validateZoomLevel(zoomLevel);
        return resolutions[zoomLevel];
    }

    @Override
    public TileIndex coordinateToTile(Coordinate coordinate, int zoomLevel) {
        validateZoomLevel(zoomLevel);

        double resolution = resolution(zoomLevel);

        // Calculate tile size in map units
        final double tileMapWidth = tileWidth * resolution;
        final double tileMapHeight = tileHeight * resolution;
        final Coordinate origin = origin();

        long tileX, tileY;

        // Transform map coordinates to tile coordinates based on corner of origin
        // Uses epsilon adjustment per OGC TileMatrixSet spec to handle floating-point precision
        switch (tilePyramid.cornerOfOrigin()) {
            case BOTTOM_LEFT -> {
                tileX = floorWithEpsilon((coordinate.x() - origin.x()) / tileMapWidth);
                tileY = floorWithEpsilon((coordinate.y() - origin.y()) / tileMapHeight);
            }
            case TOP_LEFT -> {
                tileX = floorWithEpsilon((coordinate.x() - origin.x()) / tileMapWidth);
                tileY = floorWithEpsilon((origin.y() - coordinate.y()) / tileMapHeight);
            }
            default -> throw new IllegalStateException("Unsupported corner of origin: " + tilePyramid.cornerOfOrigin());
        }

        TileIndex tile = TileIndex.xyz(tileX, tileY, zoomLevel);

        // Clamp to pyramid bounds if outside
        TileRange levelRange = tilePyramid.tileRange(zoomLevel);
        if (!levelRange.contains(tile)) {
            tileX = Math.max(levelRange.minx(), Math.min(levelRange.maxx(), tileX));
            tileY = Math.max(levelRange.miny(), Math.min(levelRange.maxy(), tileY));
            tile = TileIndex.xyz(tileX, tileY, zoomLevel);
        }

        return tile;
    }

    private void validateZoomLevel(int zoomLevel) {
        if (!tilePyramid.hasZoom(zoomLevel)) {
            throw new IllegalArgumentException("Zoom level " + zoomLevel + " is not supported by this tile matrix set");
        }
    }

    // TileMatrix-based API implementation

    @Override
    public List<TileMatrix> tileMatrices() {
        List<TileRange> levels = tilePyramid.levels();
        List<TileMatrix> matrices = new ArrayList<>(levels.size());
        for (int i = 0; i < levels.size(); i++) {
            matrices.add(createTileMatrix(levels.get(i)));
        }
        return matrices;
        // return tilePyramid.levels().stream().map(this::createTileMatrix).toList();
    }

    @Override
    public Optional<TileMatrix> tileMatrix(int zoomLevel) {
        return tilePyramid.level(zoomLevel).map(this::createTileMatrix);
    }

    @Override
    public Optional<TileMatrixSet> intersection(BoundingBox2D mapExtent) {
        return TileMatrixSetView.intersection(this, mapExtent);
    }

    @Override
    public TileMatrixSet subset(int minZoomLevel, int maxZoomLevel) {
        return TileMatrixSetView.subset(this, minZoomLevel, maxZoomLevel);
    }

    /**
     * Creates a TileMatrix from a TileRange and this matrixset's spatial
     * properties.
     */
    private TileMatrix createTileMatrix(TileRange tileRange) {
        int z = tileRange.zoomLevel();

        return new TileMatrix(tileRange, resolutions[z], origin(), crsId, tileWidth, tileHeight);
    }

    static TileMatrixSetBuilder toBuilder(TileMatrixSet orig) {
        TileMatrixSetBuilder builder = new TileMatrixSetBuilder()
                .tilePyramid(orig.tilePyramid())
                .crs(orig.crsId())
                .tileSize(orig.tileWidth(), orig.tileHeight())
                .extent(orig.boundingBox());

        // For StandardTileMatrixSet records, we can access the arrays directly
        if (orig instanceof StandardTileMatrixSet std) {
            return builder.resolutions(std.resolutions());
        }

        // For other implementations, build arrays from zoom level data
        int minZoom = orig.minZoomLevel();
        int maxZoom = orig.maxZoomLevel();
        double[] resolutions = new double[maxZoom + 1];

        for (int z = minZoom; z <= maxZoom; z++) {
            resolutions[z] = orig.resolution(z);
        }

        return builder.resolutions(resolutions);
    }

    /**
     * Applies floor function with epsilon adjustment to handle floating-point precision issues.
     * Follows OGC TileMatrixSet specification Annex I recommendations.
     *
     * @param value the floating-point value to floor
     * @return the floor value with epsilon compensation for precision
     */
    private static long floorWithEpsilon(double value) {
        // For coordinate-to-tile transformations, add small epsilon to avoid precision issues
        return (long) Math.floor(value + 1e-6);
    }
}
