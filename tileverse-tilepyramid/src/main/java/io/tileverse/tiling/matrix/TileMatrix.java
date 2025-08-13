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

import static java.util.Objects.requireNonNull;

import io.tileverse.tiling.pyramid.TileIndex;
import io.tileverse.tiling.pyramid.TileRange;
import java.util.Optional;

/**
 * A tile matrix represents a collection of tiles with the same size and properties
 * placed on a regular grid with no overlapping, within a specific coordinate reference system.
 *
 * <p>A tile matrix combines the discrete tile grid ({@link TileRange}) with the spatial
 * properties needed for coordinate transformations (resolution, origin, extent).
 * This allows spatial operations to be performed directly on the matrix without
 * requiring external coordinate arrays.
 *
 * <p>Key properties:
 * <ul>
 * <li><b>Tile Range</b>: The discrete grid of tiles (X, Y coordinates and zoom level)</li>
 * <li><b>Resolution</b>: Map units per pixel at this zoom level</li>
 * <li><b>Origin</b>: Map space coordinate corresponding to tile (0,0)</li>
 * <li><b>Extent</b>: The map space bounding box covered by this matrix</li>
 * <li><b>CRS</b>: Coordinate reference system identifier</li>
 * <li><b>Tile Size</b>: Pixel dimensions of each tile</li>
 * </ul>
 *
 * @param tileRange the discrete tile grid for this matrix
 * @param resolution map units per pixel
 * @param origin map space coordinate for tile (0,0)
 * @param extent map space bounding box covered by this matrix
 * @param crsId coordinate reference system identifier
 * @param tileWidth tile width in pixels
 * @param tileHeight tile height in pixels
 *
 * @since 1.0
 */
public record TileMatrix(
        TileRange tileRange,
        double resolution,
        Coordinate origin,
        Extent extent,
        String crsId,
        int tileWidth,
        int tileHeight) {

    /**
     * Compact constructor with validation.
     */
    public TileMatrix {
        if (tileRange == null) {
            throw new IllegalArgumentException("tileRange cannot be null");
        }
        if (origin == null) {
            throw new IllegalArgumentException("origin cannot be null");
        }
        if (extent == null) {
            throw new IllegalArgumentException("extent cannot be null");
        }
        if (crsId == null || crsId.trim().isEmpty()) {
            throw new IllegalArgumentException("crsId cannot be null or empty");
        }
        if (resolution <= 0) {
            throw new IllegalArgumentException("resolution must be positive");
        }
        if (tileWidth <= 0 || tileHeight <= 0) {
            throw new IllegalArgumentException("tile dimensions must be positive");
        }
    }

    /**
     * Returns the zoom level for this tile matrix.
     *
     * @return the zoom level
     */
    public int zoomLevel() {
        return tileRange.zoomLevel();
    }

    /**
     * Returns the tile at the specified index, or empty if not within this matrix.
     *
     * @param tileIndex the tile index
     * @return the tile at the specified index, or empty if not within bounds
     */
    public Optional<Tile> tile(TileIndex tileIndex) {
        return Optional.of(requireNonNull(tileIndex, "tileIndex"))
                .filter(tileRange::contains)
                .map(this::buildTile);
    }

    private Tile buildTile(TileIndex index) {
        Extent tileExtent = tileExtent(index);
        return Tile.builder()
                .tileIndex(index)
                .extent(tileExtent)
                .size(tileWidth, tileHeight)
                .resolution(resolution)
                .crs(crsId)
                .build();
    }

    /**
     * Returns the tile at the specified coordinates.
     *
     * @param x the tile X coordinate
     * @param y the tile Y coordinate
     * @return the tile at the specified coordinates, or empty if not within bounds
     */
    public Optional<Tile> tile(long x, long y) {
        return tile(TileIndex.of(x, y, zoomLevel()));
    }

    /**
     * Returns the map space extent covered by a specific tile index.
     *
     * @param tileIndex the tile index
     * @return the map space extent of the tile
     * @throws IllegalArgumentException if the tile is not within this matrix
     */
    private Extent tileExtent(TileIndex tileIndex) {
        if (!tileRange.contains(tileIndex)) {
            throw new IllegalArgumentException("Tile " + tileIndex + " is not within matrix bounds");
        }

        // Calculate tile extent based on axis origin
        double tileMapWidth = tileWidth * resolution;
        double tileMapHeight = tileHeight * resolution;

        double minX, minY, maxX, maxY;

        // Transform tile coordinates to map coordinates based on axis origin
        switch (tileRange.axisOrigin()) {
            case LOWER_LEFT -> {
                minX = origin.x() + tileIndex.x() * tileMapWidth;
                minY = origin.y() + tileIndex.y() * tileMapHeight;
                maxX = minX + tileMapWidth;
                maxY = minY + tileMapHeight;
            }
            case UPPER_LEFT -> {
                minX = origin.x() + tileIndex.x() * tileMapWidth;
                maxY = origin.y() - tileIndex.y() * tileMapHeight;
                maxX = minX + tileMapWidth;
                minY = maxY - tileMapHeight;
            }
            case LOWER_RIGHT -> {
                maxX = origin.x() - tileIndex.x() * tileMapWidth;
                minY = origin.y() + tileIndex.y() * tileMapHeight;
                minX = maxX - tileMapWidth;
                maxY = minY + tileMapHeight;
            }
            case UPPER_RIGHT -> {
                maxX = origin.x() - tileIndex.x() * tileMapWidth;
                maxY = origin.y() - tileIndex.y() * tileMapHeight;
                minX = maxX - tileMapWidth;
                minY = maxY - tileMapHeight;
            }
            default -> throw new IllegalStateException("Unsupported axis origin: " + tileRange.axisOrigin());
        }

        return Extent.of(minX, minY, maxX, maxY);
    }

    /**
     * Converts a map space coordinate to the corresponding tile.
     * If the coordinate falls outside the matrix extent, returns empty.
     *
     * @param coordinate the map space coordinate
     * @return the tile containing the coordinate, or empty if outside matrix bounds
     */
    public Optional<Tile> coordinateToTile(Coordinate coordinate) {
        // Calculate tile size in map units
        double tileMapWidth = tileWidth * resolution;
        double tileMapHeight = tileHeight * resolution;

        long tileX, tileY;

        // Transform map coordinates to tile coordinates based on axis origin
        switch (tileRange.axisOrigin()) {
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
            default -> throw new IllegalStateException("Unsupported axis origin: " + tileRange.axisOrigin());
        }

        return tile(TileIndex.of(tileX, tileY, zoomLevel()));
    }

    /**
     * Converts a map space extent to the corresponding tile range within this matrix.
     * The returned range covers all tiles that intersect with the given extent.
     *
     * @param mapExtent the map space extent
     * @return the tile range covering the extent, intersected with matrix bounds
     */
    public Optional<TileRange> extentToRange(Extent mapExtent) {
        // Calculate tile coordinates for corner points without bounds checking
        double tileMapWidth = tileWidth * resolution;
        double tileMapHeight = tileHeight * resolution;

        long minTileX, minTileY, maxTileX, maxTileY;

        // Transform map coordinates to tile coordinates based on axis origin
        Coordinate minCoord = mapExtent.min();
        Coordinate maxCoord = mapExtent.max();

        switch (tileRange.axisOrigin()) {
            case LOWER_LEFT -> {
                minTileX = (long) Math.floor((minCoord.x() - origin.x()) / tileMapWidth);
                minTileY = (long) Math.floor((minCoord.y() - origin.y()) / tileMapHeight);
                maxTileX = (long) Math.floor((maxCoord.x() - origin.x()) / tileMapWidth);
                maxTileY = (long) Math.floor((maxCoord.y() - origin.y()) / tileMapHeight);
            }
            case UPPER_LEFT -> {
                minTileX = (long) Math.floor((minCoord.x() - origin.x()) / tileMapWidth);
                minTileY = (long) Math.floor((origin.y() - maxCoord.y()) / tileMapHeight); // Note: swapped for Y
                maxTileX = (long) Math.floor((maxCoord.x() - origin.x()) / tileMapWidth);
                maxTileY = (long) Math.floor((origin.y() - minCoord.y()) / tileMapHeight); // Note: swapped for Y
            }
            case LOWER_RIGHT -> {
                minTileX = (long) Math.floor((origin.x() - maxCoord.x()) / tileMapWidth); // Note: swapped for X
                minTileY = (long) Math.floor((minCoord.y() - origin.y()) / tileMapHeight);
                maxTileX = (long) Math.floor((origin.x() - minCoord.x()) / tileMapWidth); // Note: swapped for X
                maxTileY = (long) Math.floor((maxCoord.y() - origin.y()) / tileMapHeight);
            }
            case UPPER_RIGHT -> {
                minTileX = (long) Math.floor((origin.x() - maxCoord.x()) / tileMapWidth); // Note: swapped for X
                minTileY = (long) Math.floor((origin.y() - maxCoord.y()) / tileMapHeight); // Note: swapped for Y
                maxTileX = (long) Math.floor((origin.x() - minCoord.x()) / tileMapWidth); // Note: swapped for X
                maxTileY = (long) Math.floor((origin.y() - minCoord.y()) / tileMapHeight); // Note: swapped for Y
            }
            default -> throw new IllegalStateException("Unsupported axis origin: " + tileRange.axisOrigin());
        }

        // Ensure min/max are in correct order
        long actualMinX = Math.min(minTileX, maxTileX);
        long actualMinY = Math.min(minTileY, maxTileY);
        long actualMaxX = Math.max(minTileX, maxTileX);
        long actualMaxY = Math.max(minTileY, maxTileY);

        TileRange extentRange =
                TileRange.of(actualMinX, actualMinY, actualMaxX, actualMaxY, zoomLevel(), tileRange.axisOrigin());

        // Intersect with matrix bounds to ensure validity
        return tileRange.intersection(extentRange);
    }

    /**
     * Returns true if this tile matrix contains the specified tile index.
     *
     * @param tileIndex the tile index to check
     * @return true if the matrix contains the tile index
     * @throws IllegalArgumentException if tileIndex is null
     */
    public boolean contains(TileIndex tileIndex) {
        if (tileIndex == null) {
            throw new IllegalArgumentException("tileIndex cannot be null");
        }
        return tileRange.contains(tileIndex);
    }

    /**
     * Returns true if this tile matrix contains the specified tile.
     *
     * @param tile the tile to check
     * @return true if the matrix contains the tile
     * @throws IllegalArgumentException if tile is null
     */
    public boolean contains(Tile tile) {
        if (tile == null) {
            throw new IllegalArgumentException("tile cannot be null");
        }
        return contains(tile.tileIndex());
    }

    /**
     * Returns all tiles in this matrix as a stream.
     * This provides efficient iteration over all tiles without materializing them all at once.
     *
     * @return a stream of all tiles in this matrix
     */
    public java.util.stream.Stream<Tile> tiles() {
        // Create a stream that iterates through all tiles in the range
        return java.util.stream.Stream.iterate(
                        tileRange.first(),
                        tileIndex -> tileRange.next(tileIndex).isPresent(),
                        tileIndex -> tileRange.next(tileIndex).orElse(null))
                .map(tileIndex ->
                        tile(tileIndex).orElseThrow()); // Should never be empty since we're iterating valid indices
    }

    /**
     * Returns the number of tiles in this matrix.
     *
     * @return the tile count
     */
    public long tileCount() {
        return tileRange.count();
    }

    /**
     * Returns the width of this matrix in tiles (X span).
     *
     * @return the number of tiles in the X direction
     */
    public long spanX() {
        return tileRange.spanX();
    }

    /**
     * Returns the height of this matrix in tiles (Y span).
     *
     * @return the number of tiles in the Y direction
     */
    public long spanY() {
        return tileRange.spanY();
    }

    /**
     * Returns the first tile in natural traversal order for this axis origin.
     *
     * @return the first tile in traversal order
     */
    public Tile first() {
        return tile(tileRange.first()).orElseThrow();
    }

    /**
     * Returns the last tile in natural traversal order for this axis origin.
     *
     * @return the last tile in traversal order
     */
    public Tile last() {
        return tile(tileRange.last()).orElseThrow();
    }

    /**
     * Returns the next tile in natural traversal order after the given tile.
     *
     * @param current the current tile
     * @return the next tile in traversal order, or empty if current is the last tile
     * @throws IllegalArgumentException if current is null
     */
    public Optional<Tile> next(Tile current) {
        if (current == null) {
            throw new IllegalArgumentException("current tile cannot be null");
        }
        return tileRange.next(current.tileIndex()).flatMap(this::tile);
    }

    /**
     * Returns the previous tile in natural traversal order before the given tile.
     *
     * @param current the current tile
     * @return the previous tile in traversal order, or empty if current is the first tile
     * @throws IllegalArgumentException if current is null
     */
    public Optional<Tile> prev(Tile current) {
        if (current == null) {
            throw new IllegalArgumentException("current tile cannot be null");
        }
        return tileRange.prev(current.tileIndex()).flatMap(this::tile);
    }

    /**
     * Returns true if this matrix contains all tiles that are in the given matrix.
     * This checks if the given matrix's tile range is completely contained within this matrix's range.
     * Returns false if the other matrix is empty (empty matrix contains no tiles).
     *
     * @param other the other tile matrix to check
     * @return true if this matrix contains all tiles in the other matrix
     * @throws IllegalArgumentException if other is null or if the matrices have different zoom levels
     */
    public boolean contains(TileMatrix other) {
        if (other == null) {
            throw new IllegalArgumentException("other matrix cannot be null");
        }
        if (other.zoomLevel() != zoomLevel()) {
            throw new IllegalArgumentException(
                    "Cannot compare matrices with different zoom levels: " + zoomLevel() + " vs " + other.zoomLevel());
        }
        // Check if this range contains the other range by checking boundaries
        TileRange otherRange = other.tileRange;
        return tileRange.minx() <= otherRange.minx()
                && tileRange.miny() <= otherRange.miny()
                && tileRange.maxx() >= otherRange.maxx()
                && tileRange.maxy() >= otherRange.maxy();
    }

    /**
     * Returns true if this tile matrix intersects with the given extent.
     *
     * @param mapExtent the map space extent to check
     * @return true if the matrix intersects with the extent
     */
    public boolean intersects(Extent mapExtent) {
        return extent.intersects(mapExtent);
    }

    /**
     * Returns a new TileMatrix containing only the tiles that intersect with the given extent.
     * The returned matrix has the same spatial properties but a filtered tile range.
     *
     * @param mapExtent the map space extent to intersect with
     * @return a new TileMatrix containing only intersecting tiles, or empty matrix if no intersection
     */
    public Optional<TileMatrix> intersection(Extent mapExtent) {
        return extentToRange(mapExtent).flatMap(tileRange::intersection).map(this::withTileRange);
    }

    /**
     * Creates a new TileMatrix with a different tile range but same spatial properties.
     * Useful for creating subsets or filtered versions of this matrix.
     *
     * @param newRange the new tile range
     * @return a new TileMatrix with the updated range
     * @throws IllegalArgumentException if the new range has a different zoom level
     */
    public TileMatrix withTileRange(TileRange newRange) {
        if (requireNonNull(newRange).equals(tileRange)) {
            return this;
        }

        if (newRange.zoomLevel() != zoomLevel()) {
            throw new IllegalArgumentException("New range zoom level " + newRange.zoomLevel()
                    + " does not match current zoom level " + zoomLevel());
        }
        // Calculate new extent based on the new tile range
        Extent newExtent = newRange.extent(origin, resolution, tileWidth, tileHeight);
        return new TileMatrix(newRange, resolution, origin, newExtent, crsId, tileWidth, tileHeight);
    }

    /**
     * Builder for creating TileMatrix instances.
     */
    public static class Builder {
        private TileRange tileRange;
        private double resolution;
        private Coordinate origin;
        private Extent extent;
        private String crsId;
        private int tileWidth = 256;
        private int tileHeight = 256;

        public Builder tileRange(TileRange tileRange) {
            this.tileRange = tileRange;
            return this;
        }

        public Builder resolution(double resolution) {
            this.resolution = resolution;
            return this;
        }

        public Builder origin(Coordinate origin) {
            this.origin = origin;
            return this;
        }

        public Builder extent(Extent extent) {
            this.extent = extent;
            return this;
        }

        public Builder crs(String crsId) {
            this.crsId = crsId;
            return this;
        }

        public Builder tileSize(int width, int height) {
            this.tileWidth = width;
            this.tileHeight = height;
            return this;
        }

        public TileMatrix build() {
            return new TileMatrix(tileRange, resolution, origin, extent, crsId, tileWidth, tileHeight);
        }
    }

    /**
     * Creates a new builder for TileMatrix.
     *
     * @return a new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }
}
