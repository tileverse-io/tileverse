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

import io.tileverse.tiling.common.BoundingBox2D;
import io.tileverse.tiling.common.Coordinate;
import io.tileverse.tiling.common.CornerOfOrigin;
import io.tileverse.tiling.pyramid.TileIndex;
import io.tileverse.tiling.pyramid.TileRange;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

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
 * @param tileRange the discrete tile grid for this matrix, enabled operations on {@link TileIndex} and provides  {@link #matrixWidth()}, {@link #matrixHeight()}, {@link #cornerOfOrigin()}, and more grid related info and operations
 * @param resolution map units per pixel
 * @param pointOfOrigin in CRS units for the 0,0 tile corner
 * @param crsId coordinate reference system identifier
 * @param tileWidth tile width in pixels
 * @param tileHeight tile height in pixels
 *
 * @since 1.0
 */
public record TileMatrix(
        TileRange tileRange, double resolution, Coordinate pointOfOrigin, String crsId, int tileWidth, int tileHeight) {

    /**
     * Compact constructor with validation.
     */
    public TileMatrix {
        if (tileRange == null) {
            throw new IllegalArgumentException("tileRange cannot be null");
        }
        if (pointOfOrigin == null) {
            throw new IllegalArgumentException("pointOfOrigin cannot be null");
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
     * Corner of the tile matrix used as the origin for numbering tile rows and
     * columns.
     */
    public CornerOfOrigin cornerOfOrigin() {
        return tileRange.cornerOfOrigin();
    }

    /**
     * Position in CRS coordinates of the {@link #cornerOfOrigin() corner of origin} for a tile matrix.
     *
     * @return the origin coordinate of the {@link #first()} tile in map space
     */
    public Coordinate pointOfOrigin() {
        return pointOfOrigin;
    }

    public BoundingBox2D boundingBox() {
        return calculateExtent(tileRange, pointOfOrigin(), resolution(), tileWidth(), tileHeight());
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
    public long matrixWidth() {
        return tileRange.spanX();
    }

    /**
     * Returns the height of this matrix in tiles (Y span).
     *
     * @return the number of tiles in the Y direction
     */
    public long matrixHeight() {
        return tileRange.spanY();
    }

    /**
     * Returns the tile at the specified coordinates.
     *
     * @param x the tile X coordinate
     * @param y the tile Y coordinate
     * @return the tile at the specified coordinates, or empty if not within bounds
     */
    public Optional<Tile> tile(long x, long y) {
        return tile(TileIndex.xyz(x, y, zoomLevel()));
    }

    /**
     * Returns the tile at the specified index, or empty if not within this matrix.
     *
     * @param tileIndex the tile index
     * @return the tile at the specified index, or empty if not within bounds
     */
    public Optional<Tile> tile(TileIndex tileIndex) {
        requireNonNull(tileIndex, "tileIndex");
        if (tileRange.contains(tileIndex)) {
            return Optional.of(buildTile(tileIndex));
        }
        return Optional.empty();
    }

    private Tile buildTile(TileIndex index) {
        BoundingBox2D tileExtent = tileExtent(index);
        return new Tile(index, tileExtent, tileWidth, tileHeight, resolution, crsId);
    }

    /**
     * Returns the map space extent covered by a specific tile index.
     *
     * @param tileIndex the tile index
     * @return the map space extent of the tile
     * @throws IllegalArgumentException if the tile is not within this matrix
     */
    private BoundingBox2D tileExtent(TileIndex tileIndex) {
        if (!tileRange.contains(tileIndex)) {
            throw new IllegalArgumentException("Tile " + tileIndex + " is not within matrix bounds");
        }
        // coordinate of the 0,0 tile, regardless of whether this is a matrix subset
        final Coordinate origin = pointOfOrigin();

        // Calculate tile extent based on corner of origin
        final double tileMapWidth = tileWidth * resolution;
        final double tileMapHeight = tileHeight * resolution;

        double minX, minY, maxX, maxY;
        // Transform tile coordinates to map coordinates based on corner of origin
        switch (this.cornerOfOrigin()) {
            case BOTTOM_LEFT -> {
                minX = origin.x() + tileIndex.x() * tileMapWidth;
                minY = origin.y() + tileIndex.y() * tileMapHeight;
                maxX = minX + tileMapWidth;
                maxY = minY + tileMapHeight;
            }
            case TOP_LEFT -> {
                minX = origin.x() + tileIndex.x() * tileMapWidth;
                maxY = origin.y() - tileIndex.y() * tileMapHeight;
                maxX = minX + tileMapWidth;
                minY = maxY - tileMapHeight;
            }
            default -> throw new IllegalStateException("Unsupported corner of origin: " + tileRange.cornerOfOrigin());
        }

        return BoundingBox2D.extent(minX, minY, maxX, maxY);
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
        final double tileMapWidth = tileWidth * resolution;
        final double tileMapHeight = tileHeight * resolution;
        final Coordinate origin = pointOfOrigin();

        long tileX, tileY;

        // Transform map coordinates to tile coordinates based on corner of origin
        // Uses epsilon adjustment per OGC TileMatrixSet spec to handle floating-point precision
        switch (tileRange.cornerOfOrigin()) {
            case BOTTOM_LEFT -> {
                tileX = floorWithEpsilon((coordinate.x() - origin.x()) / tileMapWidth);
                tileY = floorWithEpsilon((coordinate.y() - origin.y()) / tileMapHeight);
            }
            case TOP_LEFT -> {
                tileX = floorWithEpsilon((coordinate.x() - origin.x()) / tileMapWidth);
                tileY = floorWithEpsilon((origin.y() - coordinate.y()) / tileMapHeight);
            }
            default -> throw new IllegalStateException("Unsupported corner of origin: " + tileRange.cornerOfOrigin());
        }

        return tile(TileIndex.xyz(tileX, tileY, zoomLevel()));
    }

    /**
     * Converts a map space extent to the corresponding tile range within this matrix.
     * The returned range covers all tiles that intersect with the given extent.
     *
     * <p>Implements OGC TileMatrixSet specification algorithm with epsilon adjustments
     * to handle floating-point precision issues per Annex I.
     *
     * @param mapExtent the map space extent
     * @return the tile range covering the extent, intersected with matrix bounds
     */
    public Optional<TileRange> extentToRange(BoundingBox2D mapExtent) {
        // Calculate tile coordinates for corner points without bounds checking
        final double tileMapWidth = tileWidth * resolution;
        final double tileMapHeight = tileHeight * resolution;
        final Coordinate origin = pointOfOrigin();

        long minTileX, minTileY, maxTileX, maxTileY;

        // Transform map coordinates to tile coordinates based on corner of origin
        // Uses epsilon adjustments per OGC spec: add epsilon for min, subtract for max
        Coordinate minCoord = mapExtent.lowerLeft();
        Coordinate maxCoord = mapExtent.upperRight();

        switch (tileRange.cornerOfOrigin()) {
            case BOTTOM_LEFT -> {
                minTileX = floorWithEpsilon((minCoord.x() - origin.x()) / tileMapWidth, true);
                minTileY = floorWithEpsilon((minCoord.y() - origin.y()) / tileMapHeight, true);
                maxTileX = floorWithEpsilon((maxCoord.x() - origin.x()) / tileMapWidth, false);
                maxTileY = floorWithEpsilon((maxCoord.y() - origin.y()) / tileMapHeight, false);
            }
            case TOP_LEFT -> {
                minTileX = floorWithEpsilon((minCoord.x() - origin.x()) / tileMapWidth, true);
                minTileY = floorWithEpsilon((origin.y() - maxCoord.y()) / tileMapHeight, true); // Note: swapped for Y
                maxTileX = floorWithEpsilon((maxCoord.x() - origin.x()) / tileMapWidth, false);
                maxTileY = floorWithEpsilon((origin.y() - minCoord.y()) / tileMapHeight, false); // Note: swapped for Y
            }
            default -> throw new IllegalStateException("Unsupported corner of origin: " + tileRange.cornerOfOrigin());
        }

        // Ensure min/max are in correct order
        long actualMinX = Math.min(minTileX, maxTileX);
        long actualMinY = Math.min(minTileY, maxTileY);
        long actualMaxX = Math.max(minTileX, maxTileX);
        long actualMaxY = Math.max(minTileY, maxTileY);

        TileRange extentRange =
                TileRange.of(actualMinX, actualMinY, actualMaxX, actualMaxY, zoomLevel(), tileRange.cornerOfOrigin());

        // Intersect with matrix bounds to ensure validity
        return tileRange.intersection(extentRange);
    }

    /**
     * Returns true if this tile matrix contains the specified tile index.
     *
     * @param tileIndex the tile index to check
     * @return true if the matrix contains the tile index
     */
    public boolean contains(TileIndex tileIndex) {
        requireNonNull(tileIndex, "tileIndex cannot be null");
        return tileRange.contains(tileIndex);
    }

    /**
     * Returns true if this tile matrix contains the specified tile.
     *
     * @param tile the tile to check
     * @return true if the matrix contains the tile
     */
    public boolean contains(Tile tile) {
        requireNonNull("tile cannot be null");
        return contains(tile.tileIndex());
    }

    /**
     * Returns all tiles in this matrix as a stream.
     * This provides efficient iteration over all tiles without materializing them all at once.
     *
     * @return a stream of all tiles in this matrix
     */
    public Stream<Tile> tiles() {
        return tileRange().all().map(this::tile).map(Optional::orElseThrow);
    }
    /**
     * Returns the first tile in natural traversal order for this corner of origin.
     *
     * @return the first tile in traversal order
     */
    public Tile first() {
        return tile(tileRange.first()).orElseThrow();
    }

    /**
     * Returns the last tile in natural traversal order for this corner of origin.
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
     */
    public Optional<Tile> next(Tile current) {
        requireNonNull(current, "current tile cannot be null");
        return tileRange.next(current.tileIndex()).map(this::buildTile);
    }

    /**
     * Returns the previous tile in natural traversal order before the given tile.
     *
     * @param current the current tile
     * @return the previous tile in traversal order, or empty if current is the first tile
     */
    public Optional<Tile> prev(Tile current) {
        requireNonNull(current, "current tile cannot be null");
        return tileRange.prev(current.tileIndex()).map(this::buildTile);
    }

    /**
     * Returns true if this matrix contains all tiles that are in the given matrix.
     * This checks if the given matrix's tile range is completely contained within this matrix's range.
     * Returns false if the other matrix is empty (empty matrix contains no tiles).
     *
     * @param other the other tile matrix to check
     * @return true if this matrix contains all tiles in the other matrix
     * @throws IllegalArgumentException if the matrices have different zoom levels
     */
    public boolean contains(TileMatrix other) {
        requireNonNull(other, "other matrix cannot be null");
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
    public boolean intersects(BoundingBox2D mapExtent) {
        return boundingBox().intersects(mapExtent);
    }

    /**
     * Returns a new TileMatrix containing only the tiles that intersect with the given extent.
     * The returned matrix has the same spatial properties but a filtered tile range.
     *
     * @param mapExtent the map space extent to intersect with
     * @return a new TileMatrix containing only intersecting tiles, or empty matrix if no intersection
     */
    public Optional<TileMatrix> intersection(BoundingBox2D mapExtent) {
        return extentToRange(mapExtent).flatMap(tileRange::intersection).map(this::withTileRange);
    }

    /**
     * Returns a (potentially sparse) tile matrix whose {@link #tileRange()
     * TileRange} is the union of this one's with {@code other}'s.
     * <p>
     * The two matrices must have the same {@link #zoomLevel()}
     * <p>
     * If the {@code other} matrix has a different {@link #cornerOfOrigin()}, the resulting
     * matrix will have this matrix's corner of origin.
     */
    public TileMatrix union(TileMatrix other) {
        requireNonNull(other);
        TileRange union = this.tileRange().union(other.tileRange);
        return this.withTileRange(union);
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

        Coordinate origin = pointOfOrigin();
        return new TileMatrix(newRange, resolution, origin, crsId, tileWidth, tileHeight);
    }

    /**
     * Calculates the map space extent covered by a tile range.
     * This bridges abstract tile coordinates to real-world spatial coordinates.
     */
    private static BoundingBox2D calculateExtent(
            TileRange tileRange, Coordinate origin, double resolution, int tileWidth, int tileHeight) {
        // Calculate tile size in map units
        final double tileMapWidth = tileWidth * resolution;
        final double tileMapHeight = tileHeight * resolution;

        double minX, minY, maxX, maxY;

        // Transform tile coordinates to map coordinates based on corner of origin
        switch (tileRange.cornerOfOrigin()) {
            case BOTTOM_LEFT -> {
                minX = origin.x() + tileRange.minx() * tileMapWidth;
                minY = origin.y() + tileRange.miny() * tileMapHeight;
                maxX = origin.x() + (tileRange.maxx() + 1) * tileMapWidth;
                maxY = origin.y() + (tileRange.maxy() + 1) * tileMapHeight;
            }
            case TOP_LEFT -> {
                minX = origin.x() + tileRange.minx() * tileMapWidth;
                maxY = origin.y() - tileRange.miny() * tileMapHeight;
                maxX = origin.x() + (tileRange.maxx() + 1) * tileMapWidth;
                minY = origin.y() - (tileRange.maxy() + 1) * tileMapHeight;
            }
            default -> throw new IllegalStateException("Unsupported corner of origin: " + tileRange.cornerOfOrigin());
        }

        return BoundingBox2D.extent(minX, minY, maxX, maxY);
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

    /**
     * Applies floor function with epsilon adjustment for extent-to-range transformations.
     * Follows OGC TileMatrixSet specification Annex I recommendations.
     *
     * @param value the floating-point value to floor
     * @param addEpsilon true to add epsilon (for min values), false to subtract (for max values)
     * @return the floor value with appropriate epsilon adjustment
     */
    private static long floorWithEpsilon(double value, boolean addEpsilon) {
        double epsilon = 1e-6;
        double adjusted = addEpsilon ? value + epsilon : value - epsilon;
        return (long) Math.floor(adjusted);
    }

    /**
     * Creates a new builder for TileMatrix.
     *
     * @return a new builder instance
     */
    public static TileMatrixBuilder builder() {
        return new TileMatrixBuilder();
    }

    public static TileMatrix union(List<TileMatrix> matrices) {
        if (matrices.isEmpty()) {
            throw new IllegalArgumentException("matrices is empty");
        }

        TileMatrix first = requireNonNull(matrices.get(0));
        TileMatrix union = first;
        for (int i = 1; i < matrices.size(); i++) {
            TileMatrix next = requireNonNull(matrices.get(i));
            union = union.union(next);
        }
        return union;
    }
}
