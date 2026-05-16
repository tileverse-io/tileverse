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

import io.tileverse.geom.BoundingBox2D;
import io.tileverse.geom.Coordinate;
import io.tileverse.tiling.pyramid.CornerOfOrigin;
import io.tileverse.tiling.pyramid.TileIndex;
import io.tileverse.tiling.pyramid.TilePyramid;
import io.tileverse.tiling.pyramid.TileRange;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Standard implementation of TileMatrixSet that provides coordinate transformation between tile space and map space
 * using composition with a TilePyramid.
 *
 * <p>This implementation supports common tiling schemes such as:
 *
 * <ul>
 *   <li>Web Mercator (EPSG:3857) - Google/OSM style tiling
 *   <li>Geographic (EPSG:4326) - WGS84 longitude/latitude
 *   <li>Custom projected coordinate systems
 * </ul>
 *
 * @param identifier the OGC TMS identifier (clause 7.1)
 * @param tilePyramid the tile pyramid defining the tiling structure
 * @param crsId the coordinate reference system identifier
 * @param supportedCRS URI form of the CRS (clause 7.1); may equal {@code URI.create(crsId)} when {@code crsId} is
 *     already a URI
 * @param tileWidth the width of each tile in pixels
 * @param tileHeight the height of each tile in pixels
 * @param extent the bounding box covering all tiles
 * @param resolutions the map resolution for each zoom level
 * @param title optional human-readable title (clause 7.1)
 * @param abstractDescription optional narrative description (clause 7.1)
 * @param keywords optional keywords (clause 7.1)
 * @param wellKnownScaleSet optional URI of a well-known scale set (clause 7.1)
 * @since 1.0
 */
@SuppressWarnings("java:S6207")
public record StandardTileMatrixSet(
        String identifier,
        TilePyramid tilePyramid,
        String crsId,
        URI supportedCRS,
        int tileWidth,
        int tileHeight,
        BoundingBox2D extent,
        double[] resolutions,
        Optional<String> title,
        Optional<String> abstractDescription,
        List<String> keywords,
        Optional<URI> wellKnownScaleSet)
        implements TileMatrixSet {

    /** Compact constructor: validates required fields and defensively copies the keywords list. */
    public StandardTileMatrixSet {
        requireNonNull(identifier, "identifier");
        requireNonNull(tilePyramid, "tilePyramid");
        requireNonNull(crsId, "crsId");
        requireNonNull(supportedCRS, "supportedCRS");
        requireNonNull(extent, "extent");
        requireNonNull(resolutions, "resolutions");
        requireNonNull(title, "title");
        requireNonNull(abstractDescription, "abstractDescription");
        requireNonNull(wellKnownScaleSet, "wellKnownScaleSet");
        keywords = List.copyOf(requireNonNull(keywords, "keywords"));
        if (identifier.isBlank()) {
            throw new IllegalArgumentException("identifier cannot be blank");
        }
    }

    @Override
    public String identifier() {
        return identifier;
    }

    @Override
    public TilePyramid tilePyramid() {
        return tilePyramid;
    }

    @Override
    public String crsId() {
        return crsId;
    }

    @Override
    public URI supportedCRS() {
        return supportedCRS;
    }

    @Override
    public Optional<String> title() {
        return title;
    }

    @Override
    public Optional<String> abstractDescription() {
        return abstractDescription;
    }

    @Override
    public List<String> keywords() {
        return keywords;
    }

    @Override
    public Optional<URI> wellKnownScaleSet() {
        return wellKnownScaleSet;
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

        long tileX;
        long tileY;

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

    /** Creates a TileMatrix from a TileRange and this matrixset's spatial properties. */
    private TileMatrix createTileMatrix(TileRange tileRange) {
        int z = tileRange.zoomLevel();

        return new TileMatrix(tileRange, resolutions[z], origin(), crsId, tileWidth, tileHeight);
    }

    static TileMatrixSetBuilder toBuilder(TileMatrixSet orig) {
        TileMatrixSetBuilder builder = new TileMatrixSetBuilder()
                .identifier(orig.identifier())
                .tilePyramid(orig.tilePyramid())
                .crs(orig.crsId())
                .supportedCRS(orig.supportedCRS())
                .tileSize(orig.tileWidth(), orig.tileHeight())
                .extent(orig.boundingBox());

        orig.title().ifPresent(builder::title);
        orig.abstractDescription().ifPresent(builder::abstractDescription);
        if (!orig.keywords().isEmpty()) {
            builder.keywords(orig.keywords());
        }
        orig.wellKnownScaleSet().ifPresent(builder::wellKnownScaleSet);

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
     * Applies floor function with epsilon adjustment to handle floating-point precision issues. Follows OGC
     * TileMatrixSet specification Annex I recommendations.
     *
     * @param value the floating-point value to floor
     * @return the floor value with epsilon compensation for precision
     */
    private static long floorWithEpsilon(double value) {
        // For coordinate-to-tile transformations, add small epsilon to avoid precision issues
        return (long) Math.floor(value + 1e-6);
    }

    // The default record-generated equals/hashCode/toString compare the resolutions array by reference
    // and print its identity hash, which gives wrong semantics for value-based comparison and logging.
    // Override to use Arrays.equals / Arrays.hashCode / Arrays.toString on the array component.

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof StandardTileMatrixSet other)) {
            return false;
        }
        return tileWidth == other.tileWidth
                && tileHeight == other.tileHeight
                && Objects.equals(identifier, other.identifier)
                && Objects.equals(tilePyramid, other.tilePyramid)
                && Objects.equals(crsId, other.crsId)
                && Objects.equals(supportedCRS, other.supportedCRS)
                && Objects.equals(extent, other.extent)
                && Arrays.equals(resolutions, other.resolutions)
                && Objects.equals(title, other.title)
                && Objects.equals(abstractDescription, other.abstractDescription)
                && Objects.equals(keywords, other.keywords)
                && Objects.equals(wellKnownScaleSet, other.wellKnownScaleSet);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(
                identifier,
                tilePyramid,
                crsId,
                supportedCRS,
                tileWidth,
                tileHeight,
                extent,
                title,
                abstractDescription,
                keywords,
                wellKnownScaleSet);
        result = 31 * result + Arrays.hashCode(resolutions);
        return result;
    }

    @Override
    public String toString() {
        return "StandardTileMatrixSet[identifier=" + identifier + ", crsId=" + crsId + ", tileWidth=" + tileWidth
                + ", tileHeight=" + tileHeight + ", extent=" + extent + ", resolutions="
                + Arrays.toString(resolutions) + "]";
    }
}
