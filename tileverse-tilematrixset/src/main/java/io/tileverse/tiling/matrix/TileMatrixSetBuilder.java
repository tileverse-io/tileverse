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

import io.tileverse.geom.BoundingBox2D;
import io.tileverse.tiling.pyramid.TilePyramid;
import java.net.URI;
import java.util.List;
import java.util.Optional;

/** Builder for creating StandardTileMatrixSet instances. */
public class TileMatrixSetBuilder {
    private String identifier;
    private TilePyramid tilePyramid;
    private String crsId;
    private URI supportedCRS;
    private int tileWidth = 256;
    private int tileHeight = 256;
    private BoundingBox2D extent;
    private double[] resolutions;
    private Optional<String> title = Optional.empty();
    private Optional<String> abstractDescription = Optional.empty();
    private List<String> keywords = List.of();
    private Optional<URI> wellKnownScaleSet = Optional.empty();

    /**
     * Sets the OGC TMS {@code identifier} (clause 7.1). Required.
     *
     * @param identifier the tile matrix set identifier
     * @return this builder
     */
    public TileMatrixSetBuilder identifier(String identifier) {
        this.identifier = identifier;
        return this;
    }

    /**
     * Sets the OGC TMS {@code supportedCRS} URI (clause 7.1).
     *
     * <p>If not set, defaults to {@code URI.create(crsId)} at build time, which is correct only when {@code crsId} is
     * itself a URI.
     *
     * @param supportedCRS the CRS as a URI
     * @return this builder
     */
    public TileMatrixSetBuilder supportedCRS(URI supportedCRS) {
        this.supportedCRS = supportedCRS;
        return this;
    }

    /**
     * Sets the optional OGC TMS {@code title} (clause 7.1).
     *
     * @param title the title (may be null to clear)
     * @return this builder
     */
    public TileMatrixSetBuilder title(String title) {
        this.title = Optional.ofNullable(title);
        return this;
    }

    /**
     * Sets the optional OGC TMS {@code abstract} (clause 7.1). Named {@code abstractDescription} because
     * {@code abstract} is a Java keyword.
     *
     * @param abstractDescription the narrative description (may be null to clear)
     * @return this builder
     */
    public TileMatrixSetBuilder abstractDescription(String abstractDescription) {
        this.abstractDescription = Optional.ofNullable(abstractDescription);
        return this;
    }

    /**
     * Sets the optional OGC TMS {@code keywords} (clause 7.1).
     *
     * @param keywords the keywords (null becomes empty list)
     * @return this builder
     */
    public TileMatrixSetBuilder keywords(List<String> keywords) {
        this.keywords = keywords == null ? List.of() : List.copyOf(keywords);
        return this;
    }

    /**
     * Sets the optional OGC TMS {@code wellKnownScaleSet} URI (clause 7.1).
     *
     * @param wellKnownScaleSet the WKSS URI (may be null to clear)
     * @return this builder
     */
    public TileMatrixSetBuilder wellKnownScaleSet(URI wellKnownScaleSet) {
        this.wellKnownScaleSet = Optional.ofNullable(wellKnownScaleSet);
        return this;
    }

    /**
     * Sets the tile pyramid that defines the discrete grid structure.
     *
     * @param tilePyramid the tile pyramid
     * @return this builder
     */
    public TileMatrixSetBuilder tilePyramid(TilePyramid tilePyramid) {
        this.tilePyramid = tilePyramid;
        return this;
    }

    /**
     * Sets the coordinate reference system identifier.
     *
     * @param crsId the CRS identifier (e.g., "EPSG:4326", "EPSG:3857")
     * @return this builder
     */
    public TileMatrixSetBuilder crs(String crsId) {
        this.crsId = crsId;
        return this;
    }

    /**
     * Sets the tile dimensions in pixels.
     *
     * @param width the tile width in pixels
     * @param height the tile height in pixels
     * @return this builder
     */
    public TileMatrixSetBuilder tileSize(int width, int height) {
        this.tileWidth = width;
        this.tileHeight = height;
        return this;
    }

    /**
     * Sets the map space extent covered by this tile matrix set.
     *
     * @param extent the map space extent
     * @return this builder
     */
    public TileMatrixSetBuilder extent(BoundingBox2D extent) {
        this.extent = extent;
        return this;
    }

    /**
     * Sets the resolutions for each zoom level. The array length must match the number of zoom levels in the tile
     * pyramid.
     *
     * @param resolutions the resolution array (map units per pixel)
     * @return this builder
     */
    public TileMatrixSetBuilder resolutions(double... resolutions) {
        this.resolutions = resolutions.clone();
        return this;
    }

    /**
     * Sets the zoom level range by subsetting the tile pyramid and adjusting resolutions. This is a convenience method
     * for creating tile matrix sets with limited zoom ranges.
     *
     * @param minZoom the minimum zoom level (inclusive)
     * @param maxZoom the maximum zoom level (inclusive)
     * @return this builder
     * @throws IllegalStateException if tilePyramid, resolutions, or origins are not set
     */
    public TileMatrixSetBuilder zoomRange(int minZoom, int maxZoom) {
        if (tilePyramid == null) {
            throw new IllegalStateException("tilePyramid must be set before calling zoomRange()");
        }
        if (resolutions == null) {
            throw new IllegalStateException("resolutions must be set before calling zoomRange()");
        }

        // Validate zoom range is within current arrays
        if (minZoom < 0 || maxZoom >= resolutions.length) {
            throw new IllegalArgumentException("Zoom range [" + minZoom + ", " + maxZoom
                    + "] is outside available array bounds [0, " + (resolutions.length - 1) + "]");
        }

        // Validate all zoom levels in range have valid values
        for (int z = minZoom; z <= maxZoom; z++) {
            if (resolutions[z] <= 0) {
                throw new IllegalArgumentException("Invalid resolution at zoom level " + z + ": " + resolutions[z]);
            }
        }

        // Create new arrays that maintain direct indexing but sized to the max zoom needed: indices below
        // minZoom stay at the default 0.0, the minZoom..maxZoom slice is copied verbatim.
        double[] subsetResolutions = new double[maxZoom + 1];
        System.arraycopy(resolutions, minZoom, subsetResolutions, minZoom, maxZoom - minZoom + 1);

        this.resolutions = subsetResolutions;

        // Now subset the tile pyramid
        this.tilePyramid = tilePyramid.subset(minZoom, maxZoom);

        return this;
    }

    /**
     * Builds the StandardTileMatrixSet instance.
     *
     * @return the configured tile matrix set
     * @throws IllegalStateException if required properties are not set
     */
    public TileMatrixSet build() {
        if (identifier == null) {
            throw new IllegalStateException("identifier is required");
        }
        if (tilePyramid == null) {
            throw new IllegalStateException("tilePyramid is required");
        }
        if (crsId == null) {
            throw new IllegalStateException("crsId is required");
        }
        if (extent == null) {
            throw new IllegalStateException("extent is required");
        }
        if (resolutions == null) {
            throw new IllegalStateException("resolutions are required");
        }
        // Arrays must be large enough to directly index by zoom level
        int maxZoom = tilePyramid.maxZoomLevel();
        int minZoom = tilePyramid.minZoomLevel();

        if (resolutions.length <= maxZoom) {
            throw new IllegalStateException("resolutions array length (" + resolutions.length
                    + ") must be greater than max zoom level (" + maxZoom + ")");
        }

        // Validate that all required zoom levels have values
        for (int z = minZoom; z <= maxZoom; z++) {
            if (resolutions[z] <= 0) {
                throw new IllegalStateException("Invalid resolution at zoom level " + z + ": " + resolutions[z]);
            }
        }

        URI crsUri = supportedCRS != null ? supportedCRS : URI.create(crsId);
        return new StandardTileMatrixSet(
                identifier,
                tilePyramid,
                crsId,
                crsUri,
                tileWidth,
                tileHeight,
                extent,
                resolutions,
                title,
                abstractDescription,
                keywords,
                wellKnownScaleSet);
    }
}
