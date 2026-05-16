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
package io.tileverse.pmtiles.store;

import io.tileverse.cache.CacheManager;
import io.tileverse.jackson.databind.pmtiles.v3.PMTilesMetadata;
import io.tileverse.jackson.databind.tilejson.v3.VectorLayer;
import io.tileverse.pmtiles.PMTilesReader;
import io.tileverse.tiling.common.BoundingBox2D;
import io.tileverse.tiling.matrix.Tile;
import io.tileverse.tiling.store.TileData;
import io.tileverse.vectortile.model.VectorTile;
import io.tileverse.vectortile.mvt.VectorTileCodec;
import io.tileverse.vectortile.store.VectorTileCache;
import io.tileverse.vectortile.store.VectorTileStore;
import io.tileverse.vectortile.store.WebMercatorTransform;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.jspecify.annotations.NullMarked;
import org.locationtech.jts.geom.Envelope;

/**
 * A {@link VectorTileStore} implementation backed by a PMTiles archive.
 *
 * <p>This store provides access to vector tiles stored in a PMTiles file. It uses a {@link PMTilesReader} to fetch the
 * raw tile data and decodes it into {@link VectorTile} objects.
 *
 * <p>The store manages a {@link VectorTileCache} to optimize performance when multiple layers or features are requested
 * from the same tile.
 */
@NullMarked
public class PMTilesVectorTileStore extends VectorTileStore {

    private final PMTilesReader reader;

    /** Cache for decoded {@link VectorTile} objects to avoid redundant decoding overhead. */
    private final VectorTileCache vectorTileCache;

    /**
     * Creates a new PMTiles vector tile store.
     *
     * @param reader the reader for the underlying PMTiles archive
     */
    public PMTilesVectorTileStore(PMTilesReader reader) {
        super(PMTilesTileMatrixSet.fromWebMercator(reader));
        this.reader = Objects.requireNonNull(reader);
        VectorTileCodec decoder = new VectorTileCodec();
        this.vectorTileCache = new VectorTileCache(
                reader.getSourceIdentifier(),
                tileIndex -> reader.getTile(reader.getTileId(tileIndex), decoder::decode));
    }

    /**
     * Configures a {@link CacheManager} for both the PMTiles reader and the vector tile cache.
     *
     * @param cacheManager the cache manager to use
     * @return this store instance for method chaining
     */
    public PMTilesVectorTileStore cacheManager(CacheManager cacheManager) {
        reader.cacheManager(cacheManager);
        vectorTileCache.setCacheManager(cacheManager);
        return this;
    }

    /**
     * Returns the metadata for all vector layers available in the PMTiles archive.
     *
     * @return a list of vector layer metadata
     */
    @Override
    public List<VectorLayer> getVectorLayersMetadata() {
        return getMetadata().vectorLayers();
    }

    /**
     * Returns the full PMTiles metadata as parsed at reader construction time.
     *
     * @return the archive metadata
     */
    public PMTilesMetadata getMetadata() {
        return reader.getMetadata();
    }

    /**
     * Returns the geographic extent of the data in the PMTiles archive, converted to Web Mercator.
     *
     * @return the extent in Web Mercator (EPSG:3857)
     */
    @Override
    public BoundingBox2D getExtent() {
        BoundingBox2D geographicBoundingBox = reader.getHeader().geographicBoundingBox();
        return WebMercatorTransform.latLonToWebMercator(geographicBoundingBox);
    }

    /**
     * Loads a single vector tile from the archive.
     *
     * <p>This method first checks the cache and, if not found, uses the PMTiles reader to fetch and decode the tile
     * data.
     *
     * @param tile the tile metadata
     * @return an optional containing the tile data, or empty if not found
     * @throws UncheckedIOException if an error occurs during loading
     */
    @Override
    public Optional<TileData<VectorTile>> loadTile(Tile tile) {
        Optional<VectorTile> vectorTile;
        try {
            vectorTile = vectorTileCache.getVectorTile(tile.tileIndex());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Envelope bounds = toEnvelope(tile.extent());
        vectorTile = vectorTile.map(vt -> vt.withBoundingBox(bounds));

        return vectorTile.map(vt -> new TileData<>(tile, vt));
    }

    private Envelope toEnvelope(BoundingBox2D extent) {
        return new Envelope(extent.minX(), extent.maxX(), extent.minY(), extent.maxY());
    }
}
