/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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
package io.tileverse.versatiles.store;

import static java.util.Objects.requireNonNull;

import io.tileverse.cache.CacheManager;
import io.tileverse.geom.BoundingBox2D;
import io.tileverse.jackson.databind.tilejson.v3.VectorLayer;
import io.tileverse.jackson.databind.versatiles.v2.VersatilesMetadata;
import io.tileverse.tiling.matrix.Tile;
import io.tileverse.tiling.store.TileData;
import io.tileverse.vectortile.model.VectorTile;
import io.tileverse.vectortile.mvt.VectorTileCodec;
import io.tileverse.vectortile.store.VectorTileCache;
import io.tileverse.vectortile.store.VectorTileStore;
import io.tileverse.vectortile.store.WebMercatorTransform;
import io.tileverse.versatiles.TileFormat;
import io.tileverse.versatiles.UnsupportedTileFormatException;
import io.tileverse.versatiles.VersatilesReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import org.jspecify.annotations.NullMarked;
import org.locationtech.jts.geom.Envelope;

/**
 * A {@link VectorTileStore} over a Versatiles archive of Mapbox Vector Tiles ({@link TileFormat#PBF}).
 *
 * <p>The store decodes tiles with {@link VectorTileCodec} and keeps the decoded tiles in a {@link VectorTileCache}. It
 * borrows its {@link VersatilesReader}: the caller closes the reader.
 */
@NullMarked
public class VersatilesVectorTileStore extends VectorTileStore {

    private final VersatilesReader reader;

    private final VectorTileCache vectorTileCache;

    /**
     * Creates a store over the archive read by {@code reader}.
     *
     * @throws UnsupportedTileFormatException if the archive does not hold Mapbox Vector Tiles
     * @throws IllegalArgumentException if the archive has no zoom level within the levels of {@code WebMercatorQuad}
     */
    public VersatilesVectorTileStore(VersatilesReader reader) throws UnsupportedTileFormatException {
        super(VersatilesTileMatrixSet.fromWebMercator(requireNonNull(reader, "reader")));
        this.reader = reader;
        requireMapboxVectorTiles(reader);
        VectorTileCodec decoder = new VectorTileCodec();
        this.vectorTileCache = new VectorTileCache(
                reader.getSourceIdentifier(), tileIndex -> reader.getTile(tileIndex, decoder::decode));
    }

    /**
     * GeoJSON and TopoJSON archives hold vector tiles too, but {@link VectorTileCodec} reads only Mapbox Vector Tiles.
     */
    private static void requireMapboxVectorTiles(VersatilesReader reader) throws UnsupportedTileFormatException {
        TileFormat format = reader.getTileFormat();
        if (format != TileFormat.PBF) {
            throw new UnsupportedTileFormatException(
                    "Versatiles archive at %s holds %s tiles; this store handles only Mapbox Vector Tiles (PBF)."
                            .formatted(reader.getSourceIdentifier(), format));
        }
    }

    /**
     * Keeps the decoded tiles of this store, and the tile indexes of its reader, in {@code cacheManager} instead of
     * {@link CacheManager#getDefault()}.
     *
     * @return this store
     */
    public VersatilesVectorTileStore cacheManager(CacheManager cacheManager) {
        reader.cacheManager(cacheManager);
        vectorTileCache.setCacheManager(cacheManager);
        return this;
    }

    /** Returns the {@code vector_layers} of the archive metadata. */
    @Override
    public List<VectorLayer> getVectorLayersMetadata() {
        return getMetadata().vectorLayers();
    }

    /** Returns the metadata of the archive. */
    public VersatilesMetadata getMetadata() {
        return reader.getMetadata();
    }

    /** Returns the bounding box of the archive in Web Mercator (EPSG:3857). */
    @Override
    public BoundingBox2D getExtent() {
        return WebMercatorTransform.latLonToWebMercator(reader.getHeader().geographicBoundingBox());
    }

    /**
     * Loads and decodes a tile, or returns empty if the archive has no tile at that position.
     *
     * @throws UncheckedIOException if the tile cannot be read or decoded
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
        return vectorTile.map(vt -> new TileData<>(tile, vt.withBoundingBox(bounds)));
    }

    /** Returns the tile format of the archive: always {@link TileFormat#PBF} for this store. */
    public TileFormat getTileFormat() {
        return reader.getTileFormat();
    }

    private Envelope toEnvelope(BoundingBox2D extent) {
        return new Envelope(extent.minX(), extent.maxX(), extent.minY(), extent.maxY());
    }
}
