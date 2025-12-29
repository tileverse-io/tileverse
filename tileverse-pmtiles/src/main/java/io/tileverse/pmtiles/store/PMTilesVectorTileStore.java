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
import io.tileverse.tiling.pyramid.TileIndex;
import io.tileverse.tiling.store.TileData;
import io.tileverse.vectortile.model.VectorTile;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.jspecify.annotations.NullMarked;
import org.locationtech.jts.geom.Envelope;

/**
 * A VectorTileStore implementation backed by a PMTiles file.
 * <p>
 * This store provides access to vector tiles stored in a PMTiles archive. It uses
 * a {@link PMTilesReader} to fetch the raw tile data and decodes it into
 * {@link VectorTile} objects.
 * <p>
 * Features:
 * <ul>
 *   <li>Efficient tile access using the PMTiles directory structure</li>
 *   <li>Caching of decoded {@link VectorTile} objects</li>
 *   <li>Integration with the {@link VectorTileStore} abstraction</li>
 * </ul>
 */
@NullMarked
public class PMTilesVectorTileStore extends VectorTileStore {

    private final PMTilesReader reader;

    /**
     * Short-lived (expireAfterAccess) {@link VectorTile} cache to account for consecutive single-layer requests.
     * <p>
     * Since {@link VectorTile} objects are immutable and relatively expensive to decode,
     * caching them improves performance when multiple layers are requested for the same tile.
     */
    private final VectorTileCache vectorTileCache;

    public PMTilesVectorTileStore(PMTilesReader reader) {
        super(PMTilesTileMatrixSet.fromWebMercator(reader));
        this.reader = Objects.requireNonNull(reader);
        this.vectorTileCache = new VectorTileCache(reader);
    }

    public PMTilesVectorTileStore cacheManager(CacheManager cacheManager) {
        reader.cacheManager(cacheManager);
        vectorTileCache.setCacheManager(cacheManager);
        return this;
    }

    /**
     * @throws UncheckedIOException if an IO exception happens when obtaining the {@link PMTilesMetadata} containing the list of {@link VectorLayer}
     */
    @Override
    public List<VectorLayer> getVectorLayersMetadata() {
        try {
            return getMetadata().vectorLayers();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public PMTilesMetadata getMetadata() throws IOException {
        return reader.getMetadata();
    }

    /**
     * {@inheritDoc}
     * @return the extent declared in the {@link PMTilesReader#getHeader() PMTiles header}, converted to WebMercator
     */
    @Override
    public BoundingBox2D getExtent() {
        BoundingBox2D geographicBoundingBox = reader.getHeader().geographicBoundingBox();
        return WebMercatorTransform.latLonToWebMercator(geographicBoundingBox);
    }

    /**
     * Delegates to {@link PMTilesReader#getTile(TileIndex, java.util.function.Function)} with a decoding function to parse the
     * {@link ByteBuffer}  blob as a {@link VectorTile}.
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
