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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.tileverse.jackson.databind.pmtiles.v3.PMTilesMetadata;
import io.tileverse.jackson.databind.tilejson.v3.VectorLayer;
import io.tileverse.pmtiles.PMTilesReader;
import io.tileverse.tiling.common.BoundingBox2D;
import io.tileverse.tiling.matrix.Tile;
import io.tileverse.tiling.pyramid.TileIndex;
import io.tileverse.tiling.store.TileData;
import io.tileverse.vectortile.model.VectorTile;
import io.tileverse.vectortile.mvt.VectorTileCodec;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.locationtech.jts.geom.Envelope;

public class PMTilesVectorTileStore extends VectorTileStore {

    private final PMTilesReader reader;

    private static final Duration expireAfterAccess = Duration.ofSeconds(10);

    /**
     * Short-lived (expireAfterAccess) {@link VectorTile} cache to account for consecutive single-layer requests
     */
    private final LoadingCache<TileIndex, Optional<VectorTile>> vectorTileCache;

    /**
     * Decodes {@code ByteBuffer} blobs from
     * {@link PMTilesReader#getTile(TileIndex, java.util.function.Function)
     * PMTilesReader} as {@link VectorTile}s
     *
     * @see #decodeVectorTile(ByteBuffer)
     */
    private final VectorTileCodec vectorTileDecoder = new VectorTileCodec();

    public PMTilesVectorTileStore(PMTilesReader reader) {
        super(PMTilesTileMatrixSet.fromWebMercator(reader));
        this.reader = Objects.requireNonNull(reader);
        this.vectorTileCache = Caffeine.newBuilder()
                .softValues()
                .expireAfterAccess(expireAfterAccess)
                .build(this::loadVectorTile);
    }

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
        try {
            Optional<VectorTile> decoded = getVectorTile(tile);
            Envelope bounds = toEnvelope(tile.extent());
            decoded = decoded.map(vt -> vt.withBoundingBox(bounds));

            return decoded.map(vt -> new TileData<>(tile, vt));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Optional<VectorTile> getVectorTile(Tile tile) throws IOException {
        TileIndex tileIndex = tile.tileIndex();
        return vectorTileCache.get(tileIndex);
    }

    private Optional<VectorTile> loadVectorTile(TileIndex tileIndex) throws IOException {
        return reader.getTile(tileIndex, this::decodeVectorTile);
    }

    private Envelope toEnvelope(BoundingBox2D extent) {
        return new Envelope(extent.minX(), extent.maxX(), extent.minY(), extent.maxY());
    }

    private VectorTile decodeVectorTile(ByteBuffer rawTile) {
        try {
            return vectorTileDecoder.decode(rawTile);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
