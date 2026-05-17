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
package io.tileverse.pmtiles.store;

import io.tileverse.cache.CacheManager;
import io.tileverse.geom.BoundingBox2D;
import io.tileverse.pmtiles.PMTilesHeader;
import io.tileverse.pmtiles.PMTilesReader;
import io.tileverse.pmtiles.UnsupportedTileTypeException;
import io.tileverse.rastertile.store.RasterTileStore;
import io.tileverse.tiling.matrix.Tile;
import io.tileverse.tiling.store.TileData;
import io.tileverse.vectortile.store.WebMercatorTransform;
import java.awt.image.BufferedImage;
import java.awt.image.RenderedImage;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.Optional;
import javax.imageio.ImageIO;
import org.jspecify.annotations.NullMarked;

/**
 * A {@link RasterTileStore} backed by a PMTiles archive holding image tiles (PNG/JPEG/WebP/AVIF).
 *
 * <p>Tile bytes are decoded straight from the underlying channel: {@link PMTilesReader#getTile(long,
 * io.tileverse.io.IOFunction)} hands an {@link InputStream} to {@link ImageIO#read(InputStream)}, so the encoded
 * payload never sits in a separately allocated {@code ByteBuffer}. No in-memory decode cache is layered on top because
 * the bytes are already in their final form and the {@code PMTilesReader}'s directory cache plus the
 * {@code RangeReader}'s own caching already minimize I/O.
 *
 * <p>Construction fails fast with {@link UnsupportedTileTypeException} if the archive does not contain a raster tile
 * type, so a misconfigured PMTiles URI never silently feeds image bytes to a vector consumer (or vice versa).
 */
@NullMarked
public class PMTilesRasterTileStore extends RasterTileStore {

    private final PMTilesReader reader;
    private final String mimeType;

    public PMTilesRasterTileStore(PMTilesReader reader) throws UnsupportedTileTypeException {
        super(PMTilesTileMatrixSet.fromWebMercator(Objects.requireNonNull(reader, "reader")));
        this.reader = reader;
        PMTilesHeader header = reader.getHeader();
        if (!header.isRasterTileType()) {
            throw new UnsupportedTileTypeException(
                    "PMTiles archive at %s holds tile type 0x%02X (%s); this store handles only raster tile formats (PNG/JPEG/WebP/AVIF)."
                            .formatted(reader.getSourceIdentifier(), header.tileType(), header.tileMimeType()));
        }
        this.mimeType = header.tileMimeType();
    }

    public PMTilesRasterTileStore cacheManager(CacheManager cacheManager) {
        reader.cacheManager(cacheManager);
        return this;
    }

    @Override
    public String mimeType() {
        return mimeType;
    }

    @Override
    public BoundingBox2D getExtent() {
        return WebMercatorTransform.latLonToWebMercator(reader.getHeader().geographicBoundingBox());
    }

    @Override
    public Optional<TileData<RenderedImage>> loadTile(Tile tile) {
        try {
            long tileId = reader.getTileId(tile.tileIndex());
            Optional<RenderedImage> img = reader.getTile(tileId, this::decode);
            return img.map(i -> new TileData<>(tile, i));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private RenderedImage decode(InputStream in) throws IOException {
        BufferedImage img = ImageIO.read(in);
        if (img == null) {
            throw new IOException("No ImageIO reader available for tile MIME type " + mimeType
                    + " (missing JDK ImageIO plugin for this format)");
        }
        return img;
    }
}
