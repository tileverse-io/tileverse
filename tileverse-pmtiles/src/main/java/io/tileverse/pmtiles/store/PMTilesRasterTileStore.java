/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.pmtiles.store;

import io.tileverse.cache.CacheManager;
import io.tileverse.geom.BoundingBox2D;
import io.tileverse.pmtiles.PMTilesDirectory;
import io.tileverse.pmtiles.PMTilesEntry;
import io.tileverse.pmtiles.PMTilesHeader;
import io.tileverse.pmtiles.PMTilesReader;
import io.tileverse.pmtiles.UnsupportedTileTypeException;
import io.tileverse.rastertile.store.RasterTileStore;
import io.tileverse.tiling.matrix.Tile;
import io.tileverse.tiling.matrix.TileMatrixSet;
import io.tileverse.tiling.store.TileData;
import io.tileverse.vectortile.store.WebMercatorTransform;
import java.awt.Dimension;
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
 * type, or if no {@link ImageIO} decoder reads its tiles. A misconfigured PMTiles URI never feeds image bytes to a
 * vector consumer, or vector bytes to an image decoder.
 *
 * <p>The PMTiles header does not record the tile size. Construction reads the first tile of the archive: an archive of
 * 512 x 512 tiles gets the {@code WebMercatorQuadx2} tile matrix set, and other archives get {@code WebMercatorQuad}.
 */
@NullMarked
public class PMTilesRasterTileStore extends RasterTileStore {

    private final PMTilesReader reader;
    private final String mimeType;

    /**
     * Creates a store over the archive read by {@code reader}.
     *
     * @throws UnsupportedTileTypeException if the archive does not hold PNG, JPEG, WebP, or AVIF tiles, or if no
     *     {@link ImageIO} decoder reads its tiles
     * @throws UncheckedIOException if the first tile of the archive cannot be read, or if its size cannot be decoded
     */
    public PMTilesRasterTileStore(PMTilesReader reader) throws UnsupportedTileTypeException {
        super(matrixSetOf(Objects.requireNonNull(reader, "reader")));
        this.reader = reader;
        this.mimeType = reader.getHeader().tileMimeType();
    }

    /**
     * Returns {@code WebMercatorQuadx2} when the first tile of the archive is 512 x 512 pixels, and
     * {@code WebMercatorQuad} otherwise.
     */
    private static TileMatrixSet matrixSetOf(PMTilesReader reader) throws UnsupportedTileTypeException {
        requireRasterTiles(reader);
        PMTilesHeader header = reader.getHeader();
        if (hasTilesOf512Pixels(reader)) {
            return PMTilesTileMatrixSet.fromWebMercator512(header);
        }
        return PMTilesTileMatrixSet.fromWebMercator(header);
    }

    private static void requireRasterTiles(PMTilesReader reader) throws UnsupportedTileTypeException {
        PMTilesHeader header = reader.getHeader();
        if (!header.isRasterTileType()) {
            throw new UnsupportedTileTypeException(
                    "PMTiles archive at %s holds tile type 0x%02X (%s); this store handles only raster tile formats (PNG/JPEG/WebP/AVIF)."
                            .formatted(reader.getSourceIdentifier(), header.tileType(), header.tileMimeType()));
        }
        if (!hasImageReader(header.tileMimeType())) {
            throw new UnsupportedTileTypeException(
                    "PMTiles archive at %s holds %s tiles, and no ImageIO decoder reads them; add an ImageIO plugin for it."
                            .formatted(reader.getSourceIdentifier(), header.tileMimeType()));
        }
    }

    private static boolean hasTilesOf512Pixels(PMTilesReader reader) {
        Optional<Dimension> size = firstTileSize(reader);
        return size.isPresent() && size.get().width == 512 && size.get().height == 512;
    }

    private static Optional<Dimension> firstTileSize(PMTilesReader reader) {
        long firstTileId = firstTileId(reader, reader.getRootDirectory());
        String tileMimeType = reader.getHeader().tileMimeType();
        try {
            return reader.getTile(firstTileId, tile -> readImageSize(tile, tileMimeType));
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to read the size of the first tile of " + reader.getSourceIdentifier(), e);
        }
    }

    /** Returns the id of the first tile listed by {@code directory}, or by its first leaf directory. */
    private static long firstTileId(PMTilesReader reader, PMTilesDirectory directory) {
        // the specification requires at least one entry in a directory, and the reader refuses an empty one
        PMTilesEntry first = directory.firstEntry();
        if (first.isTile()) {
            return first.tileId();
        }
        return firstTileId(reader, reader.getDirectory(first));
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
