/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.versatiles.store;

import static java.util.Objects.requireNonNull;

import io.tileverse.cache.CacheManager;
import io.tileverse.geom.BoundingBox2D;
import io.tileverse.rastertile.store.RasterTileStore;
import io.tileverse.tiling.matrix.Tile;
import io.tileverse.tiling.matrix.TileMatrixSet;
import io.tileverse.tiling.pyramid.TileIndex;
import io.tileverse.tiling.store.TileData;
import io.tileverse.vectortile.store.WebMercatorTransform;
import io.tileverse.versatiles.TileFormat;
import io.tileverse.versatiles.UnsupportedTileFormatException;
import io.tileverse.versatiles.VersatilesHeader;
import io.tileverse.versatiles.VersatilesReader;
import java.awt.Dimension;
import java.awt.image.RenderedImage;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import javax.imageio.ImageIO;
import org.jspecify.annotations.NullMarked;

/**
 * A {@link RasterTileStore} over a Versatiles archive of image tiles: PNG, JPEG, WebP, or AVIF.
 *
 * <p>The store decodes tiles with {@link ImageIO}, from the stream handed over by
 * {@link VersatilesReader#getTile(io.tileverse.tiling.pyramid.TileIndex, io.tileverse.io.IOFunction)}. PNG and JPEG
 * decoders come with the JDK, and this module brings a WebP decoder. AVIF tiles need an AVIF plugin for
 * {@link ImageIO}: without one, the store refuses AVIF archives. The store borrows its {@link VersatilesReader}: the
 * caller closes the reader.
 *
 * <p>The Versatiles header does not record the tile size. Creating a store reads the first tile of the archive: an
 * archive of 512 x 512 tiles gets the {@code WebMercatorQuadx2} tile matrix set, and other archives get
 * {@code WebMercatorQuad}.
 */
@NullMarked
public class VersatilesRasterTileStore extends RasterTileStore {

    private static final Set<TileFormat> RASTER_FORMATS =
            EnumSet.of(TileFormat.PNG, TileFormat.JPEG, TileFormat.WEBP, TileFormat.AVIF);

    private final VersatilesReader reader;

    /**
     * Creates a store over the archive read by {@code reader}.
     *
     * @throws UnsupportedTileFormatException if the archive does not hold PNG, JPEG, WebP, or AVIF tiles, or if no
     *     {@link ImageIO} decoder reads its format
     * @throws IllegalArgumentException if the archive has no zoom level within the levels of {@code WebMercatorQuad}
     * @throws UncheckedIOException if the first tile of the archive cannot be read, or if its size cannot be decoded
     */
    public VersatilesRasterTileStore(VersatilesReader reader) throws UnsupportedTileFormatException {
        super(matrixSetOf(requireNonNull(reader, "reader")));
        this.reader = reader;
    }

    /**
     * Returns {@code WebMercatorQuadx2} when the first tile of the archive is 512 x 512 pixels, and
     * {@code WebMercatorQuad} otherwise.
     */
    private static TileMatrixSet matrixSetOf(VersatilesReader reader) throws UnsupportedTileFormatException {
        requireRasterTiles(reader);
        VersatilesHeader header = reader.getHeader();
        if (hasTilesOf512Pixels(reader)) {
            return VersatilesTileMatrixSet.fromWebMercator512(header);
        }
        return VersatilesTileMatrixSet.fromWebMercator(header);
    }

    private static void requireRasterTiles(VersatilesReader reader) throws UnsupportedTileFormatException {
        TileFormat format = reader.getTileFormat();
        if (!RASTER_FORMATS.contains(format)) {
            throw new UnsupportedTileFormatException(
                    "Versatiles archive at %s holds %s tiles; this store handles only raster tile formats (PNG/JPEG/WebP/AVIF)."
                            .formatted(reader.getSourceIdentifier(), format));
        }
        if (!hasImageReader(format.mimeType())) {
            throw new UnsupportedTileFormatException(
                    "Versatiles archive at %s holds %s tiles, and no ImageIO decoder reads %s; add an ImageIO plugin for it."
                            .formatted(reader.getSourceIdentifier(), format, format.mimeType()));
        }
    }

    private static boolean hasTilesOf512Pixels(VersatilesReader reader) {
        Optional<Dimension> size = firstTileSize(reader);
        return size.isPresent() && size.get().width == 512 && size.get().height == 512;
    }

    /** Reads the size of the first tile of the archive, or returns empty if the archive has no tiles. */
    private static Optional<Dimension> firstTileSize(VersatilesReader reader) {
        String tileMimeType = reader.getTileFormat().mimeType();
        try {
            Optional<TileIndex> firstTile = reader.firstTileIndex();
            if (firstTile.isEmpty()) {
                return Optional.empty();
            }
            return reader.getTile(firstTile.get(), tile -> readImageSize(tile, tileMimeType));
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to read the size of the first tile of " + reader.getSourceIdentifier(), e);
        }
    }

    /**
     * Keeps the tile indexes of the reader in {@code cacheManager} instead of {@link CacheManager#getDefault()}.
     *
     * @return this store
     */
    public VersatilesRasterTileStore cacheManager(CacheManager cacheManager) {
        reader.cacheManager(cacheManager);
        return this;
    }

    /** Returns the MIME type of the encoded tiles, such as {@code image/webp}. */
    @Override
    public String mimeType() {
        return reader.getTileFormat().mimeType();
    }

    /** Returns the bounding box of the archive in Web Mercator (EPSG:3857). */
    @Override
    public BoundingBox2D getExtent() {
        return WebMercatorTransform.latLonToWebMercator(reader.getHeader().geographicBoundingBox());
    }

    /**
     * Loads and decodes a tile, or returns empty if the archive has no tile at that position.
     *
     * @throws UncheckedIOException if the tile cannot be read, or if no {@link ImageIO} decoder reads its format
     */
    @Override
    public Optional<TileData<RenderedImage>> loadTile(Tile tile) {
        try {
            Optional<RenderedImage> image = reader.getTile(tile.tileIndex(), encoded -> readImage(encoded, mimeType()));
            return image.map(decoded -> new TileData<>(tile, decoded));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Returns the tile format of the archive: PNG, JPEG, WebP, or AVIF. */
    public TileFormat getTileFormat() {
        return reader.getTileFormat();
    }
}
