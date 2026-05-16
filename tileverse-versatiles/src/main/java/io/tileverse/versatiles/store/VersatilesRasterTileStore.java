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
import io.tileverse.rastertile.store.RasterTileStore;
import io.tileverse.tiling.matrix.Tile;
import io.tileverse.tiling.store.TileData;
import io.tileverse.vectortile.store.WebMercatorTransform;
import io.tileverse.versatiles.TileFormat;
import io.tileverse.versatiles.UnsupportedTileFormatException;
import io.tileverse.versatiles.VersatilesReader;
import java.awt.image.RenderedImage;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import javax.imageio.IIOException;
import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.MemoryCacheImageInputStream;
import org.jspecify.annotations.NullMarked;

/**
 * A {@link RasterTileStore} over a Versatiles archive of image tiles: PNG, JPEG, WebP, or AVIF.
 *
 * <p>The store decodes tiles with {@link ImageIO}, from the stream handed over by
 * {@link VersatilesReader#getTile(io.tileverse.tiling.pyramid.TileIndex, io.tileverse.io.IOFunction)}. PNG and JPEG
 * decoders come with the JDK, and this module brings a WebP decoder. AVIF tiles need an AVIF plugin for
 * {@link ImageIO}: without one, the store refuses AVIF archives. The store borrows its {@link VersatilesReader}: the
 * caller closes the reader.
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
     */
    public VersatilesRasterTileStore(VersatilesReader reader) throws UnsupportedTileFormatException {
        super(VersatilesTileMatrixSet.fromWebMercator(requireNonNull(reader, "reader")));
        this.reader = reader;
        requireRasterTiles(reader);
    }

    private static void requireRasterTiles(VersatilesReader reader) throws UnsupportedTileFormatException {
        TileFormat format = reader.getTileFormat();
        if (!RASTER_FORMATS.contains(format)) {
            throw new UnsupportedTileFormatException(
                    "Versatiles archive at %s holds %s tiles; this store handles only raster tile formats (PNG/JPEG/WebP/AVIF)."
                            .formatted(reader.getSourceIdentifier(), format));
        }
        if (!hasImageReader(format)) {
            throw new UnsupportedTileFormatException(
                    "Versatiles archive at %s holds %s tiles, and no ImageIO decoder reads %s; add an ImageIO plugin for it."
                            .formatted(reader.getSourceIdentifier(), format, format.mimeType()));
        }
    }

    private static boolean hasImageReader(TileFormat format) {
        Iterator<ImageReader> readers = ImageIO.getImageReadersByMIMEType(format.mimeType());
        return readers.hasNext();
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
            Optional<RenderedImage> image = reader.getTile(tile.tileIndex(), this::decode);
            return image.map(decoded -> new TileData<>(tile, decoded));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Returns the tile format of the archive: PNG, JPEG, WebP, or AVIF. */
    public TileFormat getTileFormat() {
        return reader.getTileFormat();
    }

    /**
     * Decodes through an in-memory image stream, with a reader for the tile format of the archive.
     * {@link ImageIO#read(InputStream)} would copy the tile to a temporary file first, and fail on a read-only file
     * system. {@link ImageIO#read(ImageInputStream)} would leave the stream open when no reader recognizes the tile.
     */
    private RenderedImage decode(InputStream in) throws IOException {
        ImageReader imageReader = newImageReader();
        try (ImageInputStream imageStream = new MemoryCacheImageInputStream(in)) {
            imageReader.setInput(imageStream, true, true);
            return imageReader.read(0);
        } catch (RuntimeException e) {
            // some decoders fail on corrupt bytes with an unchecked exception
            throw new IIOException("Failed to decode a " + mimeType() + " tile", e);
        } finally {
            imageReader.dispose();
        }
    }

    private ImageReader newImageReader() throws IOException {
        Iterator<ImageReader> readers = ImageIO.getImageReadersByMIMEType(mimeType());
        if (!readers.hasNext()) {
            throw new IOException("No ImageIO reader available for tile MIME type " + mimeType()
                    + " (missing JDK ImageIO plugin for this format)");
        }
        return readers.next();
    }
}
