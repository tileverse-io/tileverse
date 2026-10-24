/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.rastertile.store;

import io.tileverse.geom.BoundingBox2D;
import io.tileverse.tiling.matrix.TileMatrixSet;
import io.tileverse.tiling.store.AbstractTileStore;
import java.awt.Dimension;
import java.awt.image.RenderedImage;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import javax.imageio.IIOException;
import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.MemoryCacheImageInputStream;

/**
 * Abstract base class for {@link io.tileverse.tiling.store.TileStore} implementations that provide raster image tiles
 * as decoded {@link RenderedImage}s.
 *
 * <p>This is the raster counterpart of {@link io.tileverse.vectortile.store.VectorTileStore}, which exposes the decoded
 * {@code VectorTile} model rather than raw protobuf bytes. Same idea here: callers get pixels, not encoded bytes. The
 * implementation streams the tile body straight from the underlying channel into the JDK image decoder, so the encoded
 * payload never sits in a separately allocated {@code ByteBuffer}.
 *
 * <p>The publicly visible payload type is the {@link RenderedImage} interface (not the concrete {@code BufferedImage})
 * so callers can flow results directly into APIs like {@code GridCoverageFactory.create(String, RenderedImage,
 * ReferencedEnvelope)} and so the store can later swap in a tile-aware or lazy decoder without breaking the contract.
 *
 * <p>Passthrough use cases that need raw encoded bytes (e.g. an HTTP proxy serving WebP straight from S3) should bypass
 * this store and call the underlying reader's streaming {@code getTile(tileId, IOFunction<InputStream, D>)} overload
 * with a {@code ByteBuffer}- or {@code byte[]}-producing mapper.
 */
public abstract class RasterTileStore extends AbstractTileStore<RenderedImage> {

    protected RasterTileStore(TileMatrixSet matrixSet) {
        super(matrixSet);
    }

    /**
     * Returns the IANA media type of the encoded tile bytes underlying this store (for example {@code "image/png"},
     * {@code "image/jpeg"}, {@code "image/webp"}, {@code "image/avif"}).
     *
     * <p>All tiles in a single archive use the same encoding, so this is a per-store property. The store always returns
     * decoded {@link RenderedImage}s; the MIME type is exposed for callers that need to set HTTP {@code Content-Type}
     * headers, choose a writer, or pick an alternative decoder pipeline.
     *
     * @return the MIME type string, never null
     */
    public abstract String mimeType();

    /**
     * Returns the overall spatial extent of the data in this store.
     *
     * @return the extent in the {@link TileMatrixSet#crsId() TileMatrixSet CRS}
     */
    public abstract BoundingBox2D getExtent();

    /** Whether an {@link ImageIO} reader decodes images of {@code mimeType}. */
    protected static boolean hasImageReader(String mimeType) {
        Iterator<ImageReader> readers = ImageIO.getImageReadersByMIMEType(mimeType);
        return readers.hasNext();
    }

    /**
     * Reads the width and height of an encoded image from its header, without decoding its pixels. A store can learn
     * its tile size this way when the archive does not record it. The image stream stays in memory:
     * {@link ImageIO#createImageInputStream} would copy the image to a temporary file.
     *
     * @throws IOException if no {@link ImageIO} reader decodes {@code mimeType}, or if the header cannot be read
     */
    protected static Dimension readImageSize(InputStream encoded, String mimeType) throws IOException {
        ImageReader imageReader = newImageReader(mimeType);
        try (ImageInputStream imageStream = new MemoryCacheImageInputStream(encoded)) {
            imageReader.setInput(imageStream, true, true);
            return new Dimension(imageReader.getWidth(0), imageReader.getHeight(0));
        } catch (RuntimeException e) {
            // some decoders fail on corrupt bytes with an unchecked exception
            throw new IIOException("Failed to read the size of a " + mimeType + " image", e);
        } finally {
            imageReader.dispose();
        }
    }

    private static ImageReader newImageReader(String mimeType) throws IOException {
        Iterator<ImageReader> readers = ImageIO.getImageReadersByMIMEType(mimeType);
        if (!readers.hasNext()) {
            throw new IOException("No ImageIO reader decodes " + mimeType);
        }
        return readers.next();
    }
}
