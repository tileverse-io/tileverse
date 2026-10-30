/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.rastertile.store;

import io.tileverse.geom.BoundingBox2D;
import io.tileverse.io.ByteBufferPool;
import io.tileverse.io.ByteBufferPool.PooledByteBuffer;
import io.tileverse.io.IOFunction;
import io.tileverse.tiling.matrix.TileMatrixSet;
import io.tileverse.tiling.store.AbstractTileStore;
import java.awt.Dimension;
import java.awt.image.RenderedImage;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import javax.imageio.IIOException;
import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;

/**
 * Abstract base class for {@link io.tileverse.tiling.store.TileStore} implementations that provide raster image tiles
 * as decoded {@link RenderedImage}s.
 *
 * <p>This is the raster counterpart of {@link io.tileverse.vectortile.store.VectorTileStore}, which exposes the decoded
 * {@code VectorTile} model rather than raw protobuf bytes. Same idea here: callers get pixels, not encoded bytes. The
 * JDK image decoder reads the encoded tile from a buffer borrowed from the {@link ByteBufferPool}, and the buffer goes
 * back to the pool after the decode.
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

    /** The smallest buffer borrowed for an encoded image. */
    private static final int MIN_BUFFER_BYTES = 16 * 1024;

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
        try {
            return withImageReader(encoded, mimeType, RasterTileStore::sizeOf);
        } catch (RuntimeException e) {
            // some decoders fail on corrupt bytes with an unchecked exception
            throw new IIOException("Failed to read the size of a " + mimeType + " image", e);
        }
    }

    private static Dimension sizeOf(ImageReader imageReader) throws IOException {
        return new Dimension(imageReader.getWidth(0), imageReader.getHeight(0));
    }

    /**
     * Decodes an encoded image with the {@link ImageIO} reader for {@code mimeType}. The image stream stays in memory:
     * {@link ImageIO#read(InputStream)} would copy the image to a temporary file, and fail on a read-only file system.
     * {@link ImageIO#read(ImageInputStream)} would leave the stream open when no reader recognizes the image.
     *
     * @throws IOException if no {@link ImageIO} reader decodes {@code mimeType}, or if the image cannot be decoded
     */
    protected static RenderedImage readImage(InputStream encoded, String mimeType) throws IOException {
        try {
            return withImageReader(encoded, mimeType, imageReader -> imageReader.read(0));
        } catch (RuntimeException e) {
            // some decoders fail on corrupt bytes with an unchecked exception
            throw new IIOException("Failed to decode a " + mimeType + " image", e);
        }
    }

    /**
     * Reads {@code encoded} into a pooled buffer, and applies {@code function} to the {@link ImageIO} reader for
     * {@code mimeType}, with the buffer as its input. The reader is disposed before the buffer goes back to the pool.
     */
    private static <T> T withImageReader(InputStream encoded, String mimeType, IOFunction<ImageReader, T> function)
            throws IOException {
        try (PooledByteBuffer image = readFully(encoded)) {
            ImageReader imageReader = newImageReader(mimeType);
            try (ImageInputStream imageStream = new ByteBufferImageInputStream(image.buffer())) {
                imageReader.setInput(imageStream, true, true);
                return function.apply(imageReader);
            } finally {
                imageReader.dispose();
            }
        }
    }

    /**
     * Reads {@code in} to its end into a buffer borrowed from the {@link ByteBufferPool}, and returns the buffer flipped.
     * The buffer starts one byte longer than the length reported by {@link InputStream#available()}, and at least 16 KiB
     * long. The stream of an uncompressed tile reports its exact length, and ends before the buffer fills. The buffer
     * doubles each time it fills.
     */
    private static PooledByteBuffer readFully(InputStream in) throws IOException {
        int initialLength = Math.max(in.available() + 1, MIN_BUFFER_BYTES);
        PooledByteBuffer pooled = ByteBufferPool.heapBuffer(initialLength);
        try {
            while (readInto(pooled.buffer(), in)) {
                if (!pooled.buffer().hasRemaining()) {
                    pooled = doubled(pooled);
                }
            }
            pooled.buffer().flip();
            return pooled;
        } catch (IOException | RuntimeException e) {
            pooled.close();
            throw e;
        }
    }

    /** Reads from {@code in} into the remaining space of {@code buffer}, and returns false at the end of {@code in}. */
    private static boolean readInto(ByteBuffer buffer, InputStream in) throws IOException {
        int read = in.read(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
        if (read == -1) {
            return false;
        }
        buffer.position(buffer.position() + read);
        return true;
    }

    /** Borrows a buffer twice as long as {@code full}, moves the bytes of {@code full} into it, and releases {@code full}. */
    private static PooledByteBuffer doubled(PooledByteBuffer full) {
        ByteBuffer bytes = full.buffer().flip();
        PooledByteBuffer larger = ByteBufferPool.heapBuffer(2 * bytes.capacity());
        larger.buffer().put(bytes);
        full.close();
        return larger;
    }

    private static ImageReader newImageReader(String mimeType) throws IOException {
        Iterator<ImageReader> readers = ImageIO.getImageReadersByMIMEType(mimeType);
        if (!readers.hasNext()) {
            throw new IOException("No ImageIO reader decodes " + mimeType);
        }
        return readers.next();
    }
}
