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
package io.tileverse.rastertile.store;

import io.tileverse.geom.BoundingBox2D;
import io.tileverse.tiling.matrix.TileMatrixSet;
import io.tileverse.tiling.store.AbstractTileStore;
import java.awt.image.RenderedImage;

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
}
