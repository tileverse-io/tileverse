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

import io.tileverse.tiling.common.BoundingBox2D;
import io.tileverse.tiling.store.TileData;
import io.tileverse.vectortile.model.GeometryReader;
import io.tileverse.vectortile.model.VectorTile;
import io.tileverse.vectortile.mvt.VectorTileCodec;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.util.AffineTransformation;

record VectorTileLayerReader(VectorTile.Layer layer, TileData<VectorTile> tileData, VectorTilesQuery query) {

    public Stream<VectorTile.Layer.Feature> getFeatures() {
        if (layer.count() == 0) {
            return Stream.empty();
        }

        Predicate<VectorTile.Layer.Feature> filter = query.filter().orElse(f -> true);
        GeometryReader decoder = buildTileGeometryDecoder();
        Stream<VectorTile.Layer.Feature> features = layer.getFeatures(filter, decoder);
        return features;
    }

    public GeometryReader buildTileGeometryDecoder() {
        GeometryFactory geometryFactory = query.geometryFactory().orElse(null);
        UnaryOperator<Geometry> transform = buildGeometryTransform();

        GeometryReader geometryReader = VectorTileCodec.newGeometryReader();
        if (geometryFactory != null) {
            geometryReader = geometryReader.withGeometryFactory(geometryFactory);
        }
        if (transform != null) {
            geometryReader = geometryReader.withGeometryTransformation(transform);
        }

        return geometryReader;
    }

    private UnaryOperator<Geometry> buildGeometryTransform() {
        UnaryOperator<Geometry> toCrs = transformToCrs();
        return query.geometryTransformation()
                .map(post -> GeometryReader.concat(toCrs, post))
                .orElse(toCrs);
    }

    private UnaryOperator<Geometry> transformToCrs() {
        if (!query.transformToCrs()) {
            return UnaryOperator.identity();
        }

        BoundingBox2D nativeCrsExtent = tileData.tile().extent();
        int tileExtent = layer.getExtent(); // e.g. 4096x4096

        AffineTransformation at = createTileToCrsTransformation(tileExtent, nativeCrsExtent);

        return GeometryReader.toFunction(at);
    }

    /**
     * Creates an AffineTransformation that maps tile coordinates to CRS coordinates.
     *
     * @param tileExtent The tile extent in tile coordinate system (typically 0,0 to 4096,4096)
     * @param tileEnvelope The corresponding envelope in the target CRS coordinate system
     * @return AffineTransformation for converting tile coords to CRS coords
     */
    public static AffineTransformation createTileToCrsTransformation(int tileExtent, BoundingBox2D tileEnvelope) {

        // Calculate scale factors
        double scaleX = tileEnvelope.width() / tileExtent;
        // CRS's origin is lower-left, tile space is upper-left
        double scaleY = -1 * (tileEnvelope.height() / tileExtent);

        // Calculate translation offsets
        // We need to map tile's (0,0) upper-left to CRS's upper-left
        double translateX = tileEnvelope.minX();
        double translateY = tileEnvelope.maxY(); // Tile (0,0) maps to CRS upper-left

        return AffineTransformation.scaleInstance(scaleX, scaleY).translate(translateX, translateY);
    }
}
