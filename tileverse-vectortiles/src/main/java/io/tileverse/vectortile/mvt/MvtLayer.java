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
package io.tileverse.vectortile.mvt;

import io.tileverse.vectortile.model.Feature;
import io.tileverse.vectortile.model.Layer;
import java.util.Set;
import java.util.stream.Stream;
import org.locationtech.jts.geom.GeometryFactory;

class MvtLayer implements Layer {

    private final MvtTile tile;
    private final VectorTileProto.Tile.Layer layer;
    private GeometryDecoder decoder;

    MvtLayer(MvtTile tile, VectorTileProto.Tile.Layer layer) {
        this.tile = tile;
        this.layer = layer;
    }

    VectorTileProto.Tile.Layer layer() {
        return layer;
    }

    @Override
    public String getName() {
        return layer.getName();
    }

    @Override
    public int getExtent() {
        return layer.getExtent();
    }

    @Override
    public Set<String> getAttributeNames() {
        return Set.copyOf(layer.getKeysList());
    }

    @Override
    public int count() {
        return layer.getFeaturesCount();
    }

    /**
     * Returns a stream of features in this layer.
     * <p>
     * If the features don't have ids, they'll be assigned an id based on their position in the list
     * @return a sequential-only stream that reuses Feature instances for memory
     *         efficiency
     * @see MvtFeature#copy() for creating independent Feature copies
     */
    @Override
    public Stream<Feature> getFeatures() {
        return layer.getFeaturesList().stream().map(this::createFeature);
    }

    private MvtFeature createFeature(VectorTileProto.Tile.Feature feature) {
        return new MvtFeature(this, feature);
    }

    GeometryDecoder decoder() {
        if (this.decoder == null) {
            this.decoder = new GeometryDecoder(getGeometryFactory());
        }
        return decoder;
    }

    GeometryFactory getGeometryFactory() {
        return tile.getGeometryFactory();
    }
}
