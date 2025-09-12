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

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import io.tileverse.vectortile.model.VectorTile;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.locationtech.jts.geom.Envelope;

/**
 * Creates an MvtTile from protobuf data with default GeometryFactory.
 * <p>
 * Coordinates are returned in their natural extent space (0 to extent-1) as stored in the MVT data.
 *
 * @param tileProto the MVT protobuf tile data
 */
record MvtTile(VectorTileProto.Tile tileProto, Envelope bounds) implements VectorTile {

    MvtTile(VectorTileProto.Tile tileProto) {
        this(tileProto, null);
    }

    @Override
    public Optional<Envelope> boundingBox() {
        return Optional.ofNullable(bounds);
    }

    @Override
    public VectorTile withBoundingBox(Envelope bounds) {
        return new MvtTile(tileProto, bounds);
    }

    @Override
    public Set<String> getLayerNames() {
        return layers().keySet();
    }

    @Override
    public List<Layer> getLayers() {
        return layers().values().stream().map(this::newLayer).toList();
    }

    @Override
    public Optional<Layer> getLayer(String name) {
        return tileProto.getLayersList().stream()
                .filter(l -> name.equals(l.getName()))
                .findFirst()
                .map(this::newLayer);
    }

    private Map<String, VectorTileProto.Tile.Layer> layers() {
        return tileProto.getLayersList().stream().collect(toMap(VectorTileProto.Tile.Layer::getName, identity()));
    }

    private Layer newLayer(VectorTileProto.Tile.Layer layer) {
        return new MvtLayer(this, layer);
    }

    public int getSerializedSize() {
        return tileProto.getSerializedSize();
    }

    @Override
    public String toString() {
        return "%s[%s]".formatted(getClass().getSimpleName(), getLayers());
    }
}
