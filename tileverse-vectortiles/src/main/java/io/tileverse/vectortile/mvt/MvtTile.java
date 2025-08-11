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

import io.tileverse.vectortile.model.Layer;
import io.tileverse.vectortile.model.Tile;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.impl.PackedCoordinateSequenceFactory;

final class MvtTile implements Tile {

    static final GeometryFactory DEFAULT_GEOMETRY_FACTORY = new GeometryFactory(new PackedCoordinateSequenceFactory());

    private final GeometryFactory geometryFactory;

    final VectorTileProto.Tile tile;
    private Map<String, VectorTileProto.Tile.Layer> layers;

    /**
     * Creates an MvtTile from protobuf data with default GeometryFactory.
     * <p>
     * Coordinates are returned in their natural extent space (0 to extent-1) as stored in the MVT data.
     *
     * @param tile the MVT protobuf tile data
     */
    public MvtTile(VectorTileProto.Tile tile) {
        this(tile, null);
    }

    /**
     * Creates an MvtTile from protobuf data with a specific GeometryFactory.
     * <p>
     * Coordinates are returned in their natural extent space (0 to extent-1) as stored in the MVT data.
     *
     * @param tile the MVT protobuf tile data
     * @param geometryFactory the GeometryFactory to use for creating geometries, or null for default
     */
    public MvtTile(VectorTileProto.Tile tile, GeometryFactory geometryFactory) {
        this.tile = tile;
        this.geometryFactory = geometryFactory == null ? DEFAULT_GEOMETRY_FACTORY : geometryFactory;
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
        return Optional.ofNullable(layers().get(name)).map(this::newLayer);
    }

    private Map<String, VectorTileProto.Tile.Layer> layers() {
        if (layers == null) {
            layers = tile.getLayersList().stream().collect(toMap(VectorTileProto.Tile.Layer::getName, identity()));
        }
        return layers;
    }

    private Layer newLayer(VectorTileProto.Tile.Layer layer) {
        return new MvtLayer(this, layer);
    }

    GeometryFactory getGeometryFactory() {
        return geometryFactory;
    }

    public int getSerializedSize() {
        return tile.getSerializedSize();
    }
}
