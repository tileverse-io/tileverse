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

import static java.util.Objects.requireNonNull;

import io.tileverse.tiling.common.BoundingBox2D;
import io.tileverse.tiling.matrix.TileMatrixSet;
import io.tileverse.tiling.store.TileStore;
import io.tileverse.tiling.store.TileStore.Strategy;
import io.tileverse.vectortile.model.VectorTile.Layer;
import io.tileverse.vectortile.model.VectorTile.Layer.Feature;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

public class VectorTilesQuery {

    private List<String> layerIds;
    private GeometryFactory geometryFactory;
    private boolean transformToCrs;
    private Predicate<Feature> filter;
    private UnaryOperator<Geometry> geometryOperation;
    private Integer zoomLevel;
    private List<BoundingBox2D> queryExtent = List.of();
    private Double resolution;
    private Strategy strategy = Strategy.SPEED;

    public VectorTilesQuery layers(String... layerIds) {
        this.layerIds = Stream.of(layerIds).filter(Objects::nonNull).toList();
        if (this.layerIds.isEmpty()) {
            throw new IllegalArgumentException(
                    "layerIds can't be empty. To retrieve all layers, don't call layers(String... layerIds)");
        }
        return this;
    }

    public Optional<List<String>> layers() {
        return Optional.ofNullable(this.layerIds);
    }

    /**
     * The zoom level to query. If set, it takes precedence over {@link #resolution()}
     * @param zoomLevel
     * @return
     */
    public VectorTilesQuery zoomLevel(Integer zoomLevel) {
        this.zoomLevel = zoomLevel;
        return this;
    }

    public OptionalInt zoomLevel() {
        return this.zoomLevel == null ? OptionalInt.empty() : OptionalInt.of(this.zoomLevel);
    }

    /**
     * The query resolution. If {@link #zoomLevel} is set, it takes precedence over resolution/strategy
     * @param determineResolution
     * @param strategy
     * @return
     */
    public VectorTilesQuery resolution(double resolution) {
        this.resolution = resolution;
        return this;
    }

    public OptionalDouble resolution() {
        return this.resolution == null ? OptionalDouble.empty() : OptionalDouble.of(this.resolution);
    }

    public VectorTilesQuery strategy(TileStore.Strategy strategy) {
        this.strategy = requireNonNull(strategy);
        return this;
    }

    public TileStore.Strategy strategy() {
        return this.strategy;
    }

    public VectorTilesQuery extent(BoundingBox2D... queryExtent) {
        return extent(Arrays.asList(queryExtent));
    }

    public VectorTilesQuery extent(List<BoundingBox2D> queryExtent) {
        this.queryExtent = List.copyOf(queryExtent);
        return this;
    }

    public List<BoundingBox2D> extent() {
        return this.queryExtent;
    }

    public VectorTilesQuery geometryFactory(GeometryFactory geometryFactory) {
        this.geometryFactory = geometryFactory;
        return this;
    }

    public Optional<GeometryFactory> geometryFactory() {
        return Optional.ofNullable(this.geometryFactory);
    }

    public VectorTilesQuery transformToCrs(boolean transformToCrs) {
        this.transformToCrs = transformToCrs;
        return this;
    }

    public boolean transformToCrs() {
        return this.transformToCrs;
    }

    public VectorTilesQuery filter(Predicate<Feature> filter) {
        this.filter = filter;
        return this;
    }

    public Optional<Predicate<Feature>> filter() {
        return Optional.ofNullable(this.filter);
    }

    /**
     *
     * @param geometryOperation operation to apply to geometries during {@link Layer#getFeatures(Predicate, GeometryDecoder)}
     *                          {@link Feature#getGeometry(GeometryFactory)}. If
     *                          {@code transformToCrs} is {@code true} this operation is
     *                          concatenated to the tile space to
     *                          {@link TileMatrixSet#crsId() CRS} transformation.
     */
    public VectorTilesQuery geometryTransformation(UnaryOperator<Geometry> geometryOperation) {
        this.geometryOperation = geometryOperation;
        return this;
    }

    public Optional<UnaryOperator<Geometry>> geometryTransformation() {
        return Optional.ofNullable(this.geometryOperation);
    }
}
