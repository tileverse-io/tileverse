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
package io.tileverse.vectortile.store;

import static java.util.Objects.requireNonNull;

import io.tileverse.tiling.common.BoundingBox2D;
import io.tileverse.tiling.store.TileStore;
import io.tileverse.tiling.store.TileStore.Strategy;
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

/**
 * A fluent API for defining a query to retrieve features from a {@link VectorTileStore}.
 *
 * <p>This class allows specifying filters for layers, zoom levels, resolution, spatial extent, and geometry
 * transformations.
 */
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

    /**
     * Filters the query to the specified layer identifiers.
     *
     * @param layerIds the identifiers of the layers to include
     * @return this query instance for method chaining
     * @throws IllegalArgumentException if layerIds is empty
     */
    public VectorTilesQuery layers(String... layerIds) {
        this.layerIds = Stream.of(layerIds).filter(Objects::nonNull).toList();
        if (this.layerIds.isEmpty()) {
            throw new IllegalArgumentException(
                    "layerIds can't be empty. To retrieve all layers, don't call layers(String... layerIds)");
        }
        return this;
    }

    /**
     * Returns the optional list of layer identifiers to filter by.
     *
     * @return an optional containing the list of layer identifiers
     */
    public Optional<List<String>> layers() {
        return Optional.ofNullable(this.layerIds);
    }

    /**
     * Sets the explicit zoom level to query.
     *
     * <p>If set, this takes precedence over {@link #resolution(double)}.
     *
     * @param zoomLevel the zoom level
     * @return this query instance for method chaining
     */
    public VectorTilesQuery zoomLevel(Integer zoomLevel) {
        this.zoomLevel = zoomLevel;
        return this;
    }

    /**
     * Returns the optional explicit zoom level.
     *
     * @return an OptionalInt containing the zoom level
     */
    public OptionalInt zoomLevel() {
        return this.zoomLevel == null ? OptionalInt.empty() : OptionalInt.of(this.zoomLevel);
    }

    /**
     * Sets the target resolution for the query.
     *
     * <p>The most appropriate zoom level will be selected based on this resolution and the
     * {@link #strategy(TileStore.Strategy)}. If {@link #zoomLevel(Integer)} is set, it takes precedence.
     *
     * @param resolution the target resolution (units per pixel)
     * @return this query instance for method chaining
     */
    public VectorTilesQuery resolution(double resolution) {
        this.resolution = resolution;
        return this;
    }

    /**
     * Returns the optional target resolution.
     *
     * @return an OptionalDouble containing the resolution
     */
    public OptionalDouble resolution() {
        return this.resolution == null ? OptionalDouble.empty() : OptionalDouble.of(this.resolution);
    }

    /**
     * Sets the strategy used to pick a zoom level.
     *
     * <p>The strategy is consulted in two situations: when a {@link #resolution(double) resolution} is set (to break
     * ties between candidate zoom levels), and when neither a zoom level nor a resolution is set ({@link Strategy#SPEED
     * SPEED} picks the store's minimum zoom level, {@link Strategy#QUALITY QUALITY} picks the maximum {@code minZoom}
     * reported across layers).
     *
     * @param strategy the selection strategy
     * @return this query instance for method chaining
     */
    public VectorTilesQuery strategy(TileStore.Strategy strategy) {
        this.strategy = requireNonNull(strategy);
        return this;
    }

    /**
     * Returns the selection strategy.
     *
     * @return the {@link Strategy}
     */
    public TileStore.Strategy strategy() {
        return this.strategy;
    }

    /**
     * Sets the spatial extent(s) for the query.
     *
     * @param queryExtent the bounding boxes to query
     * @return this query instance for method chaining
     */
    public VectorTilesQuery extent(BoundingBox2D... queryExtent) {
        return extent(Arrays.asList(queryExtent));
    }

    /**
     * Sets the spatial extent(s) for the query.
     *
     * @param queryExtent the list of bounding boxes to query
     * @return this query instance for method chaining
     */
    public VectorTilesQuery extent(List<BoundingBox2D> queryExtent) {
        this.queryExtent = List.copyOf(queryExtent);
        return this;
    }

    /**
     * Returns the list of bounding boxes that define the query extent.
     *
     * @return the query extent
     */
    public List<BoundingBox2D> extent() {
        return this.queryExtent;
    }

    /**
     * Sets the JTS {@link GeometryFactory} to use when decoding feature geometries.
     *
     * @param geometryFactory the geometry factory
     * @return this query instance for method chaining
     */
    public VectorTilesQuery geometryFactory(GeometryFactory geometryFactory) {
        this.geometryFactory = geometryFactory;
        return this;
    }

    /**
     * Returns the optional geometry factory.
     *
     * @return an optional containing the geometry factory
     */
    public Optional<GeometryFactory> geometryFactory() {
        return Optional.ofNullable(this.geometryFactory);
    }

    /**
     * Specifies whether geometries should be automatically transformed from tile coordinates to the store's CRS.
     *
     * @param transformToCrs {@code true} to transform to CRS, {@code false} to keep in tile coordinates
     * @return this query instance for method chaining
     */
    public VectorTilesQuery transformToCrs(boolean transformToCrs) {
        this.transformToCrs = transformToCrs;
        return this;
    }

    /**
     * Returns whether automatic CRS transformation is enabled.
     *
     * @return {@code true} if enabled, {@code false} otherwise
     */
    public boolean transformToCrs() {
        return this.transformToCrs;
    }

    /**
     * Sets a predicate to filter features.
     *
     * @param filter the predicate to apply to each feature
     * @return this query instance for method chaining
     */
    public VectorTilesQuery filter(Predicate<Feature> filter) {
        this.filter = filter;
        return this;
    }

    /**
     * Returns the optional feature filter.
     *
     * @return an optional containing the filter predicate
     */
    public Optional<Predicate<Feature>> filter() {
        return Optional.ofNullable(this.filter);
    }

    /**
     * Sets a geometry transformation to apply to features during reading.
     *
     * <p>When {@link #transformToCrs(boolean)} is {@code true}, this operation runs after the tile-to-CRS
     * transformation, so it receives geometries in the store's CRS. Otherwise it runs against the raw tile-space
     * geometries.
     *
     * @param geometryOperation operation to apply to geometries
     * @return this query instance for method chaining
     */
    public VectorTilesQuery geometryTransformation(UnaryOperator<Geometry> geometryOperation) {
        this.geometryOperation = geometryOperation;
        return this;
    }

    /**
     * Returns the optional geometry transformation.
     *
     * @return an optional containing the geometry operation
     */
    public Optional<UnaryOperator<Geometry>> geometryTransformation() {
        return Optional.ofNullable(this.geometryOperation);
    }
}
