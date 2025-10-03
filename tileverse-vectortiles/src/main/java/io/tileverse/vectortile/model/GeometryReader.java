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
package io.tileverse.vectortile.model;

import io.tileverse.vectortile.mvt.VectorTileCodec;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.locationtech.jts.geom.CoordinateSequenceFilter;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

/**
 * Interface for reading and customizing geometry decoding from vector tile features.
 * <p>
 * GeometryReader provides a flexible way to control how geometries are decoded from vector tile data,
 * allowing customization of the JTS GeometryFactory and application of coordinate transformations.
 *
 * <h2>Key Features</h2>
 * <ul>
 * <li><strong>Custom GeometryFactory</strong> - Control coordinate sequence implementation and precision</li>
 * <li><strong>Coordinate Transformations</strong> - Apply scaling, translation, projection changes</li>
 * <li><strong>Chainable Configuration</strong> - Fluent API for combining multiple customizations</li>
 * <li><strong>Transformation Utilities</strong> - Helper methods for common transformation patterns</li>
 * </ul>
 *
 * <h2>Usage Examples</h2>
 * <pre>{@code
 * // Basic usage with custom GeometryFactory
 * GeometryFactory customFactory = new GeometryFactory(new CoordinateArraySequenceFactory());
 * GeometryReader reader = VectorTileCodec.newGeometryReader()
 *     .withGeometryFactory(customFactory);
 *
 * // Apply coordinate transformation from tile extent to world coordinates
 * GeometryReader transformingReader = VectorTileCodec.newGeometryReader()
 *     .withGeometryTransformation(geometry -> {
 *         // Scale from tile extent (0-4095) to world bounds
 *         AffineTransformation transform = new AffineTransformation();
 *         transform.scale(worldWidth / 4096.0, worldHeight / 4096.0);
 *         transform.translate(worldMinX, worldMinY);
 *         return transform.transform(geometry);
 *     });
 *
 * // Chain multiple transformations using utility method
 * GeometryReader chainedReader = VectorTileCodec.newGeometryReader()
 *     .withGeometryFactory(customFactory)
 *     .withGeometryTransformation(GeometryReader.concat(
 *         scaleTransform,
 *         translateTransform,
 *         projectionTransform
 *     ));
 *
 * // Use CoordinateSequenceFilter with utility method
 * CoordinateSequenceFilter filter = new ScaleCoordinateSequenceFilter(scale);
 * GeometryReader filterReader = VectorTileCodec.newGeometryReader()
 *     .withGeometryTransformation(GeometryReader.toFunction(filter));
 * }</pre>
 *
 * @since 1.0
 * @see VectorTileCodec#newGeometryReader()
 */
public interface GeometryReader {

    /**
     * Decodes geometry data from a vector tile feature.
     * <p>
     * This method converts the feature's encoded geometry using the current configuration
     * (GeometryFactory and transformations) of this GeometryReader.
     *
     * @param feature the vector tile feature containing geometry data
     * @return the decoded JTS Geometry object
     * @throws IllegalArgumentException if the feature contains invalid geometry data
     */
    Geometry decode(VectorTile.Layer.Feature feature);

    /**
     * Returns a new GeometryReader configured with the specified GeometryFactory.
     * <p>
     * The GeometryFactory controls how JTS geometry objects are created, including:
     * <ul>
     * <li>Coordinate sequence implementation (array-based, packed, etc.)</li>
     * <li>Precision model for coordinate values</li>
     * <li>Spatial reference system identifier (SRID)</li>
     * </ul>
     *
     * @param geometryFactory the JTS GeometryFactory to use for creating geometries
     * @return a new GeometryReader instance with the specified factory
     * @throws IllegalArgumentException if geometryFactory is null
     */
    GeometryReader withGeometryFactory(GeometryFactory geometryFactory);

    /**
     * Returns a new GeometryReader configured with the specified geometry transformation.
     * <p>
     * The transformation function is applied to each decoded geometry, allowing for:
     * <ul>
     * <li>Coordinate scaling and translation</li>
     * <li>Projection changes</li>
     * <li>Coordinate system transformations</li>
     * <li>Custom geometry modifications</li>
     * </ul>
     *
     * <p>Multiple transformations can be chained using {@link #concat(UnaryOperator...)}.
     *
     * @param transform the transformation function to apply to decoded geometries
     * @return a new GeometryReader instance with the specified transformation
     * @throws IllegalArgumentException if transform is null
     */
    GeometryReader withGeometryTransformation(UnaryOperator<Geometry> transform);

    /**
     * Converts a CoordinateSequenceFilter to a geometry transformation function.
     * <p>
     * This utility method allows using JTS CoordinateSequenceFilter implementations
     * as geometry transformations. The filter is applied to the geometry's coordinate
     * sequences in-place.
     *
     * <h3>Example</h3>
     * <pre>{@code
     * // Create a filter that scales coordinates
     * CoordinateSequenceFilter scaleFilter = new CoordinateSequenceFilter() {
     *     public void filter(CoordinateSequence seq, int i) {
     *         seq.setOrdinate(i, 0, seq.getOrdinate(i, 0) * 2.0); // scale X
     *         seq.setOrdinate(i, 1, seq.getOrdinate(i, 1) * 2.0); // scale Y
     *     }
     *     public boolean isDone() { return false; }
     *     public boolean isGeometryChanged() { return true; }
     * };
     *
     * // Use as geometry transformation
     * GeometryReader reader = VectorTileCodec.newGeometryReader()
     *     .withGeometryTransformation(GeometryReader.toFunction(scaleFilter));
     * }</pre>
     *
     * @param transform the CoordinateSequenceFilter to convert
     * @return a UnaryOperator that applies the filter to geometries
     * @throws IllegalArgumentException if transform is null
     */
    public static UnaryOperator<Geometry> toFunction(CoordinateSequenceFilter transform) {
        UnaryOperator<Geometry> transformingFunction = g -> {
            g.apply(transform);
            return g;
        };
        return transformingFunction;
    }

    /**
     * Concatenates multiple geometry transformation operations into a single function.
     * <p>
     * This utility method chains multiple UnaryOperator transformations in sequence,
     * applying them in the order specified. This is useful for complex coordinate
     * transformations that require multiple steps.
     *
     * <h3>Example</h3>
     * <pre>{@code
     * // Define individual transformations
     * UnaryOperator<Geometry> scale = geom -> scaleGeometry(geom, 2.0);
     * UnaryOperator<Geometry> translate = geom -> translateGeometry(geom, 100, 200);
     * UnaryOperator<Geometry> project = geom -> reprojectGeometry(geom, targetCRS);
     *
     * // Chain them together
     * UnaryOperator<Geometry> combined = GeometryReader.concat(scale, translate, project);
     *
     * // Use in GeometryReader
     * GeometryReader reader = VectorTileCodec.newGeometryReader()
     *     .withGeometryTransformation(combined);
     * }</pre>
     *
     * @param operations the transformation operations to chain, applied in order
     * @return a single UnaryOperator that applies all transformations sequentially
     * @throws IllegalArgumentException if operations array is null or contains null elements
     */
    @SafeVarargs
    public static UnaryOperator<Geometry> concat(UnaryOperator<Geometry>... operations) {
        Function<Geometry, Geometry> concat = UnaryOperator.identity();
        for (UnaryOperator<Geometry> operation : operations) {
            concat = concat.andThen(operation);
        }
        return concat::apply;
    }
}
