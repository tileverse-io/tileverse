/*
 * Copyright 2015 Electronic Chart Centre
 * Copyright 2026 Multiversio LLC. All rights reserved.
 *
 * Modifications: the simplification of VectorTileEncoder as a pipeline step.
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
package io.tileverse.vectortile.pipeline;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Lineal;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier;
import org.locationtech.jts.simplify.TopologyPreservingSimplifier;

/**
 * Simplifies the geometries with a distance tolerance in the units of the geometries, as java-vector-tile does.
 *
 * <p>A {@link Point} is returned as is. Lines go through the Douglas-Peucker simplifier of JTS. Polygons go through it
 * too, and through the topology-preserving simplifier instead when the Douglas-Peucker result is not polygonal. Other
 * geometries, such as a {@code MultiPoint}, go through the topology-preserving simplifier.
 *
 * <p>The Douglas-Peucker simplifier checks a simplified polygon with {@code isValid()}, and returns the
 * {@code buffer(0)} of an invalid one. The topology-preserving simplifier keeps a valid input valid. Simplified lines
 * are not checked.
 */
public final class SimplificationStep implements GeometryStep {

    private final double distanceTolerance;

    /**
     * Creates a step simplifying with the tolerance.
     *
     * @param distanceTolerance the distance tolerance of the simplifiers, in the units of the geometries
     * @throws IllegalArgumentException if the tolerance is not positive and finite
     */
    public SimplificationStep(double distanceTolerance) {
        this.distanceTolerance = Parameters.requirePositiveAndFinite(distanceTolerance, "distance tolerance");
    }

    @Override
    public Geometry apply(Geometry geometry) {
        if (geometry instanceof Point) {
            return geometry;
        }
        if (geometry instanceof Lineal) {
            return DouglasPeuckerSimplifier.simplify(geometry, distanceTolerance);
        }
        if (geometry instanceof Polygonal) {
            return simplifyPolygonal(geometry);
        }
        return TopologyPreservingSimplifier.simplify(geometry, distanceTolerance);
    }

    /**
     * Simplifies a polygon or a multipolygon with Douglas-Peucker, or with the topology-preserving simplifier when the
     * Douglas-Peucker result is not polygonal. Douglas-Peucker returns a collection when the shell of a member of a
     * multipolygon collapses and a hole of that member does not: the hole comes back as a ring.
     */
    private Geometry simplifyPolygonal(Geometry geometry) {
        Geometry simplified = DouglasPeuckerSimplifier.simplify(geometry, distanceTolerance);
        if (simplified instanceof Polygonal) {
            return simplified;
        }
        return TopologyPreservingSimplifier.simplify(geometry, distanceTolerance);
    }

    @Override
    public String toString() {
        return "SimplificationStep[distanceTolerance=" + distanceTolerance + "]";
    }
}
