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

import io.tileverse.vectortile.mvt.VectorTileBuilder.BuildParams;
import java.util.List;
import java.util.Map;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Lineal;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.geom.Puntal;
import org.locationtech.jts.precision.GeometryPrecisionReducer;

public class FeatureBuilder {

    private final LayerBuilder layerBuilder;
    private final VectorTileProto.Tile.Feature.Builder featureBuilder;

    private GeometryPrecisionReducer precisionReducer;
    private Geometry geometry;

    private GeometryEncoder encoder;

    FeatureBuilder(LayerBuilder layerBuilder) {
        this.layerBuilder = layerBuilder;
        this.featureBuilder = VectorTileProto.Tile.Feature.newBuilder();
        PrecisionModel integerPrecision = new PrecisionModel(1.0); // Scale 1.0 = snap to integers
        precisionReducer = new GeometryPrecisionReducer(integerPrecision);
        precisionReducer.setRemoveCollapsedComponents(true); // Remove degenerate components

        encoder = new GeometryEncoder(params());
    }

    BuildParams params() {
        return layerBuilder.params();
    }

    /**
     * Reset this feature builder for reuse with a new feature ID.
     * This avoids object creation overhead in tight loops.
     */
    FeatureBuilder reset(long id) {
        featureBuilder.clear();
        if (id > -1L) {
            featureBuilder.setId(id);
        }
        return this;
    }

    public FeatureBuilder id(long id) {
        featureBuilder.setId(id);
        return this;
    }

    /**
     * Set the feature geometry.
     * The geometry will be processed at {@link #build()} time and may result in duplicate features added if
     * pre-processing results in multiple geometries.
     *
     * @param geometry a {@link Geometry} for the vector tile feature.
     * @return this FeatureBuilder for method chaining
     */
    public FeatureBuilder geometry(Geometry geometry) {
        this.geometry = geometry;
        return this;
    }

    /**
     * Add an attribute to this feature.
     * Attributes are added directly to the protobuf builder as they're set.
     *
     * @param name the attribute name
     * @param value the attribute value (null values are ignored)
     * @return this FeatureBuilder for method chaining
     */
    public FeatureBuilder attribute(String name, Object value) {
        if (value != null) {
            featureBuilder.addTags(layerBuilder.getKeyIndex(name));
            featureBuilder.addTags(layerBuilder.getValueIndex(value));
        }
        return this;
    }

    public FeatureBuilder attributes(Map<String, Object> attributes) {
        attributes.forEach(this::attribute);
        return this;
    }

    /**
     * Complete building this feature and add it directly to the parent LayerBuilder.
     *
     * @return the layer builder to either build it or continue adding features
     */
    public LayerBuilder build() {

        List<Geometry> finalGeometries = encoder.prepareGeometries(this.geometry);

        if (finalGeometries.isEmpty()) {
            // skip empty/ignored geometry
            return layerBuilder;
        }
        if (finalGeometries.size() == 1) {
            // single-feature to build
            Geometry resultingGeometry = finalGeometries.isEmpty() ? null : finalGeometries.get(0);
            buildFinalFeature(featureBuilder, resultingGeometry);
        } else {
            // We need to create multiple Fetures as processGeometries resulted in disparate geometry types
            // use featureBuilder as prototype
            for (Geometry preparedGeom : finalGeometries) {
                VectorTileProto.Tile.Feature.Builder subFeatureBuilder = copyPrototype(featureBuilder);
                buildFinalFeature(subFeatureBuilder, preparedGeom);
            }
        }

        return layerBuilder;
    }

    private VectorTileProto.Tile.Feature.Builder copyPrototype(VectorTileProto.Tile.Feature.Builder prototype) {
        long id = params().getNextFeatureId();

        VectorTileProto.Tile.Feature.Builder newF = VectorTileProto.Tile.Feature.newBuilder();
        newF.setId(id);
        newF.addAllTags(prototype.getTagsList());
        return newF;
    }

    private void buildFinalFeature(
            VectorTileProto.Tile.Feature.Builder finalFeatureBuilder, Geometry resultingGeometry) {
        VectorTileProto.Tile.GeomType geomType = toGeomType(resultingGeometry);
        finalFeatureBuilder.setType(geomType);
        List<Integer> commands = encoder.commands(resultingGeometry);
        finalFeatureBuilder.addAllGeometry(commands);

        VectorTileProto.Tile.Feature builtFeature = finalFeatureBuilder.build();
        layerBuilder.addFeature(builtFeature);
    }

    private static VectorTileProto.Tile.GeomType toGeomType(Geometry geometry) {
        if (geometry instanceof Puntal) {
            return VectorTileProto.Tile.GeomType.POINT;
        } else if (geometry instanceof Lineal) {
            return VectorTileProto.Tile.GeomType.LINESTRING;
        } else if (geometry instanceof Polygonal) {
            return VectorTileProto.Tile.GeomType.POLYGON;
        }
        return VectorTileProto.Tile.GeomType.UNKNOWN;
    }

    static boolean shouldClosePath(Geometry geometry) {
        return (geometry instanceof Polygon) || (geometry instanceof LinearRing);
    }
}
