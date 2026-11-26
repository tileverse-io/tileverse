/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import io.tileverse.vectortile.mvt.VectorTileBuilder.BuildParams;
import java.util.ArrayList;
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

/**
 * Builder for creating vector tile features with geometry and attributes.
 *
 * <p>This builder is reused by LayerBuilder for efficiency, avoiding object creation overhead when building multiple
 * features.
 */
public class FeatureBuilder {

    // Field numbers of the Feature message of the vector tile schema
    private static final int FEATURE_ID = 1;
    private static final int FEATURE_TAGS = 2;
    private static final int FEATURE_TYPE = 3;
    private static final int FEATURE_GEOMETRY = 4;

    private final LayerBuilder layerBuilder;

    // The id and the tags of the feature being built. A feature without an id has no id field.
    private long id;
    private boolean hasId;
    private final List<Integer> tags = new ArrayList<>();

    // Holds each feature until the layer copies it
    private final WireWriter encodedFeature = new WireWriter();

    private GeometryPrecisionReducer precisionReducer;
    private Geometry geometry;

    private GeometryEncoder encoder;

    FeatureBuilder(LayerBuilder layerBuilder) {
        this.layerBuilder = layerBuilder;
        PrecisionModel integerPrecision = new PrecisionModel(1.0); // Scale 1.0 = snap to integers
        precisionReducer = new GeometryPrecisionReducer(integerPrecision);
        precisionReducer.setRemoveCollapsedComponents(true); // Remove degenerate components

        encoder = new GeometryEncoder(params());
    }

    BuildParams params() {
        return layerBuilder.params();
    }

    /**
     * Reset this feature builder for reuse with a new feature ID. This avoids object creation overhead in tight loops.
     */
    FeatureBuilder reset(long id) {
        tags.clear();
        this.id = id;
        hasId = id > -1L;
        return this;
    }

    /**
     * Sets the feature identifier.
     *
     * @param id the feature ID
     * @return this FeatureBuilder for method chaining
     */
    public FeatureBuilder id(long id) {
        this.id = id;
        hasId = true;
        return this;
    }

    /**
     * Set the feature geometry. The geometry will be processed at {@link #build()} time and may result in duplicate
     * features added if pre-processing results in multiple geometries.
     *
     * @param geometry a {@link Geometry} for the vector tile feature.
     * @return this FeatureBuilder for method chaining
     */
    public FeatureBuilder geometry(Geometry geometry) {
        this.geometry = geometry;
        return this;
    }

    /**
     * Add an attribute to this feature. Attributes are added directly to the tags of the feature as they're set.
     *
     * @param name the attribute name
     * @param value the attribute value (null values are ignored)
     * @return this FeatureBuilder for method chaining
     */
    public FeatureBuilder attribute(String name, Object value) {
        if (value != null) {
            tags.add(layerBuilder.getKeyIndex(name));
            tags.add(layerBuilder.getValueIndex(value));
        }
        return this;
    }

    /**
     * Adds multiple attributes to this feature from a map.
     *
     * @param attributes map of attribute names to values
     * @return this FeatureBuilder for method chaining
     */
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
            addFeature(finalGeometries.get(0));
        } else {
            // We need to create multiple Fetures as processGeometries resulted in disparate geometry types
            for (Geometry preparedGeom : finalGeometries) {
                addFeature(params().getNextFeatureId(), preparedGeom);
            }
        }

        return layerBuilder;
    }

    /** Adds a feature with the id and the tags of this builder. */
    private void addFeature(Geometry geometry) {
        encodedFeature.clear();
        if (hasId) {
            encodedFeature.writeVarint64(FEATURE_ID, id);
        }
        writeTagsTypeAndGeometry(geometry);
        layerBuilder.addFeature(encodedFeature);
    }

    /** Adds a feature with its own id, and the tags of this builder. */
    private void addFeature(long featureId, Geometry geometry) {
        encodedFeature.clear();
        encodedFeature.writeVarint64(FEATURE_ID, featureId);
        writeTagsTypeAndGeometry(geometry);
        layerBuilder.addFeature(encodedFeature);
    }

    // The fields after the id, in the order of their field numbers
    private void writeTagsTypeAndGeometry(Geometry geometry) {
        GeomType geomType = toGeomType(geometry);
        List<Integer> commands = encoder.commands(geometry);
        encodedFeature.writePackedVarints(FEATURE_TAGS, tags);
        encodedFeature.writeVarint32(FEATURE_TYPE, geomType.number());
        encodedFeature.writePackedVarints(FEATURE_GEOMETRY, commands);
    }

    private static GeomType toGeomType(Geometry geometry) {
        if (geometry instanceof Puntal) {
            return GeomType.POINT;
        } else if (geometry instanceof Lineal) {
            return GeomType.LINESTRING;
        } else if (geometry instanceof Polygonal) {
            return GeomType.POLYGON;
        }
        return GeomType.UNKNOWN;
    }

    static boolean shouldClosePath(Geometry geometry) {
        return (geometry instanceof Polygon) || (geometry instanceof LinearRing);
    }
}
