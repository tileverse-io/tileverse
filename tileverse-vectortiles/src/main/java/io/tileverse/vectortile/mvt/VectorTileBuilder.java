/*
 * Copyright 2015 Electronic Chart Centre
 * Copyright 2025 Multiversio LLC. All rights reserved.
 *
 * Modifications: Modernized and integrated into Tileverse PMTiles project.
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

import io.tileverse.vectortile.model.Tile;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;

/**
 * A streaming builder for creating Mapbox Vector Tiles (MVT) without
 * intermediate memory storage.
 * <p>
 * <strong>IMPORTANT - Coordinate System Requirements:</strong>
 * <p>
 * This builder expects geometries to already be in <strong>tile extent
 * coordinate space</strong>, NOT in real-world coordinate reference systems
 * (CRS) like WGS84 or Web Mercator.
 *
 * <h2>Coordinate Transformation Responsibility</h2>
 * <ul>
 * <li><strong>Client responsibility:</strong> Transform geometries from
 * real-world CRS to tile extent coordinates</li>
 * <li><strong>VectorTileBuilder responsibility:</strong> Encode geometries
 * to MVT protobuf format</li>
 * </ul>
 *
 * <h3>Input Coordinate Expectations</h3>
 * <p>
 * Geometries must have coordinates in the range [0, extent-1].
 * For example, with the default extent of 4096, coordinates should be
 * in the range [0, 4095]. No coordinate scaling is performed - coordinates
 * are preserved exactly as provided.
 *
 * <h2>Typical Usage Pattern</h2>
 *
 * <pre>{@code
 * // 1. Client transforms from real-world CRS to tile extent space
 * Geometry realWorldGeom = feature.getDefaultGeometry(); // WGS84, Web Mercator, etc.
 * Geometry extentSpaceGeom = transformToExtentSpace(realWorldGeom, tileEnvelope, 4096);
 *
 * // 2. VectorTileBuilder encodes coordinates directly
 * VectorTileBuilder tileBuilder = new VectorTileBuilder().setExtent(4096);
 * tileBuilder.layer().name("roads").feature().geometry(extentSpaceGeom)
 * 		.attribute("highway", "primary").build().build();
 * }</pre>
 *
 * <h2>MVT Coordinate System Details</h2>
 * <p>
 * Mapbox Vector Tiles use integer coordinates for efficient storage and
 * rendering. The extent parameter defines both the coordinate range and
 * precision for features in a tile.
 *
 * <h3>Coordinate Precision and Storage</h3>
 * <p>
 * MVT stores coordinates as integers to minimize file size. The extent
 * parameter (typically 4096) defines the valid coordinate range [0, extent-1]
 * and determines the precision available for representing geographic features
 * within the tile boundaries.
 *
 * <h3>Coordinate Flow Example</h3>
 * <p>
 * For a tile with extent=4096:
 *
 * <pre>{@code
 * Input coordinate: POINT(40, 60) in extent coordinate space [0, extent-1]
 *                         ↓
 * Store as MVT integers: 40, 60
 *                         ↓
 * Decode: POINT(40, 60) in extent coordinate space [Coordinates preserved]
 * }</pre>
 *
 * <p>
 * The {@code extent} parameter controls the coordinate precision and valid coordinate
 * range [0, extent-1] for both input and output geometries.
 *
 * <h3>Thread Safety</h3>
 * <p>
 * This class is <strong>NOT</strong> thread-safe. Each thread should use its
 * own VectorTileBuilder instance.
 */
public class VectorTileBuilder {

    // parameters
    private static final class Params {
        private int extent = 4096;
        private int clipBuffer = 32;
        private boolean autoincrementIds = true;
        private double simplificationDistanceTolerance = -1d;
        private boolean usePrecisionModelSnapping = false;
    }

    // for internal processing
    static final class BuildParams {
        private final Params params = new Params();

        private double minimumLength;
        private double minimumArea;
        private Geometry clipGeometry;
        private Envelope clipEnvelope;
        private PreparedGeometry clipGeometryPrepared;
        private long autoincrement;

        private void reset() {
            minimumArea = 0d;
            minimumLength = 0d;
            clipGeometry = null;
            clipEnvelope = null;
            clipGeometryPrepared = null;
            autoincrement = 0L;
        }

        public int getExtent() {
            return params.extent;
        }

        private void setExtent(int extent) {
            params.extent = extent;
            reset();
        }

        private void setClipBuffer(int clipBuffer) {
            params.clipBuffer = clipBuffer;
            reset();
        }

        private void setAutoincrementIds(boolean autoincrementIds) {
            params.autoincrementIds = autoincrementIds;
            reset();
        }

        private void setSimplificationDistanceTolerance(double simplificationDistanceTolerance) {
            params.simplificationDistanceTolerance = simplificationDistanceTolerance;
            reset();
        }

        private void setUsePrecisionModelSnapping(boolean usePrecisionModelSnapping) {
            params.usePrecisionModelSnapping = usePrecisionModelSnapping;
            reset();
        }

        public long getNextFeatureId() {
            return params.autoincrementIds ? autoincrement++ : -1;
        }

        public double getMinimumLength() {
            return minimumLength;
        }

        public double getMinimumArea() {
            return minimumArea;
        }

        public double getSimplificationDistanceTolerance() {
            return params.simplificationDistanceTolerance;
        }

        public boolean isUsePrecisionModelSnapping() {
            return params.usePrecisionModelSnapping;
        }

        public Geometry getClipGeometry() {
            ensureClipGeometry();
            return clipGeometry;
        }

        public Envelope getClipEnvelope() {
            ensureClipGeometry();
            return clipEnvelope;
        }

        public PreparedGeometry getClipGeometryPrepared() {
            ensureClipGeometry();
            return clipGeometryPrepared;
        }

        private void ensureClipGeometry() {
            if (clipGeometry == null) {
                clipGeometry = createTileEnvelope();
                clipEnvelope = clipGeometry.getEnvelopeInternal();
                clipGeometryPrepared = PreparedGeometryFactory.prepare(clipGeometry);
            }
        }

        private Geometry createTileEnvelope() {
            int buffer = params.clipBuffer;
            int size = params.extent;
            Coordinate[] coords = new Coordinate[5];
            coords[0] = new Coordinate(0 - buffer, size + buffer);
            coords[1] = new Coordinate(size + buffer, size + buffer);
            coords[2] = new Coordinate(size + buffer, 0 - buffer);
            coords[3] = new Coordinate(0 - buffer, 0 - buffer);
            coords[4] = coords[0];
            return new GeometryFactory().createPolygon(coords);
        }
    }

    private final BuildParams state = new BuildParams();

    public VectorTileBuilder init(Tile tile) {
        throw new UnsupportedOperationException("implement");
    }

    public VectorTileBuilder merge(Tile tile) {
        throw new UnsupportedOperationException("implement");
    }

    /**
     * Create a new layer builder for this vector tile.
     *
     * @return a new LayerBuilder instance
     */
    public LayerBuilder layer() {
        return new LayerBuilder(this);
    }

    /**
     * Internal method to build the protobuf tile structure. Layers are added
     * directly via LayerBuilder.build(), so this just finalizes.
     */
    public Tile build() {
        io.tileverse.vectortile.mvt.VectorTileProto.Tile proto = buildProto();
        MvtTile tile = new MvtTile(proto);
        protoTileBuilder.clear();
        return tile;
    }

    VectorTileProto.Tile buildProto() {
        return protoTileBuilder.build();
    }

    /**
     * The proto tile being built
     */
    private VectorTileProto.Tile.Builder protoTileBuilder = VectorTileProto.Tile.newBuilder();

    /**
     * The extent value control how detailed the coordinates are encoded in the
     * vector tile. 4096 is the default.
     * <p>
     * This will be the extent on the resulting {@link io.tileverse.vectortile.mvt.VectorTileProto.Tile.Layer#getExtent()}, and
     * coordinates added to {@link FeatureBuilder#geometry(Geometry)} are expected to be in {@code 0..extent-1} coordinate space in
     * both axis. Input geometries will be clipped to this extent plus the {@link #setClipBuffer(int) buffer}.
     *
     * @param extent an int with extent value. 4096 is a good value.
     * @return
     */
    public VectorTileBuilder setExtent(int extent) {
        state.setExtent(extent);
        return this;
    }

    /**
     * The clip buffer value control how large the clipping area is outside of the
     * tile for geometries. 0 means that the clipping is done at the tile border. 8
     * is a good default.
     *
     * @param clipBuffer an int with clip buffer size for geometries. 8 is a good
     *                   value.
     * @return
     */
    public VectorTileBuilder setClipBuffer(int clipBuffer) {
        state.setClipBuffer(clipBuffer);
        return this;
    }

    /**
     * when true the vector tile feature id is auto incremented
     *
     * @param autoincrementIds
     * @return
     */
    public VectorTileBuilder setAutoIncrementIds(boolean autoincrementIds) {
        state.setAutoincrementIds(autoincrementIds);
        return this;
    }

    /**
     * A positive double representing the distance tolerance to be used for
     * non-points before (optional) scaling and encoding. A value {@code <=0} will
     * prevent simplifying geometry. A typical value is 0.1 for most use cases.
     *
     * @param simplificationDistanceTolerance
     * @return
     */
    public VectorTileBuilder setSimplificationDistanceTolerance(double simplificationDistanceTolerance) {
        state.setSimplificationDistanceTolerance(simplificationDistanceTolerance);
        return this;
    }

    /**
     * When true, applies JTS PrecisionModel snapping as final step after geometry
     * processing to ensure integer coordinates and discard invalid geometries. When
     * false, uses Math.round() during encoding (default behavior).
     *
     * @param usePrecisionModelSnapping
     * @return
     */
    public VectorTileBuilder setUsePrecisionModelSnapping(boolean usePrecisionModelSnapping) {
        state.setUsePrecisionModelSnapping(usePrecisionModelSnapping);
        return this;
    }

    /**
     * Internal method for LayerBuilder to add completed layers directly.
     */
    void addLayer(VectorTileProto.Tile.Layer layer) {
        if (layer != null && layer.getFeaturesCount() > 0) {
            protoTileBuilder.addLayers(layer);
        }
    }

    /**
     * Expose configuration to LayerBuilder
     */
    BuildParams params() {
        return state;
    }
}
