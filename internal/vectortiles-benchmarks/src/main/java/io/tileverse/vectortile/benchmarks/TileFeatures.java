/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.benchmarks;

import io.tileverse.vectortile.model.GeometryReader;
import io.tileverse.vectortile.model.VectorTile;
import io.tileverse.vectortile.model.VectorTile.Layer;
import io.tileverse.vectortile.model.VectorTile.Layer.Feature;
import io.tileverse.vectortile.mvt.VectorTileCodec;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.function.UnaryOperator;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;
import org.locationtech.jts.geom.impl.PackedCoordinateSequence;
import org.locationtech.jts.geom.impl.PackedCoordinateSequenceFactory;
import org.locationtech.jts.geom.util.AffineTransformation;
import org.locationtech.jts.geom.util.GeometryEditor;

/**
 * The features of a tile, as given to the writers: the layers in tile order, and the features of the layers with their
 * ids, attributes and geometries in tile coordinates.
 *
 * <p>The geometries decoded by {@link #decode(byte[])} have {@link CoordinateArraySequence}s, the sequences of a
 * default {@link GeometryFactory}. Their {@code getCoordinates()}, called by java-vector-tile, returns the array of the
 * sequence without copying it.
 *
 * @param layers the layers in tile order
 */
public record TileFeatures(List<LayerFeatures> layers) {

    private static final VectorTileCodec CODEC = new VectorTileCodec();

    private static final GeometryReader ARRAY_SEQUENCE_READER =
            VectorTileCodec.newGeometryReader().withGeometryFactory(new GeometryFactory());

    /** The factory of the copies made by {@link #packed()}. */
    private static final GeometryFactory PACKED_FACTORY =
            new GeometryFactory(PackedCoordinateSequenceFactory.DOUBLE_FACTORY);

    /** The largest offset of a jittered ordinate: an offset below 0.5 keeps the rounded ordinate. */
    private static final double MAX_JITTER = 0.49;

    public TileFeatures {
        layers = List.copyOf(layers);
    }

    /**
     * Decodes the features of the tile, with geometries of {@link CoordinateArraySequence}s. A feature gets its
     * position in its layer plus one as id: the ids of a layer differ, and none is 0, the id read for a feature without
     * one.
     *
     * @param tile the encoded tile
     * @return the features of the tile
     * @throws IOException if the tile is malformed
     */
    public static TileFeatures decode(byte[] tile) throws IOException {
        return decode(tile, ARRAY_SEQUENCE_READER);
    }

    /**
     * Decodes the features of the tile, with the geometries built by the reader, and numbers them as
     * {@link #decode(byte[])} does.
     *
     * @param tile the encoded tile
     * @param reader the reader of the geometries
     * @return the features of the tile
     * @throws IOException if the tile is malformed
     */
    public static TileFeatures decode(byte[] tile, GeometryReader reader) throws IOException {
        VectorTile decoded = CODEC.decode(tile);
        List<LayerFeatures> layers = new ArrayList<>();
        for (Layer layer : decoded.getLayers()) {
            layers.add(decodeLayer(layer, reader));
        }
        return new TileFeatures(layers);
    }

    private static LayerFeatures decodeLayer(Layer layer, GeometryReader reader) {
        List<Feature> decoded = layer.getFeatures(feature -> true, reader).toList();
        List<InputFeature> features = new ArrayList<>(decoded.size());
        for (int position = 0; position < decoded.size(); position++) {
            Feature feature = decoded.get(position);
            features.add(new InputFeature(position + 1L, feature.getAttributes(), feature.getGeometry()));
        }
        return new LayerFeatures(layer.getName(), layer.getExtent(), features);
    }

    /**
     * Returns a copy of these features in the coordinates of a tile of the extent. The geometries of a layer of another
     * extent are scaled by the extent divided by the extent of the layer, and the layer takes the extent. A layer of
     * that extent is kept as it is.
     *
     * @param extent the extent of the tile of the copy
     * @return the features in the coordinates of the extent, with the ids and the attributes of these features
     */
    public TileFeatures toExtent(int extent) {
        List<LayerFeatures> changed = new ArrayList<>(layers.size());
        for (LayerFeatures layer : layers) {
            changed.add(layer.toExtent(extent));
        }
        return new TileFeatures(changed);
    }

    /**
     * Returns a copy of these features with the points moved by offsets of at most 0.49 on both axes, drawn from a
     * {@link SplittableRandom} created with the seed. The points of a decoded tile have integer coordinates, and a
     * moved point rounds to its original place. The closing point of a ring moves with its first point: the ring stays
     * closed.
     *
     * @param seed the seed of the offsets: the same seed moves the points of the same features the same way
     * @return the moved features, with the ids and the attributes of these features
     */
    public TileFeatures jittered(long seed) {
        Jitter jitter = new Jitter(new SplittableRandom(seed));
        return withGeometries(jitter::applyTo);
    }

    /**
     * Returns a copy of these features with their coordinates multiplied by the factor.
     *
     * @param factor the factor of both axes
     * @return the scaled features, with the ids and the attributes of these features
     */
    public TileFeatures scaled(double factor) {
        AffineTransformation scale = AffineTransformation.scaleInstance(factor, factor);
        return withGeometries(scale::transform);
    }

    /**
     * Returns a copy of these features with geometries of {@link PackedCoordinateSequence.Double}s. A packed sequence
     * holds the ordinates in one array of doubles. Its {@code getCoordinates()} creates a {@code Coordinate} per point
     * at its first call, and keeps them until an ordinate is set.
     *
     * @return the features with packed coordinate sequences, with the ids and the attributes of these features
     */
    public TileFeatures packed() {
        return withGeometries(PACKED_FACTORY::createGeometry);
    }

    /** The number of features of the layers together. */
    public int featureCount() {
        return layers.stream().mapToInt(layer -> layer.features().size()).sum();
    }

    /** Returns a copy of these features with the geometries changed in layer order, then in feature order. */
    private TileFeatures withGeometries(UnaryOperator<Geometry> change) {
        List<LayerFeatures> changed = new ArrayList<>(layers.size());
        for (LayerFeatures layer : layers) {
            changed.add(layer.withGeometries(change));
        }
        return new TileFeatures(changed);
    }

    /**
     * A layer of the tile.
     *
     * @param name the name of the layer
     * @param extent the extent of the layer in the decoded tile, or the extent given to {@link TileFeatures#toExtent}
     * @param features the features of the layer, in layer order
     */
    public record LayerFeatures(String name, int extent, List<InputFeature> features) {

        public LayerFeatures {
            features = List.copyOf(features);
        }

        /** Returns this layer in the coordinates of a layer of the extent: as it is when it has that extent. */
        private LayerFeatures toExtent(int newExtent) {
            if (newExtent == extent) {
                return this;
            }
            double factor = (double) newExtent / extent;
            AffineTransformation scale = AffineTransformation.scaleInstance(factor, factor);
            return new LayerFeatures(name, newExtent, changedFeatures(scale::transform));
        }

        private LayerFeatures withGeometries(UnaryOperator<Geometry> change) {
            return new LayerFeatures(name, extent, changedFeatures(change));
        }

        private List<InputFeature> changedFeatures(UnaryOperator<Geometry> change) {
            List<InputFeature> changed = new ArrayList<>(features.size());
            for (InputFeature feature : features) {
                Geometry geometry = change.apply(feature.geometry());
                changed.add(new InputFeature(feature.id(), feature.attributes(), geometry));
            }
            return changed;
        }
    }

    /**
     * A feature given to the writers. Its attributes are the map decoded from the tile, shared with the jittered and
     * scaled copies: the writers only read it, and an unmodifiable wrapper would add its own cost to the measured
     * writes.
     *
     * @param id the id of the feature, unique in its layer
     * @param attributes the attributes of the feature, by name
     * @param geometry the geometry of the feature, in tile coordinates
     */
    public record InputFeature(long id, Map<String, Object> attributes, Geometry geometry) {}

    /** Moves the points of geometries by random offsets, and keeps their rings closed. */
    private static final class Jitter extends GeometryEditor.CoordinateSequenceOperation {

        private final SplittableRandom random;

        Jitter(SplittableRandom random) {
            this.random = random;
        }

        Geometry applyTo(Geometry geometry) {
            GeometryEditor editor = new GeometryEditor(geometry.getFactory());
            return editor.edit(geometry, this);
        }

        /**
         * Returns a moved copy of the sequence. The closing point of a ring gets the place of the moved first point.
         */
        @Override
        public CoordinateSequence edit(CoordinateSequence sequence, Geometry geometry) {
            CoordinateSequence moved = sequence.copy();
            boolean ring = geometry instanceof LinearRing && sequence.size() > 0;
            int movedPoints = ring ? sequence.size() - 1 : sequence.size();
            for (int i = 0; i < movedPoints; i++) {
                moved.setOrdinate(i, CoordinateSequence.X, sequence.getX(i) + offset());
                moved.setOrdinate(i, CoordinateSequence.Y, sequence.getY(i) + offset());
            }
            if (ring) {
                closeRing(moved);
            }
            return moved;
        }

        private double offset() {
            return random.nextDouble(-MAX_JITTER, MAX_JITTER);
        }

        private static void closeRing(CoordinateSequence ring) {
            int last = ring.size() - 1;
            ring.setOrdinate(last, CoordinateSequence.X, ring.getX(0));
            ring.setOrdinate(last, CoordinateSequence.Y, ring.getY(0));
        }
    }
}
