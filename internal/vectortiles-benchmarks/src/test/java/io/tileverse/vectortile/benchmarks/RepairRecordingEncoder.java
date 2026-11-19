/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.benchmarks;

import io.tileverse.vectortile.benchmarks.TileComparison.FeatureKey;
import io.tileverse.vectortile.model.VectorTile;
import io.tileverse.vectortile.model.VectorTile.Layer;
import io.tileverse.vectortile.mvt.VectorTileCodec;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import no.ecc.vectortile.VectorTileEncoder;
import org.locationtech.jts.geom.Geometry;

/**
 * A java-vector-tile encoder recording the repaired features. java-vector-tile repairs the commands of a polygon in
 * {@link #encode()}, when it simplifies the geometries and the commands decode to an invalid polygon. A repair returns
 * another list of commands than the one given.
 *
 * <p>{@link #validateAndRepairCommands} receives the clipped and simplified geometry of a feature, without its layer
 * and its id. The encoder records the position of the repaired feature in the tile instead. {@code encode()} calls the
 * method once per feature, layer after layer, and leaves out a feature when the method returns no commands: the
 * features given commands are the features of the tile, in tile order.
 *
 * <p>An encoder writes one tile, as an encoder keeps the features added to it.
 */
final class RepairRecordingEncoder extends VectorTileEncoder {

    private static final VectorTileCodec CODEC = new VectorTileCodec();

    /** The positions of the repaired features in the tile, counted from 0 across the layers. */
    private final BitSet repairedPositions = new BitSet();

    /** The number of features given commands so far: the position of the next one in the tile. */
    private int featureCount;

    /** Creates an encoder with the settings of the {@link VectorTileEncoder} constructor of the same parameters. */
    RepairRecordingEncoder(
            int extent,
            int clipBuffer,
            boolean autoScale,
            boolean autoincrementIds,
            double simplificationDistanceTolerance) {
        super(extent, clipBuffer, autoScale, autoincrementIds, simplificationDistanceTolerance);
    }

    /** Records the position of the feature when java-vector-tile repairs its commands. */
    @Override
    protected List<Integer> validateAndRepairCommands(List<Integer> commands, Geometry geometry) {
        List<Integer> validated = super.validateAndRepairCommands(commands, geometry);
        // encode() leaves out a feature without commands: the feature takes no position in the tile
        if (validated.isEmpty()) {
            return validated;
        }
        if (validated != commands) {
            repairedPositions.set(featureCount);
        }
        featureCount++;
        return validated;
    }

    /**
     * Returns the features repaired by this encoder, by layer name and id.
     *
     * @param tile the tile encoded by this encoder
     * @return the repaired features
     * @throws IOException if the tile cannot be decoded
     * @throws IllegalArgumentException if the number of features of the tile differs from the number written by this
     *     encoder
     */
    Set<FeatureKey> repairedFeatures(byte[] tile) throws IOException {
        List<FeatureKey> features = featuresInTileOrder(tile);
        if (features.size() != featureCount) {
            throw new IllegalArgumentException("The tile has " + features.size() + " features, and this encoder wrote "
                    + featureCount + ": it is not the tile of this encoder");
        }
        return repairedPositions.stream().mapToObj(features::get).collect(Collectors.toSet());
    }

    /** The keys of the features of the tile, layer after layer, in tile order. */
    private static List<FeatureKey> featuresInTileOrder(byte[] tile) throws IOException {
        VectorTile decoded = CODEC.decode(tile);
        List<FeatureKey> features = new ArrayList<>();
        for (Layer layer : decoded.getLayers()) {
            String name = layer.getName();
            layer.getFeatures().forEach(feature -> features.add(new FeatureKey(name, feature.getId())));
        }
        return features;
    }
}
