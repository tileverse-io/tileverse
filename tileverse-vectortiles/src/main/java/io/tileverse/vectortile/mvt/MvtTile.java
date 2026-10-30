/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import static java.util.stream.Collectors.toCollection;

import io.tileverse.vectortile.model.VectorTile;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import org.locationtech.jts.geom.Envelope;

/**
 * Creates an MvtTile from protobuf data.
 *
 * <p>Coordinates are returned in their natural extent space (0 to extent-1) as stored in the MVT data.
 *
 * @param tileProto the MVT protobuf tile data
 */
record MvtTile(VectorTileProto.Tile tileProto, Envelope bounds) implements VectorTile {

    MvtTile(VectorTileProto.Tile tileProto) {
        this(tileProto, null);
    }

    @Override
    public Optional<Envelope> boundingBox() {
        return Optional.ofNullable(bounds);
    }

    @Override
    public VectorTile withBoundingBox(Envelope bounds) {
        return new MvtTile(tileProto, bounds);
    }

    @Override
    public Set<String> getLayerNames() {
        return tileProto.getLayersList().stream()
                .map(VectorTileProto.Tile.Layer::getName)
                .collect(toCollection(LinkedHashSet::new));
    }

    @Override
    public List<Layer> getLayers() {
        return tileProto.getLayersList().stream().map(this::newLayer).toList();
    }

    @Override
    public Optional<Layer> getLayer(String name) {
        return tileProto.getLayersList().stream()
                .filter(l -> name.equals(l.getName()))
                .findFirst()
                .map(this::newLayer);
    }

    private Layer newLayer(VectorTileProto.Tile.Layer layer) {
        return new MvtLayer(this, layer);
    }

    @Override
    public OptionalInt serializedSize() {
        return OptionalInt.of(getSerializedSize());
    }

    public int getSerializedSize() {
        return tileProto.getSerializedSize();
    }

    @Override
    public String toString() {
        return "%s[%s]".formatted(getClass().getSimpleName(), getLayers());
    }
}
