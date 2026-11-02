/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import static java.util.stream.Collectors.toCollection;

import io.tileverse.vectortile.model.VectorTile;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import org.locationtech.jts.geom.Envelope;

/**
 * A vector tile read from its encoded bytes. The tile holds the bytes and the index of its layers, with the geometries
 * of their features. The features read their other fields from the bytes when asked.
 *
 * <p>The bytes and the index are never modified. A tile can be shared between threads, as the tile stores do with the
 * tiles in their caches. Coordinates are returned in their natural extent space (0 to extent-1) as stored in the MVT
 * data.
 */
final class MvtTile implements VectorTile {

    /**
     * The bytes of the fields below, for the heap size: three references (bytes, layers, bounds). A new field must be
     * counted here.
     */
    private static final int FIELD_BYTES = 3 * HeapSize.REFERENCE;

    private final byte[] bytes;
    private final LayerIndex[] layers;
    private final Envelope bounds;

    private MvtTile(byte[] bytes, LayerIndex[] layers, Envelope bounds) {
        this.bytes = bytes;
        this.layers = layers;
        this.bounds = bounds;
    }

    /**
     * Copies a range of an array and indexes the copy. The caller may reuse the array afterwards.
     *
     * @param failOnMalformedGeometry whether a malformed geometry fails the tile, or decodes as empty
     * @throws IOException if protobuf's parser rejects the tile, as described by {@link LayerIndex#indexTile}
     */
    static MvtTile copyOf(byte[] data, int offset, int length, boolean failOnMalformedGeometry) throws IOException {
        byte[] copy = Arrays.copyOfRange(data, offset, offset + length);
        return wrap(copy, failOnMalformedGeometry);
    }

    /**
     * Indexes an array handed over by the caller, and decodes a malformed geometry as empty. The builders write no
     * malformed geometry.
     *
     * @see #wrap(byte[], boolean)
     */
    static MvtTile wrap(byte[] bytes) throws IOException {
        return wrap(bytes, false);
    }

    /**
     * Indexes an array handed over by the caller. The tile owns the array from then on, and the caller must not modify
     * it.
     *
     * @param failOnMalformedGeometry whether a malformed geometry fails the tile, or decodes as empty
     * @throws IOException if protobuf's parser rejects the tile, as described by {@link LayerIndex#indexTile}
     */
    static MvtTile wrap(byte[] bytes, boolean failOnMalformedGeometry) throws IOException {
        return new MvtTile(bytes, LayerIndex.indexTile(bytes, failOnMalformedGeometry), null);
    }

    /**
     * Returns the encoded tile, without a copy. The array must stay unmodified. The codec returns a copy from
     * {@link VectorTileCodec#encode(VectorTile)}, and hands the array itself to the caller's stream in
     * {@link VectorTileCodec#encode(VectorTile, OutputStream)}.
     */
    byte[] bytes() {
        return bytes;
    }

    @Override
    public Optional<Envelope> boundingBox() {
        return Optional.ofNullable(bounds);
    }

    /** Returns a copy of this tile with the bounding box, over the same bytes and the same index. */
    @Override
    public VectorTile withBoundingBox(Envelope bounds) {
        return new MvtTile(bytes, layers, bounds);
    }

    @Override
    public Set<String> getLayerNames() {
        return Arrays.stream(layers).map(LayerIndex::name).collect(toCollection(LinkedHashSet::new));
    }

    @Override
    public List<Layer> getLayers() {
        return Arrays.stream(layers).map(this::newLayer).toList();
    }

    @Override
    public Optional<Layer> getLayer(String name) {
        return Arrays.stream(layers)
                .filter(layer -> name.equals(layer.name()))
                .findFirst()
                .map(this::newLayer);
    }

    // The layer leads back to this instance, and to its bounding box
    private Layer newLayer(LayerIndex layer) {
        return new MvtLayer(this, layer);
    }

    @Override
    public OptionalInt serializedSize() {
        return OptionalInt.of(bytes.length);
    }

    /**
     * Estimates the heap memory held by the tile object, its bytes and the index of its layers with their packed
     * geometries, capped at {@link Integer#MAX_VALUE}. A copy made by {@link #withBoundingBox} reports the size of the
     * bytes and the index shared with the original. Each call computes the estimate again: a cache asks once per tile.
     */
    @Override
    public OptionalInt heapSize() {
        long size = HeapSize.object(FIELD_BYTES) + HeapSize.array(Byte.BYTES, bytes.length) + layersHeapSize();
        int capped = (int) Math.min(size, Integer.MAX_VALUE);
        return OptionalInt.of(capped);
    }

    private long layersHeapSize() {
        long size = HeapSize.array(HeapSize.REFERENCE, layers.length);
        for (LayerIndex layer : layers) {
            size += layer.heapSize();
        }
        return size;
    }

    @Override
    public String toString() {
        return "%s[%s]".formatted(getClass().getSimpleName(), getLayers());
    }
}
