/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import io.tileverse.vectortile.model.GeometryReader;
import io.tileverse.vectortile.model.VectorTile;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.function.UnaryOperator;
import org.locationtech.jts.geom.GeometryFactory;

/**
 * Codec for serializing and deserializing vector tiles between model objects and byte representations.
 *
 * <p>This class handles the conversion between the abstract {@link VectorTile} model and the MVT protobuf format,
 * without any coordinate transformations or scaling logic. The codec reads and writes the coordinates in the extent of
 * each layer.
 *
 * <h2>Design Philosophy</h2>
 *
 * <p>The codec simply converts between the model and the encoded bytes. Coordinates are always preserved in their
 * natural extent space, as stored in the tile.
 *
 * <h2>Encoding</h2>
 *
 * <p>A tile decoded by this codec or built by a {@link VectorTileBuilder} holds its encoded bytes, and the encode
 * methods write them as they are. Another {@link VectorTile} implementation is written one layer at a time, each layer
 * with its own extent, as {@link LayerBuilder#merge(VectorTile.Layer)} writes the features of a layer: without the
 * steps of a pipeline, with the coordinates rounded, and without an id for a feature of id 0. A negative id stands for
 * an id of 2^63 or above, and is written as its unsigned value. The encode methods fail with an
 * {@link IllegalArgumentException} on a layer without a name, and on two layers of one name, also when one of them has
 * no features: the MVT specification forbids two layers of one name in a tile. They fail the same way on a name with an
 * unpaired surrogate: UTF-8 has no encoding for it.
 *
 * <h2>Malformed Geometries</h2>
 *
 * <p>Decoding a tile also decodes the geometry of each feature. A geometry is malformed when it does not start with a
 * MoveTo command, when it holds an unknown command or a command announcing more points than the parameters left, or
 * when a polygon ring of three points or more does not end on its first point. A codec created with
 * {@link #VectorTileCodec()} decodes a malformed geometry as the empty geometry of its type, and the feature keeps its
 * id and its attributes. A codec from {@link #failingOnMalformedGeometry()} fails to decode the tile instead.
 *
 * <p>A decoded geometry leaves out its lines of fewer than 2 points, its rings of fewer than 3 points, and its closed
 * rings of zero area. The MVT specification says that a ring of zero area signals anomalous points. A feature with all
 * its parts left out gets the empty geometry of its type.
 *
 * <h2>Geometry Reading Customization</h2>
 *
 * <p>For advanced geometry reading with custom factories or transformations, use {@link #newGeometryReader()} to create
 * a configurable {@link GeometryReader}:
 *
 * <pre>{@code
 * // Create custom geometry reader with specific factory and transformations
 * GeometryFactory customFactory = new GeometryFactory(new CoordinateArraySequenceFactory());
 * GeometryReader geometryReader = VectorTileCodec.newGeometryReader()
 *     .withGeometryFactory(customFactory)
 *     .withGeometryTransformation(geometry -> transform(geometry));
 *
 * // Use with decoded tile to get features with custom geometry handling
 * VectorTile decoded = codec.decode(data);
 * Stream<Feature> features = decoded.getFeatures("layer_name", filter, geometryReader);
 * }</pre>
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * // Create codec instance
 * VectorTileCodec codec = new VectorTileCodec();
 *
 * // Encode model to bytes
 * byte[] encoded = codec.encode(tile);
 *
 * // Decode bytes to model (coordinates in extent space)
 * VectorTile decoded = codec.decode(encoded);
 *
 * // Get features with default geometry reading
 * Stream<Feature> features = decoded.getFeatures("layer_name", feature -> true);
 * }</pre>
 *
 * <h2>Thread Safety</h2>
 *
 * <p>This class is thread-safe and can be shared across multiple threads.
 *
 * @see VectorTileBuilder for creating tile models
 * @see GeometryReader for custom geometry reading configuration
 */
public class VectorTileCodec {

    /**
     * Arrays for the tiles read from a stream. The length of a stream is unknown until its end, and the decoded tile
     * keeps a copy of exact length: the stream is read into a borrowed array. A new array has 64 KiB and doubles until
     * the tile fits. The pool keeps up to 16 MiB of arrays between decodes.
     */
    private static final ByteArrayPool ARRAYS = new ByteArrayPool(64 * 1024, 16L * 1024 * 1024);

    private final boolean failOnMalformedGeometry;

    /** Constructs a new VectorTileCodec, decoding a malformed geometry as the empty geometry of its type. */
    public VectorTileCodec() {
        this(false);
    }

    private VectorTileCodec(boolean failOnMalformedGeometry) {
        this.failOnMalformedGeometry = failOnMalformedGeometry;
    }

    /**
     * Returns a codec whose decode methods throw an {@link IOException} at the first malformed geometry of a tile. The
     * message of the exception names the layer, the index of the feature in the layer, and the problem.
     *
     * @return a codec failing on malformed geometry
     */
    public static VectorTileCodec failingOnMalformedGeometry() {
        return new VectorTileCodec(true);
    }

    // ========== Encoding Methods ==========

    /**
     * Encodes a tile model to a byte array. The array is a copy, and the caller may modify it.
     *
     * <p>Use {@link #encode(VectorTile, OutputStream)} or {@link #encode(VectorTile, ByteBuffer)} for better
     * performance.
     *
     * @param tile the tile model to encode
     * @return the encoded vector tile as a byte array
     * @throws IllegalArgumentException if another implementation has a layer without a name, or two layers of one name,
     *     as described in the class documentation
     */
    public byte[] encode(VectorTile tile) {
        if (tile instanceof MvtTile mvt) {
            return mvt.bytes().clone();
        }
        return builderOf(tile).toByteArray();
    }

    /**
     * Encodes a tile model to an OutputStream, avoiding intermediate byte array allocation. This is the recommended
     * method for better performance and memory efficiency.
     *
     * @param tile the tile model to encode
     * @param outputStream the stream to write the encoded tile to
     * @throws IOException if writing to the stream fails
     * @throws IllegalArgumentException if another implementation has a layer without a name, or two layers of one name,
     *     as described in the class documentation
     */
    public void encode(VectorTile tile, OutputStream outputStream) throws IOException {
        if (tile instanceof MvtTile mvt) {
            outputStream.write(mvt.bytes());
        } else {
            builderOf(tile).writeTo(outputStream);
        }
    }

    /**
     * Encodes a tile model to a ByteBuffer.
     *
     * <p>The caller must ensure the ByteBuffer has sufficient remaining capacity.
     *
     * @param tile the tile model to encode
     * @param buffer the ByteBuffer to write the encoded tile to
     * @throws InsufficientBufferException if the buffer has insufficient space, containing the
     *     {@link InsufficientBufferException#getSerializedSize() required size} for proper buffer allocation
     * @throws IllegalArgumentException if another implementation has a layer without a name, or two layers of one name,
     *     as described in the class documentation
     */
    public void encode(VectorTile tile, ByteBuffer buffer) throws IOException {
        byte[] encoded = encodedBytes(tile);
        if (buffer.remaining() < encoded.length) {
            throw new InsufficientBufferException(encoded.length, buffer.remaining());
        }
        buffer.put(encoded);
    }

    /**
     * Returns the size of the encoded tile. A decoded or built tile holds its encoded bytes, and the size costs nothing
     * to compute. Another implementation is encoded to measure it.
     *
     * @param tile the tile model to calculate size for
     * @return the size in bytes that the encoded tile will occupy
     * @throws IllegalArgumentException if another implementation has a layer without a name, or two layers of one name,
     *     as described in the class documentation
     */
    public int getSerializedSize(VectorTile tile) {
        return encodedBytes(tile).length;
    }

    // ========== Decoding Methods ==========

    /**
     * Decodes a byte array to a tile model. The tile keeps a copy of the array, and the caller may reuse it.
     *
     * <p>Coordinates are returned in their natural extent space (0 to extent-1) as stored in the MVT data.
     *
     * @param data the encoded MVT data
     * @return the decoded tile model
     * @throws IOException if decoding fails
     */
    public VectorTile decode(byte[] data) throws IOException {
        return MvtTile.copyOf(data, 0, data.length, failOnMalformedGeometry);
    }
    /**
     * Decodes the remaining bytes of a ByteBuffer to a tile model. The tile keeps a copy of the bytes, and the position
     * and the limit of the buffer do not change.
     *
     * <p>Coordinates are returned in their natural extent space (0 to extent-1) as stored in the MVT data.
     *
     * @param data the encoded MVT data
     * @return the decoded tile model
     * @throws IOException if decoding fails
     */
    public VectorTile decode(ByteBuffer data) throws IOException {
        if (data.hasArray()) {
            int offset = data.arrayOffset() + data.position();
            return MvtTile.copyOf(data.array(), offset, data.remaining(), failOnMalformedGeometry);
        }
        byte[] bytes = new byte[data.remaining()];
        data.get(data.position(), bytes);
        return MvtTile.wrap(bytes, failOnMalformedGeometry);
    }

    /**
     * Decodes an InputStream to a tile model.
     *
     * <p>Coordinates are returned in their natural extent space (0 to extent-1) as stored in the MVT data.
     *
     * <p>The stream is read to its end: decoding needs the whole tile.
     *
     * @param data the encoded MVT data stream
     * @return the decoded tile model
     * @throws IOException if decoding fails
     */
    public VectorTile decode(InputStream data) throws IOException {
        return ARRAYS.withContentOf(data, (array, length) -> MvtTile.copyOf(array, 0, length, failOnMalformedGeometry));
    }

    // ========== Private Helper Methods ==========

    /**
     * Returns the encoded bytes held by a decoded or built tile, without a copy, or the bytes of another implementation
     * once encoded.
     */
    private static byte[] encodedBytes(VectorTile tile) {
        if (tile instanceof MvtTile mvt) {
            return mvt.bytes();
        }
        return builderOf(tile).toByteArray();
    }

    /**
     * Returns a builder holding the layers of another implementation. Each layer keeps its extent, its features skip
     * the pipeline, and no feature gets an automatic id.
     *
     * @throws IllegalArgumentException if a layer has no name, or the name of a layer before it, also of a layer
     *     without features
     */
    private static VectorTileBuilder builderOf(VectorTile tile) {
        VectorTileBuilder builder = new VectorTileBuilder().setAutoIncrementIds(false);
        Set<String> names = new HashSet<>();
        for (VectorTile.Layer layer : tile.getLayers()) {
            String name = layer.getName();
            VectorTileBuilder.requireLayerName(name);
            VectorTileBuilder.requireNameNotRepeated(names, name);
            builder.setExtent(layer.getExtent());
            builder.layer().name(name).merge(layer).build();
        }
        return builder;
    }

    /**
     * Creates a new configurable geometry reader for advanced geometry processing.
     *
     * <p>The returned {@link GeometryReader} can be customized with:
     *
     * <ul>
     *   <li>{@link GeometryReader#withGeometryFactory(GeometryFactory)} - specify a custom JTS GeometryFactory
     *   <li>{@link GeometryReader#withGeometryTransformation(UnaryOperator)} - apply coordinate transformations
     * </ul>
     *
     * <h4>Usage Examples</h4>
     *
     * <pre>{@code
     * // Custom GeometryFactory with specific coordinate sequence implementation
     * GeometryFactory customFactory = new GeometryFactory(new CoordinateArraySequenceFactory());
     * GeometryReader reader = VectorTileCodec.newGeometryReader()
     *     .withGeometryFactory(customFactory);
     *
     * // Apply coordinate transformation (e.g., scale from tile extent to world coordinates)
     * GeometryReader transformingReader = VectorTileCodec.newGeometryReader()
     *     .withGeometryTransformation(geometry -> {
     *         // Transform coordinates from tile extent (0-4095) to world coordinates
     *         AffineTransformation transform = new AffineTransformation();
     *         transform.scale(worldBounds.getWidth() / 4096.0, worldBounds.getHeight() / 4096.0);
     *         transform.translate(worldBounds.getMinX(), worldBounds.getMinY());
     *         return transform.transform(geometry);
     *         // or rather, apply the transformation directly to the geometry's CoordinateSequences:
     *         // geometry.apply(transform);
     *         // return geometry;
     *     });
     *
     * // Chain multiple transformations
     * GeometryReader chainedReader = VectorTileCodec.newGeometryReader()
     *     .withGeometryFactory(customFactory)
     *     .withGeometryTransformation(GeometryReader.concat(
     *         scaleTransform,
     *         translateTransform,
     *         customTransform
     *     ));
     * }</pre>
     *
     * @return a new configurable GeometryReader instance
     * @see GeometryReader#withGeometryFactory(GeometryFactory)
     * @see GeometryReader#withGeometryTransformation(UnaryOperator)
     */
    public static GeometryReader newGeometryReader() {
        return new GeometryDecoder();
    }
}
