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

import com.google.protobuf.CodedOutputStream;
import io.tileverse.vectortile.model.GeometryReader;
import io.tileverse.vectortile.model.VectorTile;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.function.UnaryOperator;
import org.locationtech.jts.geom.GeometryFactory;

/**
 * Codec for serializing and deserializing vector tiles between model objects and byte representations.
 * <p>
 * This class handles the conversion between the abstract {@link VectorTile} model and the MVT protobuf format,
 * without any coordinate transformations or scaling logic. The codec simply reads and writes data as-is.
 *
 * <h2>Design Philosophy</h2>
 * <p>
 * The codec has no configuration parameters - it simply converts between
 * the model and protobuf representations. Coordinates are always preserved in their natural extent space
 * as stored in the protobuf data.
 *
 * <h2>Geometry Reading Customization</h2>
 * <p>
 * For advanced geometry reading with custom factories or transformations, use {@link #newGeometryReader()}
 * to create a configurable {@link GeometryReader}:
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
 * <p>
 * This class is thread-safe and can be shared across multiple threads.
 *
 * @see VectorTileBuilder for creating tile models
 * @see GeometryReader for custom geometry reading configuration
 */
public class VectorTileCodec {

    // ========== Encoding Methods ==========

    /**
     * Encodes a tile model to a byte array.
     * <p>
     * Use {@link #encode(VectorTile, OutputStream)} or {@link #encode(VectorTile, ByteBuffer)} for better performance.
     *
     * @param tile the tile model to encode
     * @return the encoded vector tile as a byte array
     */
    public byte[] encode(VectorTile tile) {
        VectorTileProto.Tile proto = getProto(tile);
        return proto.toByteArray();
    }

    /**
     * Encodes a tile model to an OutputStream, avoiding intermediate byte array allocation.
     * This is the recommended method for better performance and memory efficiency.
     *
     * @param tile the tile model to encode
     * @param outputStream the stream to write the encoded tile to
     * @throws IOException if writing to the stream fails
     */
    public void encode(VectorTile tile, OutputStream outputStream) throws IOException {
        VectorTileProto.Tile proto = getProto(tile);
        proto.writeTo(outputStream);
    }

    /**
     * Encodes a tile model to a ByteBuffer.
     * <p>
     * The caller must ensure the ByteBuffer has sufficient remaining capacity.
     *
     * @param tile the tile model to encode
     * @param buffer the ByteBuffer to write the encoded tile to
     * @throws InsufficientBufferException if the buffer has insufficient space,
     *         containing the {@link InsufficientBufferException#getSerializedSize() required size} for proper buffer allocation
     * @throws IOException if encoding fails for other reasons
     */
    public void encode(VectorTile tile, ByteBuffer buffer) throws InsufficientBufferException, IOException {
        VectorTileProto.Tile proto = getProto(tile);
        int serializedSize = proto.getSerializedSize();

        if (buffer.remaining() < serializedSize) {
            throw new InsufficientBufferException(serializedSize, buffer.remaining());
        }

        // Create a CodedOutputStream that writes directly to the ByteBuffer
        CodedOutputStream codedOutput = CodedOutputStream.newInstance(buffer);
        proto.writeTo(codedOutput);
        codedOutput.flush();
    }

    /**
     * Returns the serialized size of the encoded tile without actually encoding it.
     * <p>
     * <strong>Note:</strong> This method requires building the entire protobuf structure internally,
     * which involves computational overhead. Use judiciously.
     *
     * @param tile the tile model to calculate size for
     * @return the size in bytes that the encoded tile will occupy
     */
    public int getSerializedSize(VectorTile tile) {
        VectorTileProto.Tile proto = getProto(tile);
        return proto.getSerializedSize();
    }

    // ========== Decoding Methods ==========

    /**
     * Decodes a byte array to a tile model.
     * <p>
     * Coordinates are returned in their natural extent space (0 to extent-1) as stored in the MVT data.
     *
     * @param data the encoded MVT data
     * @return the decoded tile model
     * @throws IOException if decoding fails
     */
    public VectorTile decode(byte[] data) throws IOException {
        return decode(ByteBuffer.wrap(data));
    }
    /**
     * Decodes a ByteBuffer to a tile model.
     * <p>
     * Coordinates are returned in their natural extent space (0 to extent-1) as stored in the MVT data.
     *
     * @param data the encoded MVT data
     * @return the decoded tile model
     * @throws IOException if decoding fails
     */
    public VectorTile decode(ByteBuffer data) throws IOException {
        VectorTileProto.Tile tile = VectorTileProto.Tile.parseFrom(data);
        return new MvtTile(tile);
    }

    /**
     * Decodes an InputStream to a tile model.
     * <p>
     * Coordinates are returned in their natural extent space (0 to extent-1) as stored in the MVT data.
     * <p>
     * <strong>Warning:</strong> The input stream is consumed fully due to protocol buffer requirements.
     *
     * @param data the encoded MVT data stream
     * @return the decoded tile model
     * @throws IOException if decoding fails
     */
    public VectorTile decode(InputStream data) throws IOException {
        VectorTileProto.Tile tile = VectorTileProto.Tile.parseFrom(data);
        return new MvtTile(tile);
    }

    // ========== Private Helper Methods ==========

    /**
     * Extracts the protobuf tile from a model tile.
     *
     * @param tile the model tile
     * @return the protobuf tile
     */
    private VectorTileProto.Tile getProto(VectorTile tile) {
        if (tile instanceof MvtTile mvt) {
            return mvt.tileProto();
        }
        // TODO: build an MvtTile from a generic Tile
        throw new UnsupportedOperationException("Generic Tile to MVT conversion not yet implemented");
    }

    /**
     * Creates a new configurable geometry reader for advanced geometry processing.
     * <p>
     * The returned {@link GeometryReader} can be customized with:
     * <ul>
     * <li>{@link GeometryReader#withGeometryFactory(GeometryFactory)} - specify a custom JTS GeometryFactory</li>
     * <li>{@link GeometryReader#withGeometryTransformation(UnaryOperator)} - apply coordinate transformations</li>
     * </ul>
     *
     * <h3>Usage Examples</h3>
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
