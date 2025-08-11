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
import io.tileverse.vectortile.model.Tile;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.locationtech.jts.geom.GeometryFactory;

/**
 * Codec for serializing and deserializing vector tiles between model objects and byte representations.
 * <p>
 * This class handles the conversion between the abstract {@link Tile} model and the MVT protobuf format,
 * without any coordinate transformations or scaling logic. The codec simply reads and writes data as-is.
 *
 * <h2>Design Philosophy</h2>
 * <p>
 * The codec has no configuration parameters - it simply converts between
 * the model and protobuf representations. Coordinates are always preserved in their natural extent space
 * as stored in the protobuf data.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * // Default codec with PackedCoordinateSequenceFactory
 * VectorTileCodec codec = new VectorTileCodec();
 *
 * // Codec with custom GeometryFactory
 * GeometryFactory customFactory = new GeometryFactory(new CoordinateArraySequenceFactory());
 * VectorTileCodec customCodec = new VectorTileCodec(customFactory);
 *
 * // Encode model to bytes
 * byte[] encoded = codec.encode(tile);
 *
 * // Decode bytes to model (coordinates in extent space using specified GeometryFactory)
 * Tile decoded = codec.decode(encoded);
 * }</pre>
 *
 * <h2>Thread Safety</h2>
 * <p>
 * This class is thread-safe and can be shared across multiple threads.
 *
 * @see VectorTileBuilder for creating tile models
 */
public class VectorTileCodec {

    private final GeometryFactory geometryFactory;

    /**
     * Create a new VectorTileCodec instance with default GeometryFactory.
     */
    public VectorTileCodec() {
        this(null);
    }

    /**
     * Create a new VectorTileCodec instance with a specific GeometryFactory.
     * <p>
     * The GeometryFactory will be used when decoding MVT data to create geometries.
     * If null, a default GeometryFactory will be used.
     *
     * @param geometryFactory the GeometryFactory to use for decoding geometries, or null for default
     */
    public VectorTileCodec(GeometryFactory geometryFactory) {
        this.geometryFactory = geometryFactory;
    }

    // ========== Encoding Methods ==========

    /**
     * Encodes a tile model to a byte array.
     * <p>
     * Use {@link #encode(Tile, OutputStream)} or {@link #encode(Tile, ByteBuffer)} for better performance.
     *
     * @param tile the tile model to encode
     * @return the encoded vector tile as a byte array
     */
    public byte[] encode(Tile tile) {
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
    public void encode(Tile tile, OutputStream outputStream) throws IOException {
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
    public void encode(Tile tile, ByteBuffer buffer) throws InsufficientBufferException, IOException {
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
    public int getSerializedSize(Tile tile) {
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
    public Tile decode(byte[] data) throws IOException {
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
    public Tile decode(ByteBuffer data) throws IOException {
        VectorTileProto.Tile tile = VectorTileProto.Tile.parseFrom(data);
        return new MvtTile(tile, geometryFactory);
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
    public Tile decode(InputStream data) throws IOException {
        VectorTileProto.Tile tile = VectorTileProto.Tile.parseFrom(data);
        return new MvtTile(tile, geometryFactory);
    }

    // ========== Private Helper Methods ==========

    /**
     * Extracts the protobuf tile from a model tile.
     *
     * @param tile the model tile
     * @return the protobuf tile
     */
    private VectorTileProto.Tile getProto(Tile tile) {
        if (tile instanceof MvtTile mvt) {
            return mvt.tile;
        }
        // TODO: build an MvtTile from a generic Tile
        throw new UnsupportedOperationException("Generic Tile to MVT conversion not yet implemented");
    }
}
