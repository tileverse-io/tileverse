/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import io.tileverse.vectortile.mvt.GeometryPacker.MalformedGeometryException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * Index of a layer of a vector tile, read from the tile bytes: the name, the extent, the keys and values tables, the
 * start and end of each feature, and the geometries of the features. With this index, a tile can read its features
 * without building protobuf objects.
 *
 * <p>Indexing validates the whole tile with the rules of protobuf's parser: it fails on the tiles rejected by protobuf,
 * and indexes the tiles accepted by protobuf. Keys and values are decoded. Features are checked, and their bounds are
 * kept. Their geometries are decoded into {@link PackedGeometries}, and a malformed geometry is packed as empty, unless
 * indexing fails on malformed geometry.
 *
 * @see <a href="https://github.com/mapbox/vector-tile-spec/tree/master/2.1">Vector tile specification 2.1</a>
 */
final class LayerIndex {

    /** The extent of a layer without an extent field, from the default of the vector tile schema. */
    private static final int DEFAULT_EXTENT = 4096;

    // Depths of the messages, counted against protobuf's recursion limit
    private static final int TILE_DEPTH = 0;
    private static final int LAYER_DEPTH = 1;
    static final int FEATURE_AND_VALUE_DEPTH = 2;

    // Tags of the fields of the vector tile schema: the field number in the high bits, the wire type in the low
    // bits. As in protobuf, a known field number with another wire type makes an unknown field.
    private static final int WIRE_TYPE_BITS = 3;
    private static final int TILE_LAYER = 3 << WIRE_TYPE_BITS | WireReader.LENGTH_DELIMITED;
    private static final int LAYER_NAME = 1 << WIRE_TYPE_BITS | WireReader.LENGTH_DELIMITED;
    private static final int LAYER_FEATURE = 2 << WIRE_TYPE_BITS | WireReader.LENGTH_DELIMITED;
    private static final int LAYER_KEY = 3 << WIRE_TYPE_BITS | WireReader.LENGTH_DELIMITED;
    private static final int LAYER_VALUE = 4 << WIRE_TYPE_BITS | WireReader.LENGTH_DELIMITED;
    private static final int LAYER_EXTENT = 5 << WIRE_TYPE_BITS | WireReader.VARINT;
    private static final int LAYER_VERSION = 15 << WIRE_TYPE_BITS | WireReader.VARINT;
    static final int FEATURE_ID = 1 << WIRE_TYPE_BITS | WireReader.VARINT;
    static final int FEATURE_TAGS = 2 << WIRE_TYPE_BITS | WireReader.LENGTH_DELIMITED;
    static final int FEATURE_TYPE = 3 << WIRE_TYPE_BITS | WireReader.VARINT;
    static final int FEATURE_GEOMETRY = 4 << WIRE_TYPE_BITS | WireReader.LENGTH_DELIMITED;
    // Protobuf also reads the packed fields of a feature written as single varints
    static final int FEATURE_UNPACKED_TAGS = 2 << WIRE_TYPE_BITS | WireReader.VARINT;
    static final int FEATURE_UNPACKED_GEOMETRY = 4 << WIRE_TYPE_BITS | WireReader.VARINT;
    private static final int VALUE_STRING = 1 << WIRE_TYPE_BITS | WireReader.LENGTH_DELIMITED;
    private static final int VALUE_FLOAT = 2 << WIRE_TYPE_BITS | WireReader.FIXED32;
    private static final int VALUE_DOUBLE = 3 << WIRE_TYPE_BITS | WireReader.FIXED64;
    private static final int VALUE_INT = 4 << WIRE_TYPE_BITS | WireReader.VARINT;
    private static final int VALUE_UINT = 5 << WIRE_TYPE_BITS | WireReader.VARINT;
    private static final int VALUE_SINT = 6 << WIRE_TYPE_BITS | WireReader.VARINT;
    private static final int VALUE_BOOL = 7 << WIRE_TYPE_BITS | WireReader.VARINT;

    /**
     * The bytes of the fields below, for the heap size: six references (name, keys, values, featureStarts, featureEnds,
     * geometries) and an int (extent). A new field must be counted here.
     */
    private static final int FIELD_BYTES = 6 * HeapSize.REFERENCE + Integer.BYTES;

    private final String name;
    private final int extent;
    private final String[] keys;
    private final Object[] values;
    private final int[] featureStarts;
    private final int[] featureEnds;
    private final PackedGeometries geometries;

    private LayerIndex(
            String name,
            int extent,
            String[] keys,
            Object[] values,
            int[] featureStarts,
            int[] featureEnds,
            PackedGeometries geometries) {
        this.name = name;
        this.extent = extent;
        this.keys = keys;
        this.values = values;
        this.featureStarts = featureStarts;
        this.featureEnds = featureEnds;
        this.geometries = geometries;
    }

    /**
     * Indexes the layers of a tile, in tile order.
     *
     * @param failOnMalformedGeometry whether a malformed geometry fails the tile, or is packed as empty
     * @throws IOException if protobuf's parser rejects the tile: on a wire error, or on a layer without its name or its
     *     version. Also on a malformed geometry, when it fails the tile. The cause is a {@link MalformedTileException}
     *     naming the problem and its offset.
     */
    static LayerIndex[] indexTile(byte[] tile, boolean failOnMalformedGeometry) throws IOException {
        try {
            return indexLayers(tile, failOnMalformedGeometry);
        } catch (MalformedTileException e) {
            throw new IOException("Malformed vector tile. " + e.getMessage(), e);
        }
    }

    // The layers share one packer: it keeps its buffers from one layer to the next
    private static LayerIndex[] indexLayers(byte[] tile, boolean failOnMalformedGeometry) {
        List<LayerIndex> layers = new ArrayList<>();
        GeometryPacker packer = new GeometryPacker();
        WireReader tileReader = new WireReader(tile, 0, tile.length, TILE_DEPTH);
        while (tileReader.hasRemaining()) {
            int tag = tileReader.readTag();
            if (tag == TILE_LAYER) {
                layers.add(indexLayer(tile, tileReader, packer, failOnMalformedGeometry));
            } else {
                tileReader.skipField(tag);
            }
        }
        return layers.toArray(new LayerIndex[0]);
    }

    private static LayerIndex indexLayer(
            byte[] tile, WireReader tileReader, GeometryPacker packer, boolean failOnMalformedGeometry) {
        int start = skipContent(tileReader);
        int end = tileReader.position();
        return new LayerReader(tile, start, end, packer, failOnMalformedGeometry).read();
    }

    /** Moves the reader past the content of a length-delimited field, and returns the offset of its first byte. */
    private static int skipContent(WireReader reader) {
        int length = reader.readLength();
        int start = reader.position();
        reader.skip(length);
        return start;
    }

    /** Reads a geometry type. As in protobuf, a number outside the GeomType enum keeps the previous type. */
    static GeomType readGeomType(WireReader reader, GeomType previous) {
        GeomType type = GeomType.forNumber(reader.readVarint32());
        return type == null ? previous : type;
    }

    /**
     * Moves the reader past a run of varints of the tags or the geometry commands of a feature, right after the tag of
     * its field, and returns the offset of the run. A packed field holds a run of varints, and must end on a complete
     * varint. A field written unpacked holds one varint.
     */
    static int skipRun(WireReader reader, int tag) {
        if (tag == FEATURE_TAGS || tag == FEATURE_GEOMETRY) {
            return reader.skipPackedVarints();
        }
        int varintStart = reader.position();
        reader.readVarint32();
        return varintStart;
    }

    /**
     * Copies the varints of a repeated field of a feature into one array. The field may come as several runs, and
     * protobuf reads them in tile order, as one list. Each run ends with a complete varint, and the copy needs no
     * decoding.
     *
     * @param packedTag the tag of the field written packed
     * @param unpackedTag the tag of the field written as single varints
     */
    static byte[] copyRuns(byte[] tile, int featureStart, int featureEnd, int packedTag, int unpackedTag) {
        ByteArrayOutputStream runs = new ByteArrayOutputStream();
        WireReader reader = new WireReader(tile, featureStart, featureEnd, FEATURE_AND_VALUE_DEPTH);
        while (reader.hasRemaining()) {
            int tag = reader.readTag();
            if (tag == packedTag || tag == unpackedTag) {
                int runStart = skipRun(reader, tag);
                runs.write(tile, runStart, reader.position() - runStart);
            } else {
                reader.skipField(tag);
            }
        }
        return runs.toByteArray();
    }

    String name() {
        return name;
    }

    /** The width and height of the tile in the coordinates of the layer: 4096 when the layer has no extent field. */
    int extent() {
        return extent;
    }

    /** The distinct keys of the layer, copied into a new set at each call. */
    Set<String> attributeNames() {
        return Set.copyOf(Arrays.asList(keys));
    }

    /**
     * The key at {@code index} in the keys table. The tags of a feature point into this table.
     *
     * @throws IllegalArgumentException if the table has no key at this index, as with the tags of a malformed feature
     */
    String key(int index) {
        requireIndex(index, keys.length, "keys");
        return keys[index];
    }

    /**
     * The value at {@code index} in the values table: a Boolean, Double, Float, Long or String, or null for a value
     * without a known field.
     *
     * @throws IllegalArgumentException if the table has no value at this index, as with the tags of a malformed feature
     */
    Object value(int index) {
        requireIndex(index, values.length, "values");
        return values[index];
    }

    private void requireIndex(int index, int count, String table) {
        if (index < 0 || index >= count) {
            throw new IllegalArgumentException(
                    "Index " + index + " is outside the " + count + " " + table + " of layer " + name);
        }
    }

    int featureCount() {
        return featureStarts.length;
    }

    /** The offset of the first byte of the feature message in the tile. */
    int featureStart(int feature) {
        return featureStarts[feature];
    }

    /** The offset past the last byte of the feature message in the tile. */
    int featureEnd(int feature) {
        return featureEnds[feature];
    }

    /** The geometries of the features, in feature order. A malformed geometry has no parts. */
    PackedGeometries geometries() {
        return geometries;
    }

    /**
     * Estimates the heap memory held by this index, in bytes: the index object, its name, its keys and values tables
     * with their entries, its feature offsets and its packed geometries. Each call computes the estimate again.
     */
    long heapSize() {
        return HeapSize.object(FIELD_BYTES)
                + HeapSize.string(name)
                + keysHeapSize()
                + valuesHeapSize()
                + featureOffsetsHeapSize()
                + geometries.heapSize();
    }

    private long keysHeapSize() {
        long size = HeapSize.array(HeapSize.REFERENCE, keys.length);
        for (String key : keys) {
            size += HeapSize.string(key);
        }
        return size;
    }

    private long valuesHeapSize() {
        long size = HeapSize.array(HeapSize.REFERENCE, values.length);
        for (Object value : values) {
            size += HeapSize.value(value);
        }
        return size;
    }

    private long featureOffsetsHeapSize() {
        long starts = HeapSize.array(Integer.BYTES, featureStarts.length);
        long ends = HeapSize.array(Integer.BYTES, featureEnds.length);
        return starts + ends;
    }

    /**
     * Reads the fields of one layer in two passes. The first pass counts the features, keys and values, and the second
     * fills arrays of exact size, and packs the geometry of each feature.
     */
    private static final class LayerReader {

        // Positions in valueFields. A valid value has one field. A value with several decodes to the first present in
        // this order, as in earlier releases.
        private static final int BOOL = 0;
        private static final int DOUBLE = 1;
        private static final int FLOAT = 2;
        private static final int INT = 3;
        private static final int SINT = 4;
        private static final int UINT = 5;
        private static final int STRING = 6;
        private static final int VALUE_FIELD_COUNT = 7;

        private final byte[] tile;
        private final int start;
        private final int end;
        private final WireReader fieldReader;

        // Reads the features and the values, reset for each one: checking a feature allocates nothing
        private final WireReader nestedReader;

        // The last occurrence of each field of the value being read
        private final Object[] valueFields = new Object[VALUE_FIELD_COUNT];

        private final GeometryPacker packer;
        private final boolean failOnMalformedGeometry;

        private String name;
        private int extent = DEFAULT_EXTENT;
        private boolean hasVersion;
        private String[] keys;
        private Object[] values;
        private int[] featureStarts;
        private int[] featureEnds;
        private int keysRead;
        private int valuesRead;
        private int featuresRead;

        // The type of the feature being read, the bounds of the first run of its geometry commands, and their runs
        private GeomType featureType;
        private int commandsStart;
        private int commandsEnd;
        private int commandRuns;

        // The first malformed geometry of the layer, reported once the name of the layer is known
        private MalformedGeometry malformedGeometry;

        LayerReader(byte[] tile, int start, int end, GeometryPacker packer, boolean failOnMalformedGeometry) {
            this.tile = tile;
            this.start = start;
            this.end = end;
            this.fieldReader = new WireReader(tile, start, end, LAYER_DEPTH);
            this.nestedReader = new WireReader(tile, start, end, FEATURE_AND_VALUE_DEPTH);
            this.packer = packer;
            this.failOnMalformedGeometry = failOnMalformedGeometry;
        }

        LayerIndex read() {
            allocateArrays();
            readFields();
            requireNameAndVersion();
            requireWellFormedGeometries();
            PackedGeometries geometries = packer.finishLayer();
            return new LayerIndex(name, extent, keys, values, featureStarts, featureEnds, geometries);
        }

        private void allocateArrays() {
            int featureCount = 0;
            int keyCount = 0;
            int valueCount = 0;
            while (fieldReader.hasRemaining()) {
                int tag = fieldReader.readTag();
                switch (tag) {
                    case LAYER_FEATURE -> featureCount++;
                    case LAYER_KEY -> keyCount++;
                    case LAYER_VALUE -> valueCount++;
                    default -> {
                        // The second pass reads the other fields
                    }
                }
                fieldReader.skipField(tag);
            }
            featureStarts = new int[featureCount];
            featureEnds = new int[featureCount];
            keys = new String[keyCount];
            values = new Object[valueCount];
            packer.startLayer(featureCount);
        }

        // Name, extent and version keep their last occurrence, as in protobuf
        private void readFields() {
            fieldReader.reset(start, end);
            while (fieldReader.hasRemaining()) {
                int tag = fieldReader.readTag();
                switch (tag) {
                    case LAYER_NAME -> name = fieldReader.readString();
                    case LAYER_FEATURE -> addFeature();
                    case LAYER_KEY -> addKey();
                    case LAYER_VALUE -> addValue();
                    case LAYER_EXTENT -> extent = fieldReader.readVarint32();
                    case LAYER_VERSION -> readVersion();
                    default -> fieldReader.skipField(tag);
                }
            }
        }

        private void addFeature() {
            int featureStart = skipContent(fieldReader);
            int featureEnd = fieldReader.position();
            checkFeature(featureStart, featureEnd);
            packGeometry(featureStart, featureEnd);
            featureStarts[featuresRead] = featureStart;
            featureEnds[featuresRead] = featureEnd;
            featuresRead++;
        }

        /**
         * Checks a feature as protobuf's parser does, and finds its type and its geometry commands: the feature reads
         * its other fields when asked. Skipping a field reads the bytes read by its decoder, and fails where the
         * decoder fails. A packed run of tags or geometry commands must also end on a complete varint.
         */
        private void checkFeature(int featureStart, int featureEnd) {
            featureType = GeomType.UNKNOWN;
            commandsStart = 0;
            commandsEnd = 0;
            commandRuns = 0;
            nestedReader.reset(featureStart, featureEnd);
            while (nestedReader.hasRemaining()) {
                int tag = nestedReader.readTag();
                switch (tag) {
                    case FEATURE_TYPE -> featureType = readGeomType(nestedReader, featureType);
                    case FEATURE_TAGS -> nestedReader.skipPackedVarints();
                    case FEATURE_GEOMETRY, FEATURE_UNPACKED_GEOMETRY -> addCommandsRun(skipRun(nestedReader, tag));
                    default -> nestedReader.skipField(tag);
                }
            }
        }

        private void addCommandsRun(int runStart) {
            if (commandRuns == 0) {
                commandsStart = runStart;
                commandsEnd = nestedReader.position();
            }
            commandRuns++;
        }

        /** Packs the geometry of the feature, from the bytes of its one run, or from a copy of its runs. */
        private void packGeometry(int featureStart, int featureEnd) {
            try {
                if (commandRuns > 1) {
                    byte[] runs = copyRuns(tile, featureStart, featureEnd, FEATURE_GEOMETRY, FEATURE_UNPACKED_GEOMETRY);
                    packer.pack(featureType, runs, 0, runs.length);
                } else {
                    packer.pack(featureType, tile, commandsStart, commandsEnd);
                }
            } catch (MalformedGeometryException e) {
                recordMalformedGeometry(featureStart, e);
            }
        }

        // The packer has packed the geometry as empty. A tile failing on malformed geometry fails on the first one.
        private void recordMalformedGeometry(int featureStart, MalformedGeometryException e) {
            if (failOnMalformedGeometry && malformedGeometry == null) {
                malformedGeometry = new MalformedGeometry(featureStart, featuresRead, e.getMessage());
            }
        }

        private void addKey() {
            keys[keysRead] = fieldReader.readString();
            keysRead++;
        }

        private void addValue() {
            int valueStart = skipContent(fieldReader);
            int valueEnd = fieldReader.position();
            values[valuesRead] = readValue(valueStart, valueEnd);
            valuesRead++;
        }

        private Object readValue(int valueStart, int valueEnd) {
            Arrays.fill(valueFields, null);
            nestedReader.reset(valueStart, valueEnd);
            while (nestedReader.hasRemaining()) {
                readValueField(nestedReader.readTag());
            }
            return firstValueField();
        }

        // Each field keeps its last occurrence, as in protobuf
        private void readValueField(int tag) {
            switch (tag) {
                case VALUE_BOOL -> valueFields[BOOL] = Boolean.valueOf(nestedReader.readVarint64() != 0);
                case VALUE_DOUBLE -> valueFields[DOUBLE] = Double.valueOf(readDouble());
                case VALUE_FLOAT -> valueFields[FLOAT] = Float.valueOf(readFloat());
                case VALUE_INT -> valueFields[INT] = Long.valueOf(nestedReader.readVarint64());
                case VALUE_SINT -> valueFields[SINT] = Long.valueOf(decodeZigZag(nestedReader.readVarint64()));
                case VALUE_UINT -> valueFields[UINT] = Long.valueOf(nestedReader.readVarint64());
                case VALUE_STRING -> valueFields[STRING] = nestedReader.readString();
                default -> nestedReader.skipField(tag);
            }
        }

        private double readDouble() {
            return Double.longBitsToDouble(nestedReader.readFixed64());
        }

        private float readFloat() {
            return Float.intBitsToFloat(nestedReader.readFixed32());
        }

        private static long decodeZigZag(long encoded) {
            return (encoded >>> 1) ^ -(encoded & 1);
        }

        private Object firstValueField() {
            for (Object field : valueFields) {
                if (field != null) {
                    return field;
                }
            }
            return null;
        }

        // The version must be present, and its value changes nothing in the index
        private void readVersion() {
            fieldReader.readVarint32();
            hasVersion = true;
        }

        // Protobuf rejects a message without its required fields
        private void requireNameAndVersion() {
            if (name == null) {
                throw malformedLayer("layer without its required name field");
            }
            if (!hasVersion) {
                throw malformedLayer("layer " + name + " without its required version field");
            }
        }

        private MalformedTileException malformedLayer(String problem) {
            return new MalformedTileException("At byte " + start + ": " + problem);
        }

        private void requireWellFormedGeometries() {
            if (malformedGeometry != null) {
                throw new MalformedTileException("At byte " + malformedGeometry.featureStart() + ", in feature "
                        + malformedGeometry.feature() + " of layer " + name + ": " + malformedGeometry.problem());
            }
        }

        /** A malformed geometry: the offset of its feature in the tile, the index of the feature, and the problem. */
        private record MalformedGeometry(int featureStart, int feature, String problem) {}
    }
}
