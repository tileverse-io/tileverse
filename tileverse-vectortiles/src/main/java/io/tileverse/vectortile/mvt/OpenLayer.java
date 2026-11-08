/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * The buffers of the layer under construction: its features as written, and the tables of its keys and values.
 *
 * <p>A layer goes into the tile at its end: its key and value tables come after its features in the wire format, and
 * the tile writes the length of the layer before its bytes. A tile builder writes one layer at a time, and one instance
 * serves its layers in turn, cleared between them. Clearing keeps the arrays of the buffers for the next layer.
 */
final class OpenLayer {

    /** The version of the vector tile specification followed by the written layers. */
    static final int VERSION = 2;

    /** The field number of the features of the Layer message. */
    private static final int LAYER_FEATURES = 2;

    /** The field number of the geometry of the Feature message, also written by {@link MvtFeature}. */
    static final int FEATURE_GEOMETRY_FIELD = 4;

    // Field numbers of the Tile, Layer, Feature and Value messages of the vector tile schema
    private static final int TILE_LAYERS = 3;
    private static final int LAYER_NAME = 1;
    private static final int LAYER_KEYS = 3;
    private static final int LAYER_VALUES = 4;
    private static final int LAYER_EXTENT = 5;
    private static final int LAYER_VERSION = 15;
    private static final int FEATURE_ID = 1;
    private static final int FEATURE_TAGS = 2;
    private static final int FEATURE_TYPE = 3;
    private static final int VALUE_STRING = 1;
    private static final int VALUE_FLOAT = 2;
    private static final int VALUE_DOUBLE = 3;
    private static final int VALUE_UINT = 5;
    private static final int VALUE_SINT = 6;
    private static final int VALUE_BOOL = 7;

    /**
     * The entry of a key or a value in its LinkedHashMap, for the heap size: the hash int, and references to the key,
     * the value, the next entry of its bucket, and the entries before and after it.
     */
    static final long TABLE_ENTRY = HeapSize.object(Integer.BYTES + 5 * HeapSize.REFERENCE);

    // The keys and values of the layer, each with its index in its table. The tags of the features hold the indexes.
    private final Map<String, Integer> keys = new LinkedHashMap<>();
    private final Map<Object, Integer> values = new LinkedHashMap<>();

    // Created once: a lambda capturing this would allocate an object per attribute
    private final Function<String, Integer> newKeyIndex = this::addKey;
    private final Function<Object, Integer> newValueIndex = this::addValue;

    /** The heap held by the keys and values of the tables, with their entries, counted as they join the tables. */
    private long tablesHeapSize;

    private final WireWriter features = new WireWriter();

    // The fields written before and after the features, apart: the layer needs its length before its first byte
    private final WireWriter header = new WireWriter();
    private final WireWriter tables = new WireWriter();
    private final WireWriter encodedValue = new WireWriter();

    /**
     * Adds an attribute to the tags of a feature: the index of its key, then the index of its value. The key and the
     * value join the tables of the layer when new. A null value adds nothing. The tags take the two indexes once both
     * are known: a value failing to join its table leaves the tags as they were, and its key unused in the keys table.
     *
     * @throws NullPointerException if the key is null and the value is not
     */
    void addTag(IntArray tags, String key, Object value) {
        if (value != null) {
            int keyIndex = keyIndex(key);
            int valueIndex = valueIndex(value);
            tags.add(keyIndex);
            tags.add(valueIndex);
        }
    }

    /** Returns the index of the key in the keys table, added at the end when new. */
    private int keyIndex(String key) {
        Objects.requireNonNull(key, "The name of an attribute cannot be null");
        return keys.computeIfAbsent(key, newKeyIndex);
    }

    /** Returns the index of a key joining the keys table, and counts its heap. */
    private int addKey(String key) {
        tablesHeapSize += TABLE_ENTRY + HeapSize.string(key);
        return keys.size();
    }

    /** Returns the index of the value in the values table, added at the end when new. */
    private int valueIndex(Object value) {
        return values.computeIfAbsent(value, newValueIndex);
    }

    /** Returns the index of a value joining the values table, and counts its heap. */
    private int addValue(Object value) {
        tablesHeapSize += TABLE_ENTRY + HeapSize.value(value);
        return values.size();
    }

    /**
     * Writes a feature with the geometry commands of the encoder.
     *
     * @param hasId whether the feature has an id
     * @param id the id, written as an unsigned 64-bit value: a negative long stands for an id of 2^63 or above
     * @param tags the indexes of the keys and values of the feature in the tables of the layer
     * @param geometry the encoder holding the type and the commands of the geometry
     */
    void writeFeature(boolean hasId, long id, IntArray tags, CommandEncoder geometry) {
        IntArray commands = geometry.commands();
        int geometryLength = WireWriter.packedVarintsFieldLength(FEATURE_GEOMETRY_FIELD, commands);
        startFeature(hasId, id, tags, geometry.type(), geometryLength);
        features.writePackedVarints(FEATURE_GEOMETRY_FIELD, commands);
    }

    /**
     * Writes a feature with the type and the geometry field of a decoded feature, copied from the bytes of its tile.
     *
     * @param hasId whether the feature has an id
     * @param id the id, written as an unsigned 64-bit value: a negative long stands for an id of 2^63 or above
     * @param tags the indexes of the keys and values of the feature in the tables of the layer
     * @param geometry the decoded feature holding the geometry
     */
    void writeFeature(boolean hasId, long id, IntArray tags, MvtFeature geometry) {
        startFeature(hasId, id, tags, geometry.geometryType(), geometry.geometryFieldLength());
        geometry.writeGeometryField(features);
    }

    /**
     * Writes the tag and the length of a feature, then its fields before the geometry: the feature goes straight into
     * the features of the layer, and its length comes first.
     */
    private void startFeature(boolean hasId, long id, IntArray tags, GeomType type, int geometryLength) {
        int length = lengthBeforeGeometry(hasId, id, tags, type) + geometryLength;
        features.writeFieldHeader(LAYER_FEATURES, length);
        writeFieldsBeforeGeometry(hasId, id, tags, type);
    }

    /** The number of bytes written by {@link #writeFieldsBeforeGeometry}. */
    private static int lengthBeforeGeometry(boolean hasId, long id, IntArray tags, GeomType type) {
        int length = WireWriter.packedVarintsFieldLength(FEATURE_TAGS, tags);
        if (hasId) {
            length += WireWriter.varintFieldLength(FEATURE_ID, id);
        }
        return length + WireWriter.varintFieldLength(FEATURE_TYPE, Integer.toUnsignedLong(type.number()));
    }

    /**
     * Writes the fields in the order of their field numbers, as protobuf writes them: the geometry comes last. The
     * specification wants a type field in each feature, also for the type UNKNOWN of a merged feature.
     */
    private void writeFieldsBeforeGeometry(boolean hasId, long id, IntArray tags, GeomType type) {
        if (hasId) {
            features.writeVarint64(FEATURE_ID, id);
        }
        features.writePackedVarints(FEATURE_TAGS, tags);
        features.writeVarint32(FEATURE_TYPE, type.number());
    }

    /** The features of the layer, each a field of the Layer message, as written by {@link #writeFeature}. */
    WireWriter features() {
        return features;
    }

    /** Whether a feature was written since the layer was cleared. */
    boolean hasFeatures() {
        return !features.isEmpty();
    }

    /**
     * Writes the layer into the tile as one field of the Tile message. The fields of the layer follow the order of
     * their field numbers, as protobuf writes them: name, features, keys, values, extent and version 2.
     */
    void writeInto(WireWriter tile, String name, int extent) {
        header.clear();
        header.writeString(LAYER_NAME, name);
        writeTables(extent);
        tile.writeFieldHeader(TILE_LAYERS, header.size() + features.size() + tables.size());
        tile.append(header);
        tile.append(features);
        tile.append(tables);
    }

    private void writeTables(int extent) {
        tables.clear();
        for (String key : keys.keySet()) {
            tables.writeString(LAYER_KEYS, key);
        }
        for (Object value : values.keySet()) {
            encodedValue.clear();
            encodeValue(value, encodedValue);
            tables.writeMessage(LAYER_VALUES, encodedValue);
        }
        tables.writeVarint32(LAYER_EXTENT, extent);
        tables.writeVarint32(LAYER_VERSION, VERSION);
    }

    /** Writes the value in the field of the Value message for its type. */
    private static void encodeValue(Object value, WireWriter encodedValue) {
        if (value instanceof String string) {
            encodedValue.writeString(VALUE_STRING, string);
        } else if (value instanceof Integer integer) {
            encodedValue.writeVarint64(VALUE_SINT, encodeZigZag64(integer.intValue()));
        } else if (value instanceof Long long1) {
            encodedValue.writeVarint64(VALUE_SINT, encodeZigZag64(long1.longValue()));
        } else if (value instanceof UnsignedValue unsigned) {
            encodedValue.writeVarint64(VALUE_UINT, unsigned.bits());
        } else if (value instanceof Float float1) {
            encodedValue.writeFixed32(VALUE_FLOAT, Float.floatToRawIntBits(float1.floatValue()));
        } else if (value instanceof Double double1) {
            encodedValue.writeFixed64(VALUE_DOUBLE, Double.doubleToRawLongBits(double1.doubleValue()));
        } else if (value instanceof BigDecimal) {
            encodedValue.writeString(VALUE_STRING, value.toString());
        } else if (value instanceof Number number) {
            encodedValue.writeFixed64(VALUE_DOUBLE, Double.doubleToRawLongBits(number.doubleValue()));
        } else if (value instanceof Boolean boolean1) {
            encodedValue.writeVarint32(VALUE_BOOL, boolean1.booleanValue() ? 1 : 0);
        } else {
            encodedValue.writeString(VALUE_STRING, value.toString());
        }
    }

    // A sint64 field holds the zigzag encoding of the value: values of either sign near zero take few bytes
    private static long encodeZigZag64(long value) {
        return (value << 1) ^ (value >> 63);
    }

    /** Removes the features and the tables, for the next layer. The buffers keep their arrays. */
    void clear() {
        keys.clear();
        values.clear();
        tablesHeapSize = 0;
        features.clear();
    }

    /**
     * Estimates the heap memory held by the layer, in bytes: the arrays of its buffers, by capacity, and its keys and
     * values with their entries in the tables. The hash arrays of the tables and the boxed indexes are left out. A key
     * or a value is counted when it joins its table: a call costs no walk of the tables.
     */
    long heapSize() {
        return buffersHeapSize() + tablesHeapSize;
    }

    private long buffersHeapSize() {
        return bufferHeapSize(features)
                + bufferHeapSize(header)
                + bufferHeapSize(tables)
                + bufferHeapSize(encodedValue);
    }

    private static long bufferHeapSize(WireWriter buffer) {
        return HeapSize.array(Byte.BYTES, buffer.capacity());
    }

    /**
     * A uint value of 2^63 or above, as kept by the index of a decoded layer. The model has no unsigned long: it reads
     * the value as a negative long, as it reads the sint value of the same bits. The open layer writes it as a uint
     * value, apart from that long in its values table.
     *
     * @param bits the 64 bits of the value
     */
    record UnsignedValue(long bits) {}
}
