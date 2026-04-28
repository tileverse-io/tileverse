/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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
package io.tileverse.geotools.parquet;

import io.tileverse.parquet.CloseableIterator;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.geotools.api.data.SimpleFeatureReader;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.feature.type.AttributeDescriptor;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.util.logging.Logging;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

/**
 * Streaming {@link SimpleFeatureReader} that converts Avro {@link GenericRecord}s from a Parquet source to GeoTools
 * {@link SimpleFeature}s.
 */
class GeoparquetFeatureReader implements SimpleFeatureReader {

    private static final Logger LOGGER = Logging.getLogger(GeoparquetFeatureReader.class);

    private final SimpleFeatureType featureType;
    private final CloseableIterator<GenericRecord> records;
    private final Set<String> wkbGeometryColumns;
    private final String typeName;
    private int featureId;

    GeoparquetFeatureReader(SimpleFeatureType featureType, CloseableIterator<GenericRecord> records) {
        this(featureType, records, Set.of());
    }

    GeoparquetFeatureReader(
            SimpleFeatureType featureType, CloseableIterator<GenericRecord> records, Set<String> wkbGeometryColumns) {
        this.featureType = featureType;
        this.records = records;
        this.wkbGeometryColumns = wkbGeometryColumns;
        this.typeName = featureType.getTypeName();
    }

    @Override
    public SimpleFeatureType getFeatureType() {
        return featureType;
    }

    @Override
    public boolean hasNext() throws IOException {
        return records.hasNext();
    }

    @Override
    public SimpleFeature next() throws IOException, IllegalArgumentException, NoSuchElementException {
        if (!records.hasNext()) {
            throw new NoSuchElementException();
        }
        GenericRecord record = records.next();
        return toSimpleFeature(record, featureId++);
    }

    @Override
    public void close() throws IOException {
        records.close();
    }

    private SimpleFeature toSimpleFeature(GenericRecord record, int fid) throws IOException {
        Map<String, Object> flat = new LinkedHashMap<>();
        flattenRecord("", record, flat);
        SimpleFeatureBuilder builder = new SimpleFeatureBuilder(featureType);
        for (AttributeDescriptor d : featureType.getAttributeDescriptors()) {
            String name = d.getLocalName();
            Class<?> binding = d.getType().getBinding();
            Object raw = flat.get(name);
            Object value;
            if (binding == byte[].class && raw instanceof byte[] bytes && wkbGeometryColumns.contains(name)) {
                value = tryDecodeWkb(bytes);
            } else {
                value = convertValue(raw, binding);
            }
            builder.set(name, value);
        }
        return builder.buildFeature(typeName + "." + fid);
    }

    private Object tryDecodeWkb(byte[] bytes) {
        try {
            return new WKBReader().read(bytes);
        } catch (ParseException e) {
            LOGGER.log(Level.FINE, "WKB decode failed for GeoParquet geometry column, keeping raw bytes", e);
            return bytes;
        }
    }

    void flattenRecord(String prefix, GenericRecord record, Map<String, Object> target) {
        record.getSchema().getFields().forEach(field -> {
            String fieldName = field.name();
            Object value = record.get(fieldName);
            String key = prefix.isEmpty() ? fieldName : prefix + "." + fieldName;
            if (value instanceof GenericRecord nested) {
                flattenRecord(key, nested, target);
            } else {
                target.put(key, normalizeAvroValue(value));
            }
        });
    }

    Object normalizeAvroValue(Object value) {
        if (value == null) return null;
        if (value instanceof GenericData.EnumSymbol e) return e.toString();
        if (value instanceof CharSequence chars) return chars.toString();
        if (value instanceof ByteBuffer buf) {
            ByteBuffer copy = buf.duplicate();
            byte[] bytes = new byte[copy.remaining()];
            copy.get(bytes);
            return bytes;
        }
        if (value instanceof GenericFixed fixed) return fixed.bytes();
        if (value instanceof List<?> list)
            return list.stream().map(this::normalizeAvroValue).toList();
        if (value instanceof Map<?, ?> map) {
            Map<Object, Object> normalized = new LinkedHashMap<>();
            map.forEach((k, v) -> normalized.put(k, normalizeAvroValue(v)));
            return normalized;
        }
        return value;
    }

    Object convertValue(Object value, Class<?> binding) throws IOException {
        if (value == null) return null;
        if (Geometry.class.isAssignableFrom(binding)) {
            if (value instanceof byte[] bytes) {
                try {
                    return new WKBReader().read(bytes);
                } catch (ParseException e) {
                    throw new IOException("Failed to parse geometry WKB", e);
                }
            }
            return value;
        }
        if (binding == byte[].class && value instanceof byte[] bytes) return bytes;
        if (binding == String.class) return value.toString();
        if (binding == Integer.class && value instanceof Number n) return n.intValue();
        if (binding == Long.class && value instanceof Number n) return n.longValue();
        if (binding == Float.class && value instanceof Number n) return n.floatValue();
        if (binding == Double.class && value instanceof Number n) return n.doubleValue();
        if (binding == Boolean.class && value instanceof Boolean b) return b;
        if (binding == BigDecimal.class && value instanceof BigDecimal bd) return bd;
        if (binding == UUID.class && value instanceof String s) return UUID.fromString(s);
        if (binding == LocalDate.class && value instanceof Number n) return LocalDate.ofEpochDay(n.longValue());
        if (binding == LocalTime.class && value instanceof Integer n)
            return LocalTime.ofNanoOfDay(n.longValue() * 1_000_000L);
        if (binding == LocalTime.class && value instanceof Long n) return LocalTime.ofNanoOfDay(n * 1000L);
        if (binding == Instant.class && value instanceof Number n) return Instant.ofEpochMilli(n.longValue());
        if (binding == LocalDateTime.class && value instanceof Number n)
            return Instant.ofEpochMilli(n.longValue()).atOffset(ZoneOffset.UTC).toLocalDateTime();
        return value;
    }

    static Set<String> toTopLevelColumns(Set<String> requestedColumns) {
        if (requestedColumns.isEmpty()) {
            return Set.of();
        }
        Set<String> cols = new LinkedHashSet<>();
        for (String c : requestedColumns) {
            if (c == null || c.isBlank()) {
                continue;
            }
            int dot = c.indexOf('.');
            cols.add(dot > 0 ? c.substring(0, dot) : c);
        }
        return cols;
    }
}
