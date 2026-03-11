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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tileverse.parquet.CloseableIterator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBReader;

class GeoparquetFeatureReaderTest {

    private GeoparquetFeatureReader createReader() {
        SimpleFeatureTypeBuilder b = new SimpleFeatureTypeBuilder();
        b.setName("test");
        b.add("id", String.class);
        return new GeoparquetFeatureReader(b.buildFeatureType(), emptyIterator(), new WKBReader());
    }

    @Test
    void toTopLevelColumns_extractsTopLevelFromDotPaths() {
        assertThat(GeoparquetFeatureReader.toTopLevelColumns(Set.of("meta.label", "id", "", "   ", "geometry")))
                .containsExactlyInAnyOrder("meta", "id", "geometry");
    }

    @Test
    void toTopLevelColumns_emptyInputReturnsEmpty() {
        assertThat(GeoparquetFeatureReader.toTopLevelColumns(Set.of())).isEmpty();
    }

    @Test
    void normalizeAvroValue_handlesStringCharSequence() {
        GeoparquetFeatureReader reader = createReader();
        assertThat(reader.normalizeAvroValue("abc")).isEqualTo("abc");
    }

    @Test
    void normalizeAvroValue_handlesByteBuffer() {
        GeoparquetFeatureReader reader = createReader();
        ByteBuffer bb = ByteBuffer.wrap(new byte[] {9, 8, 7});
        assertThat((byte[]) reader.normalizeAvroValue(bb)).containsExactly(9, 8, 7);
    }

    @Test
    void normalizeAvroValue_handlesGenericFixed() {
        GeoparquetFeatureReader reader = createReader();
        Schema fixedSchema = Schema.createFixed("f", null, null, 4);
        GenericData.Fixed fixed = new GenericData.Fixed(fixedSchema, new byte[] {1, 2, 3, 4});
        assertThat((byte[]) reader.normalizeAvroValue(fixed)).containsExactly(1, 2, 3, 4);
    }

    @Test
    void normalizeAvroValue_handlesNestedMap() {
        GeoparquetFeatureReader reader = createReader();
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("k", ByteBuffer.wrap(new byte[] {5, 6}));
        Object normalizedMap = reader.normalizeAvroValue(nested);
        assertThat(normalizedMap).isInstanceOf(Map.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) normalizedMap;
        assertThat((byte[]) map.get("k")).containsExactly(5, 6);
    }

    @Test
    void normalizeAvroValue_handlesNull() {
        GeoparquetFeatureReader reader = createReader();
        assertThat(reader.normalizeAvroValue(null)).isNull();
    }

    @Test
    void convertValue_numericTypes() throws IOException {
        GeoparquetFeatureReader reader = createReader();
        assertThat(reader.convertValue(10L, Integer.class)).isEqualTo(10);
        assertThat(reader.convertValue(10, Long.class)).isEqualTo(10L);
        assertThat(reader.convertValue(10, Float.class)).isEqualTo(10.0f);
        assertThat(reader.convertValue(10, Double.class)).isEqualTo(10.0d);
        assertThat(reader.convertValue(true, Boolean.class)).isEqualTo(true);
        assertThat(reader.convertValue(1234, String.class)).isEqualTo("1234");
    }

    @Test
    void convertValue_temporalTypes() throws IOException {
        GeoparquetFeatureReader reader = createReader();
        assertThat(reader.convertValue(2, LocalDate.class)).isEqualTo(LocalDate.ofEpochDay(2));
        assertThat(reader.convertValue(1000L, Instant.class)).isEqualTo(Instant.ofEpochMilli(1000L));
    }

    @Test
    void convertValue_byteArrayPassthrough() throws IOException {
        GeoparquetFeatureReader reader = createReader();
        assertThat((byte[]) reader.convertValue(new byte[] {1, 9}, byte[].class))
                .containsExactly(1, 9);
    }

    @Test
    void convertValue_invalidWkbThrowsIOException() {
        GeoparquetFeatureReader reader = createReader();
        assertThatThrownBy(() -> reader.convertValue(new byte[] {0x01, 0x02}, Geometry.class))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to parse geometry WKB");
    }

    @Test
    void convertValue_nullReturnsNull() throws IOException {
        GeoparquetFeatureReader reader = createReader();
        assertThat(reader.convertValue(null, String.class)).isNull();
    }

    private static CloseableIterator<GenericRecord> emptyIterator() {
        return new CloseableIterator<>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public GenericRecord next() {
                throw new NoSuchElementException();
            }

            @Override
            public void close() {}
        };
    }
}
