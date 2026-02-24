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

import io.tileverse.parquet.ParquetDataset;
import io.tileverse.parquet.RangeReaderInputFile;
import io.tileverse.rangereader.file.FileRangeReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.geotools.api.data.DataStore;
import org.geotools.api.data.Query;
import org.geotools.api.data.SimpleFeatureSource;
import org.geotools.api.data.Transaction;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.filter.Filter;
import org.geotools.api.filter.FilterFactory;
import org.geotools.api.filter.sort.SortBy;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.factory.CommonFactoryFinder;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

class GeoParquetFileDataStoreFactoryTest {

    private static final FilterFactory ff = CommonFactoryFinder.getFilterFactory(null);
    private static final String FIXTURE_RESOURCE = "geoparquet/sample-geoparquet.parquet";
    private static final String INVALID_GEOM_FIXTURE_RESOURCE = "geoparquet/invalid-geometry-geoparquet.parquet";
    private static final String OVERTURE_BUILDINGS_FIXTURE_RESOURCE = "geoparquet/rosario-center-buildings.parquet";
    private static final String DT_FEATURE_BUILDING_FIXTURE_RESOURCE = "geoparquet/dt-feature-building.parquet";

    private final GeoParquetFileDataStoreFactory factory = new GeoParquetFileDataStoreFactory();

    @Test
    void factoryMetadataAndValidationMethods() throws Exception {
        assertThat(factory.isAvailable()).isTrue();
        assertThat(factory.getFileExtensions()).containsExactly(".parquet");
        assertThat(factory.getParametersInfo()).hasSize(1);
        assertThat(factory.getParametersInfo()[0].key).isEqualTo("url");

        assertThat(factory.canProcess((URL) null)).isFalse();
        assertThat(factory.canProcess(new URL("file:/tmp/not-parquet.txt"))).isFalse();
        assertThatThrownBy(() -> factory.createDataStore(new URL("file:/tmp/not-parquet.txt")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unsupported URL");

        assertThatThrownBy(() -> factory.getTypeName(new URL("file:/tmp/not-parquet.txt")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unsupported URL");
    }

    @Test
    void typeNameFrom_handlesTypicalNames() throws Exception {
        assertThat(GeoParquetFileDataStore.typeNameFrom(new URL("file:/tmp/sample-geoparquet.parquet")))
                .isEqualTo("sample-geoparquet");
        assertThat(GeoParquetFileDataStore.typeNameFrom(new URL("file:/tmp/without_extension")))
                .isEqualTo("without_extension");
        assertThat(GeoParquetFileDataStore.typeNameFrom(new URL("file:/tmp/.parquet")))
                .isEqualTo(".parquet");
        assertThat(GeoParquetFileDataStore.typeNameFrom(new URL("http://example.com")))
                .isEqualTo("geoparquet");
    }

    @Test
    void createDataStoreFromMap_variants() throws Exception {
        URL url = fixtureUrl();
        DataStore store1 = factory.createDataStore(Map.of("url", url));
        store1.dispose();

        DataStore store2 = factory.createDataStore(Map.of("url", url.toString()));
        store2.dispose();

        assertThatThrownBy(() -> factory.createDataStore(Map.of()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Missing required parameter 'url'");
        assertThatThrownBy(() -> factory.createDataStore(Map.of("url", "not-a-url")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Invalid 'url' parameter");
    }

    @Test
    void canProcess_acceptsParquetUrl() throws Exception {
        URL url = fixtureUrl();
        assertThat(factory.canProcess(url)).isTrue();
        assertThat(factory.getTypeName(url)).isEqualTo("sample-geoparquet");
    }

    @Test
    void createNewDataStore_isRejected() {
        assertThatThrownBy(() -> factory.createNewDataStore(Map.of()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("read-only");
    }

    @Test
    void createDataStore_readsFeaturesAndGeometry() throws Exception {
        URL url = fixtureUrl();
        var dataStore = factory.createDataStore(url);
        try {
            SimpleFeatureType schema = dataStore.getSchema();
            assertThat(schema.getTypeName()).isEqualTo("sample-geoparquet");
            assertThat(schema.getDescriptor("id").getType().getBinding()).isEqualTo(String.class);
            assertThat(schema.getDescriptor("geometry").getType().getBinding()).isEqualTo(Geometry.class);

            SimpleFeatureSource featureSource = dataStore.getFeatureSource();
            SimpleFeatureCollection features = featureSource.getFeatures();
            assertThat(features.size()).isEqualTo(3);
            assertThat(features.isEmpty()).isFalse();
            assertThat(features.getSchema().getTypeName()).isEqualTo("sample-geoparquet");
            assertThat(features.getBounds()).isNull();
            assertThat(features.toArray()).hasSize(3);
            assertThat(features.sort(SortBy.NATURAL_ORDER).size()).isEqualTo(3);
            assertThat(features.subCollection(Filter.INCLUDE).size()).isEqualTo(3);
            assertThat(featureSource.getBounds()).isNull();
            assertThat(featureSource.getCount(Query.ALL)).isEqualTo(3);

            try (SimpleFeatureIterator it = features.features()) {
                assertThat(it.hasNext()).isTrue();
                SimpleFeature first = it.next();
                assertThat(first.getAttribute("id")).isEqualTo("fid-1");
                assertThat(first.getAttribute("value")).isEqualTo(10);
                assertThat(first.getAttribute("geometry")).isInstanceOf(Point.class);
            }

            try (SimpleFeatureIterator it = features.features()) {
                List<String> ids = new java.util.ArrayList<>();
                while (it.hasNext()) {
                    ids.add((String) it.next().getAttribute("id"));
                }
                assertThat(ids).containsExactly("fid-1", "fid-2", "fid-3");
                assertThat(it.hasNext()).isFalse();
                assertThatThrownBy(it::next).isInstanceOf(NoSuchElementException.class);
            }

            int iterated = 0;
            try (SimpleFeatureIterator it = features.features()) {
                while (it.hasNext()) {
                    it.next();
                    iterated++;
                }
            }
            assertThat(iterated).isEqualTo(3);
        } finally {
            dataStore.dispose();
        }
    }

    @Test
    void geoparquetMetadata_isPresentAndConsistent() throws Exception {
        try (FileRangeReader rangeReader =
                FileRangeReader.of(Paths.get(fixtureUrl().toURI()))) {
            ParquetDataset dataset = ParquetDataset.open(new RangeReaderInputFile(rangeReader));
            Map<String, String> metadata = dataset.getKeyValueMetadata();
            assertThat(metadata).containsKey("geo");
            assertThat(metadata.get("geo")).contains("\"primary_column\": \"geometry\"");
            assertThat(metadata.get("geo")).contains("\"encoding\": \"WKB\"");
        }
    }

    @Test
    void createDataStore_appliesFilterAndProjection() throws Exception {
        URL url = fixtureUrl();
        var dataStore = factory.createDataStore(url);
        try {
            SimpleFeatureSource featureSource = dataStore.getFeatureSource();
            Filter filter = ff.equals(ff.property("id"), ff.literal("fid-2"));
            Query query = new Query("sample-geoparquet", filter, new String[] {"id"});

            SimpleFeatureCollection filtered = featureSource.getFeatures(query);
            assertThat(filtered.size()).isEqualTo(1);

            try (SimpleFeatureIterator it = filtered.features()) {
                assertThat(it.hasNext()).isTrue();
                SimpleFeature feature = it.next();
                assertThat(feature.getAttribute("id")).isEqualTo("fid-2");
                // projected out; schema remains full, value should be null
                assertThat(feature.getAttribute("value")).isNull();
            }

            // Unsupported filter type should fall back to non-pushdown full scan
            Filter bbox = ff.bbox("geometry", -10, -10, 10, 10, "EPSG:4326");
            assertThat(featureSource.getFeatures(bbox).size()).isEqualTo(3);
        } finally {
            dataStore.dispose();
        }
    }

    @Test
    void datastoreMethods_coverReadOnlyAndClosedBranches() throws Exception {
        URL url = fixtureUrl();
        var dataStore = factory.createDataStore(url);
        try {
            assertThat(dataStore.toString()).isNotBlank();
            assertThat(dataStore.hashCode()).isNotZero();
            assertThat(dataStore.equals(dataStore)).isTrue();
            assertThat(dataStore.equals(new Object())).isFalse();

            assertThat(dataStore.getTypeNames()).containsExactly("sample-geoparquet");
            assertThat(dataStore.getNames()).hasSize(1);
            assertThat(dataStore.getSchema("sample-geoparquet")).isNotNull();
            assertThat(dataStore.getFeatureSource("sample-geoparquet")).isNotNull();

            assertThatThrownBy(() -> dataStore.getSchema("bad-type"))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Unknown type name");
            assertThatThrownBy(() -> dataStore.getFeatureSource("bad-type"))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Unknown type name");

            assertThatThrownBy(() -> dataStore.createSchema(dataStore.getSchema()))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("read-only");
            assertThatThrownBy(
                            () -> dataStore.getFeatureReader(new Query("sample-geoparquet"), Transaction.AUTO_COMMIT))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("Method not implemented");
            assertThatThrownBy(() ->
                            dataStore.getFeatureWriter("sample-geoparquet", Filter.INCLUDE, Transaction.AUTO_COMMIT))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("Method not implemented");
            assertThatThrownBy(() -> dataStore.getFeatureWriter("sample-geoparquet", Transaction.AUTO_COMMIT))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("Method not implemented");
            assertThatThrownBy(() -> dataStore.getFeatureWriterAppend("sample-geoparquet", Transaction.AUTO_COMMIT))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("Method not implemented");
            assertThatThrownBy(dataStore::getLockingManager)
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("Method not implemented");
        } finally {
            dataStore.dispose();
        }
        assertThatThrownBy(dataStore::getSchema).isInstanceOf(IOException.class).hasMessageContaining("closed");
    }

    @Test
    void featureSourceAndCollection_coverUnsupportedBranches() throws Exception {
        URL url = fixtureUrl();
        DataStore dataStore = factory.createDataStore(url);
        try {
            SimpleFeatureSource source = dataStore.getFeatureSource("sample-geoparquet");
            assertThatThrownBy(source::getInfo)
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("Method not implemented");
            assertThatThrownBy(source::getQueryCapabilities)
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("Method not implemented");
            assertThatThrownBy(source::getSupportedHints)
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("Method not implemented");
            assertThat(source.getBounds(Query.ALL)).isNull();

            SimpleFeatureCollection collection = source.getFeatures();
            assertThatThrownBy(collection::getID)
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("Method not implemented");
            assertThatThrownBy(() -> collection.contains("anything"))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("Method not implemented");
            assertThatThrownBy(() -> collection.containsAll(List.of()))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("Method not implemented");
            assertThat(collection.toArray(new Object[0])).hasSize(3);
            assertThat(collection.subCollection(Filter.INCLUDE)).isSameAs(collection);
            assertThat(collection.sort(SortBy.NATURAL_ORDER)).isSameAs(collection);

            InvocationHandler handler = Proxy.getInvocationHandler(collection);
            assertThat(handler.toString()).contains("size=");
            Method invoke = handler.getClass().getMethod("invoke", Object.class, Method.class, Object[].class);
            Object iterator =
                    invoke.invoke(handler, collection, FeatureCollectionProbe.class.getMethod("features"), null);
            Method hasNext = iterator.getClass().getMethod("hasNext");
            Method next = iterator.getClass().getMethod("next");
            Method close = iterator.getClass().getMethod("close");
            assertThat((Boolean) hasNext.invoke(iterator)).isTrue();
            assertThat(next.invoke(iterator)).isInstanceOf(SimpleFeature.class);
            close.invoke(iterator);
            invoke.invoke(handler, collection, FeatureCollectionProbe.class.getMethod("close"), null);
        } finally {
            dataStore.dispose();
        }
    }

    @Test
    void dataStoreHandler_reflectionCoversGetFileGetUrlAndDefaultBranch() throws Exception {
        URL url = fixtureUrl();
        DataStore dataStore = factory.createDataStore(url);
        try {
            InvocationHandler handler = Proxy.getInvocationHandler(dataStore);
            Method invoke = handler.getClass().getMethod("invoke", Object.class, Method.class, Object[].class);

            Object file = invoke.invoke(handler, dataStore, DataStoreProbe.class.getMethod("getFile"), null);
            assertThat(file).isNotNull();
            assertThat(file.toString()).contains("sample-geoparquet.parquet");

            Object readUrl = invoke.invoke(handler, dataStore, DataStoreProbe.class.getMethod("getURL"), null);
            assertThat(readUrl).isEqualTo(url);

            assertThatThrownBy(() -> invoke.invoke(handler, dataStore, DataStoreProbe.class.getMethod("ping"), null))
                    .hasRootCauseInstanceOf(UnsupportedOperationException.class)
                    .hasStackTraceContaining("Method not implemented");

            assertThatThrownBy(() -> invoke.invoke(
                            handler,
                            dataStore,
                            DataStoreBadSigProbe.class.getMethod("getSchema", int.class),
                            new Object[] {1}))
                    .hasRootCauseInstanceOf(IOException.class)
                    .hasStackTraceContaining("Unsupported getSchema signature");
            assertThatThrownBy(() -> invoke.invoke(
                            handler,
                            dataStore,
                            DataStoreBadSigProbe.class.getMethod("getFeatureSource", int.class),
                            new Object[] {1}))
                    .hasRootCauseInstanceOf(IOException.class)
                    .hasStackTraceContaining("Unsupported getFeatureSource signature");
        } finally {
            dataStore.dispose();
        }
    }

    @Test
    void featureSourceHandler_reflectionCoversDefaultAndListenerBranches() throws Exception {
        URL url = fixtureUrl();
        DataStore dataStore = factory.createDataStore(url);
        try {
            SimpleFeatureSource source = dataStore.getFeatureSource("sample-geoparquet");
            assertThat(source.getName().getLocalPart()).isEqualTo("sample-geoparquet");
            assertThat(source.getDataStore()).isNotNull();
            source.addFeatureListener(null);
            source.removeFeatureListener(null);

            InvocationHandler handler = Proxy.getInvocationHandler(source);
            Method invoke = handler.getClass().getMethod("invoke", Object.class, Method.class, Object[].class);
            assertThatThrownBy(() -> invoke.invoke(handler, source, FeatureSourceProbe.class.getMethod("ping"), null))
                    .hasRootCauseInstanceOf(UnsupportedOperationException.class)
                    .hasStackTraceContaining("Method not implemented");
        } finally {
            dataStore.dispose();
        }
    }

    @Test
    void queryCombinations_coverOpenRecordIteratorBranches() throws Exception {
        URL url = fixtureUrl();
        DataStore dataStore = factory.createDataStore(url);
        try {
            SimpleFeatureSource source = dataStore.getFeatureSource("sample-geoparquet");

            Query projectionOnly = new Query("sample-geoparquet", Filter.INCLUDE, new String[] {"id"});
            assertThat(source.getFeatures(projectionOnly).size()).isEqualTo(3);

            Query filterOnly = new Query("sample-geoparquet", ff.equals(ff.property("id"), ff.literal("fid-1")));
            assertThat(source.getFeatures(filterOnly).size()).isEqualTo(1);

            Query filterAndProjection = new Query(
                    "sample-geoparquet", ff.equals(ff.property("id"), ff.literal("fid-2")), new String[] {"id"});
            assertThat(source.getFeatures(filterAndProjection).size()).isEqualTo(1);
        } finally {
            dataStore.dispose();
        }
    }

    @Test
    void reflectiveInternals_coverConversionAndProjectionBranches() throws Exception {
        URL url = fixtureUrl();
        DataStore dataStore = factory.createDataStore(url);
        try {
            Object impl = unwrapDataStoreImpl(dataStore);

            Method resolveRequestedColumns =
                    impl.getClass().getDeclaredMethod("resolveRequestedColumns", Object[].class);
            resolveRequestedColumns.setAccessible(true);
            @SuppressWarnings("unchecked")
            Set<String> fromStringArray = (Set<String>)
                    resolveRequestedColumns.invoke(impl, (Object) new Object[] {new String[] {"id", "meta.label"}});
            assertThat(fromStringArray).containsExactlyInAnyOrder("id", "meta.label");

            Query q = new Query("sample-geoparquet", Filter.INCLUDE, new String[] {"value", "geometry"});
            @SuppressWarnings("unchecked")
            Set<String> fromQuery = (Set<String>) resolveRequestedColumns.invoke(impl, (Object) new Object[] {q});
            assertThat(fromQuery).containsExactlyInAnyOrder("value", "geometry");

            Method resolveFilter = impl.getClass().getDeclaredMethod("resolveFilter", Object[].class);
            resolveFilter.setAccessible(true);
            Filter onlyFid2 = ff.equals(ff.property("id"), ff.literal("fid-2"));
            assertThat((Filter) resolveFilter.invoke(impl, (Object) new Object[] {onlyFid2}))
                    .isEqualTo(onlyFid2);
            assertThat((Filter) resolveFilter.invoke(impl, (Object) new Object[] {q}))
                    .isEqualTo(Filter.INCLUDE);
            assertThat((Filter) resolveFilter.invoke(impl, (Object) new Object[] {new Object()}))
                    .isEqualTo(Filter.INCLUDE);

            Method toTopLevelColumns = impl.getClass().getDeclaredMethod("toTopLevelColumns", Set.class);
            toTopLevelColumns.setAccessible(true);
            @SuppressWarnings("unchecked")
            Set<String> topLevel =
                    (Set<String>) toTopLevelColumns.invoke(impl, Set.of("meta.label", "id", "", "   ", "geometry"));
            assertThat(topLevel).containsExactlyInAnyOrder("meta", "id", "geometry");

            Method normalizeAvroValue = impl.getClass().getDeclaredMethod("normalizeAvroValue", Object.class);
            normalizeAvroValue.setAccessible(true);
            assertThat(normalizeAvroValue.invoke(impl, "abc")).isEqualTo("abc");

            ByteBuffer bb = ByteBuffer.wrap(new byte[] {9, 8, 7});
            assertThat((byte[]) normalizeAvroValue.invoke(impl, bb)).containsExactly(9, 8, 7);

            Schema fixedSchema = Schema.createFixed("f", null, null, 4);
            GenericData.Fixed fixed = new GenericData.Fixed(fixedSchema, new byte[] {1, 2, 3, 4});
            assertThat((byte[]) normalizeAvroValue.invoke(impl, fixed)).containsExactly(1, 2, 3, 4);

            Map<String, Object> nested = new LinkedHashMap<>();
            nested.put("k", ByteBuffer.wrap(new byte[] {5, 6}));
            Object normalizedMap = normalizeAvroValue.invoke(impl, nested);
            assertThat(normalizedMap).isInstanceOf(Map.class);
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) normalizedMap;
            assertThat((byte[]) map.get("k")).containsExactly(5, 6);

            Method convertValue = impl.getClass().getDeclaredMethod("convertValue", Object.class, Class.class);
            convertValue.setAccessible(true);
            assertThat(convertValue.invoke(impl, 10L, Integer.class)).isEqualTo(10);
            assertThat(convertValue.invoke(impl, 10, Long.class)).isEqualTo(10L);
            assertThat(convertValue.invoke(impl, 10, Float.class)).isEqualTo(10.0f);
            assertThat(convertValue.invoke(impl, 10, Double.class)).isEqualTo(10.0d);
            assertThat(convertValue.invoke(impl, true, Boolean.class)).isEqualTo(true);
            assertThat(convertValue.invoke(impl, 1234, String.class)).isEqualTo("1234");
            assertThat(convertValue.invoke(impl, 2, LocalDate.class)).isEqualTo(LocalDate.ofEpochDay(2));
            assertThat(convertValue.invoke(impl, 1000L, Instant.class)).isEqualTo(Instant.ofEpochMilli(1000L));
            assertThat((byte[]) convertValue.invoke(impl, new byte[] {1, 9}, byte[].class))
                    .containsExactly(1, 9);

            assertThatThrownBy(() -> convertValue.invoke(impl, new byte[] {0x01, 0x02}, Geometry.class))
                    .hasCauseInstanceOf(IOException.class)
                    .hasStackTraceContaining("Failed to parse geometry WKB");
        } finally {
            dataStore.dispose();
        }
    }

    @Test
    void invalidGeometryFixture_throwsOnRead() throws Exception {
        URL url = invalidFixtureUrl();
        var dataStore = factory.createDataStore(url);
        try {
            SimpleFeatureSource featureSource = dataStore.getFeatureSource();
            assertThatThrownBy(() -> featureSource.getFeatures())
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Failed to parse geometry WKB");
        } finally {
            dataStore.dispose();
        }
    }

    @Test
    void overtureBuildings_schemaMatchesExpectedGeoParquetShape() throws Exception {
        URL url = overtureBuildingsFixtureUrl();
        DataStore dataStore = factory.createDataStore(url);
        try {
            assertThat(factory.getTypeName(url)).isEqualTo("rosario-center-buildings");
            SimpleFeatureType schema = dataStore.getSchema("rosario-center-buildings");
            assertThat(schema.getTypeName()).isEqualTo("rosario-center-buildings");

            assertThat(schema.getDescriptor("id").getType().getBinding()).isEqualTo(String.class);
            // This Overture subset has no GeoParquet GEOMETRY annotation, so geometry remains BYTE_ARRAY.
            assertThat(schema.getDescriptor("geometry").getType().getBinding()).isEqualTo(byte[].class);
            assertThat(schema.getDescriptor("bbox.xmin").getType().getBinding()).isEqualTo(Float.class);
            assertThat(schema.getDescriptor("bbox.xmax").getType().getBinding()).isEqualTo(Float.class);
            assertThat(schema.getDescriptor("version").getType().getBinding()).isEqualTo(Integer.class);
            assertThat(schema.getDescriptor("sources").getType().getBinding()).isEqualTo(List.class);
            assertThat(schema.getDescriptor("names.common").getType().getBinding())
                    .isEqualTo(Map.class);
            assertThat(schema.getDescriptor("has_parts").getType().getBinding()).isEqualTo(Boolean.class);
            assertThat(schema.getDescriptor("height").getType().getBinding()).isEqualTo(Double.class);
        } finally {
            dataStore.dispose();
        }
    }

    @Test
    void overtureBuildings_readsRealFeaturesAndMetadata() throws Exception {
        URL url = overtureBuildingsFixtureUrl();
        try (FileRangeReader rangeReader = FileRangeReader.of(Paths.get(url.toURI()))) {
            ParquetDataset dataset = ParquetDataset.open(new RangeReaderInputFile(rangeReader));
            Map<String, String> metadata = dataset.getKeyValueMetadata();
            assertThat(metadata).doesNotContainKey("geo");
        }

        DataStore dataStore = factory.createDataStore(url);
        try {
            SimpleFeatureSource featureSource = dataStore.getFeatureSource("rosario-center-buildings");
            SimpleFeatureCollection features = featureSource.getFeatures();
            assertThat(features.size()).isGreaterThan(0);

            int checked = 0;
            try (SimpleFeatureIterator it = features.features()) {
                while (it.hasNext() && checked < 10) {
                    SimpleFeature f = it.next();
                    Object geom = f.getAttribute("geometry");
                    assertThat(geom).isInstanceOf(byte[].class);
                    assertThat((byte[]) geom).isNotEmpty();
                    assertThat(f.getAttribute("version")).isInstanceOf(Integer.class);
                    checked++;
                }
            }
            assertThat(checked).isGreaterThan(0);
        } finally {
            dataStore.dispose();
        }
    }

    @Test
    void overtureBuildings_supportsFilterAndProjectionOnRealData() throws Exception {
        URL url = overtureBuildingsFixtureUrl();
        DataStore dataStore = factory.createDataStore(url);
        try {
            SimpleFeatureSource featureSource = dataStore.getFeatureSource("rosario-center-buildings");
            int versionValue;
            try (SimpleFeatureIterator it = featureSource.getFeatures().features()) {
                assertThat(it.hasNext()).isTrue();
                versionValue = ((Number) it.next().getAttribute("version")).intValue();
            }

            Filter byVersion = ff.equals(ff.property("version"), ff.literal(versionValue));
            Query query = new Query(
                    "rosario-center-buildings", byVersion, new String[] {"id", "version", "bbox.xmin", "geometry"});
            SimpleFeatureCollection filtered = featureSource.getFeatures(query);
            assertThat(filtered.size()).isGreaterThan(0);

            try (SimpleFeatureIterator it = filtered.features()) {
                int validated = 0;
                while (it.hasNext() && validated < 20) {
                    SimpleFeature f = it.next();
                    assertThat(((Number) f.getAttribute("version")).intValue()).isEqualTo(versionValue);
                    assertThat(f.getAttribute("geometry")).isInstanceOf(byte[].class);
                    validated++;
                }
                assertThat(validated).isGreaterThan(0);
            }

            Filter unsupportedSpatial = ff.bbox("geometry", -61.0, -33.0, -60.0, -32.0, "EPSG:4326");
            Query fallbackQuery = new Query("rosario-center-buildings", unsupportedSpatial, new String[] {"id"});
            assertThat(featureSource.getFeatures(fallbackQuery).size()).isGreaterThan(0);
        } finally {
            dataStore.dispose();
        }
    }

    @Test
    void dtFeatureBuilding_schemaAndTypeNameAreResolved() throws Exception {
        URL url = dtFeatureBuildingFixtureUrl();
        DataStore dataStore = factory.createDataStore(url);
        try {
            assertThat(factory.getTypeName(url)).isEqualTo("dt-feature-building");
            SimpleFeatureType schema = dataStore.getSchema("dt-feature-building");
            assertThat(schema).isNotNull();
            assertThat(schema.getTypeName()).isEqualTo("dt-feature-building");
            assertThat(schema.getAttributeCount()).isGreaterThan(5);
            assertThat(schema.getDescriptor("category")).isNotNull();
            assertThat(schema.getDescriptor("category").getType().getBinding()).isEqualTo(String.class);
            assertThat(schema.getDescriptor("feature_name")).isNotNull();
            assertThat(schema.getDescriptor("feature_name").getType().getBinding())
                    .isEqualTo(String.class);
            assertThat(schema.getDescriptor("geometry")).isNotNull();
            assertThat(schema.getDescriptor("geometry").getType().getBinding()).isIn(Geometry.class, byte[].class);
            assertThat(schema.getDescriptor("geometry_bbox.xmin")).isNotNull();
            assertThat(schema.getDescriptor("geometry_bbox.xmin").getType().getBinding())
                    .isEqualTo(Float.class);
            assertThat(schema.getDescriptor("geometry_bbox.ymin")).isNotNull();
            assertThat(schema.getDescriptor("geometry_bbox.xmax")).isNotNull();
            assertThat(schema.getDescriptor("geometry_bbox.ymax")).isNotNull();
        } finally {
            dataStore.dispose();
        }
    }

    @Test
    void dtFeatureBuilding_readsMetadataAndFeatures() throws Exception {
        URL url = dtFeatureBuildingFixtureUrl();
        try (FileRangeReader rangeReader = FileRangeReader.of(Paths.get(url.toURI()))) {
            ParquetDataset dataset = ParquetDataset.open(new RangeReaderInputFile(rangeReader));
            Map<String, String> metadata = dataset.getKeyValueMetadata();
            assertThat(metadata).isNotNull();
            assertThat(metadata).isNotEmpty();
            assertThat(metadata).containsKey("geo");
        }

        DataStore dataStore = factory.createDataStore(url);
        try {
            SimpleFeatureSource featureSource = dataStore.getFeatureSource("dt-feature-building");
            SimpleFeatureCollection features = featureSource.getFeatures();
            assertThat(features.size()).isGreaterThan(0);

            int checked = 0;
            try (SimpleFeatureIterator it = features.features()) {
                while (it.hasNext() && checked < 20) {
                    SimpleFeature f = it.next();
                    assertThat(f.getAttribute("category")).isInstanceOf(String.class);
                    assertThat(f.getAttribute("feature_name")).isInstanceOf(String.class);
                    assertThat(f.getAttribute("geometry_bbox.xmin")).isInstanceOf(Float.class);
                    assertThat(f.getAttribute("geometry_bbox.ymin")).isInstanceOf(Float.class);
                    checked++;
                }
            }
            assertThat(checked).isGreaterThan(0);
        } finally {
            dataStore.dispose();
        }
    }

    @Test
    void dtFeatureBuilding_supportsProjectionAndFilterVariants() throws Exception {
        URL url = dtFeatureBuildingFixtureUrl();
        DataStore dataStore = factory.createDataStore(url);
        try {
            SimpleFeatureSource featureSource = dataStore.getFeatureSource("dt-feature-building");

            String sampleFeatureName;
            String sampleCategory;
            try (SimpleFeatureIterator it = featureSource.getFeatures().features()) {
                sampleFeatureName = null;
                sampleCategory = null;
                while (it.hasNext() && sampleFeatureName == null) {
                    SimpleFeature f = it.next();
                    Object featureName = f.getAttribute("feature_name");
                    Object category = f.getAttribute("category");
                    if (featureName instanceof String fn && category instanceof String cat) {
                        sampleFeatureName = fn;
                        sampleCategory = cat;
                    }
                }
            }
            assertThat(sampleFeatureName).isNotBlank();
            assertThat(sampleCategory).isNotBlank();

            Query projected = new Query("dt-feature-building", Filter.INCLUDE, new String[] {
                "feature_name", "category", "geometry_bbox.xmin"
            });
            SimpleFeatureCollection projectedFeatures = featureSource.getFeatures(projected);
            assertThat(projectedFeatures.size()).isGreaterThan(0);
            try (SimpleFeatureIterator it = projectedFeatures.features()) {
                assertThat(it.hasNext()).isTrue();
                SimpleFeature f = it.next();
                assertThat(f.getAttribute("feature_name")).isInstanceOf(String.class);
                assertThat(f.getAttribute("category")).isInstanceOf(String.class);
                assertThat(f.getAttribute("geometry_bbox.xmin")).isInstanceOf(Float.class);
            }

            Filter byFeatureName = ff.equals(ff.property("feature_name"), ff.literal(sampleFeatureName));
            Query byFeatureNameQuery =
                    new Query("dt-feature-building", byFeatureName, new String[] {"feature_name", "category"});
            SimpleFeatureCollection featureNameFiltered = featureSource.getFeatures(byFeatureNameQuery);
            assertThat(featureNameFiltered.size()).isGreaterThan(0);
            try (SimpleFeatureIterator it = featureNameFiltered.features()) {
                while (it.hasNext()) {
                    SimpleFeature f = it.next();
                    assertThat(f.getAttribute("feature_name")).isEqualTo(sampleFeatureName);
                }
            }

            Filter byCategory = ff.equals(ff.property("category"), ff.literal(sampleCategory));
            Query byCategoryQuery =
                    new Query("dt-feature-building", byCategory, new String[] {"feature_name", "category"});
            SimpleFeatureCollection categoryFiltered = featureSource.getFeatures(byCategoryQuery);
            assertThat(categoryFiltered.size()).isGreaterThan(0);
            try (SimpleFeatureIterator it = categoryFiltered.features()) {
                int validated = 0;
                while (it.hasNext() && validated < 20) {
                    SimpleFeature f = it.next();
                    assertThat(f.getAttribute("category")).isEqualTo(sampleCategory);
                    validated++;
                }
                assertThat(validated).isGreaterThan(0);
            }

            Filter unsupportedSpatial = ff.bbox("geometry", -61.0, -33.0, -60.0, -32.0, "EPSG:4326");
            Query fallbackQuery = new Query("dt-feature-building", unsupportedSpatial, new String[] {"feature_name"});
            assertThat(featureSource.getFeatures(fallbackQuery).size()).isGreaterThan(0);
        } finally {
            dataStore.dispose();
        }
    }

    private static URL fixtureUrl() throws Exception {
        URL url = GeoParquetFileDataStoreFactoryTest.class.getClassLoader().getResource(FIXTURE_RESOURCE);
        assertThat(url).as("Missing fixture resource %s", FIXTURE_RESOURCE).isNotNull();
        assertThat(Files.size(Paths.get(url.toURI()))).isGreaterThan(0L);
        return url;
    }

    private static URL invalidFixtureUrl() throws Exception {
        URL url = GeoParquetFileDataStoreFactoryTest.class.getClassLoader().getResource(INVALID_GEOM_FIXTURE_RESOURCE);
        assertThat(url)
                .as("Missing fixture resource %s", INVALID_GEOM_FIXTURE_RESOURCE)
                .isNotNull();
        assertThat(Files.size(Paths.get(url.toURI()))).isGreaterThan(0L);
        return url;
    }

    private static URL overtureBuildingsFixtureUrl() throws Exception {
        URL url = GeoParquetFileDataStoreFactoryTest.class
                .getClassLoader()
                .getResource(OVERTURE_BUILDINGS_FIXTURE_RESOURCE);
        assertThat(url)
                .as("Missing fixture resource %s", OVERTURE_BUILDINGS_FIXTURE_RESOURCE)
                .isNotNull();
        assertThat(Files.size(Paths.get(url.toURI()))).isGreaterThan(0L);
        return url;
    }

    private static URL dtFeatureBuildingFixtureUrl() throws Exception {
        URL url = GeoParquetFileDataStoreFactoryTest.class
                .getClassLoader()
                .getResource(DT_FEATURE_BUILDING_FIXTURE_RESOURCE);
        assertThat(url)
                .as("Missing fixture resource %s", DT_FEATURE_BUILDING_FIXTURE_RESOURCE)
                .isNotNull();
        assertThat(Files.size(Paths.get(url.toURI()))).isGreaterThan(0L);
        return url;
    }

    private static Object unwrapDataStoreImpl(DataStore dataStore) throws Exception {
        InvocationHandler handler = Proxy.getInvocationHandler(dataStore);
        Field outer = handler.getClass().getDeclaredField("this$0");
        outer.setAccessible(true);
        return outer.get(handler);
    }

    private interface DataStoreProbe {
        java.io.File getFile();

        URL getURL();

        void ping();
    }

    private interface DataStoreBadSigProbe {
        Object getSchema(int badArg);

        Object getFeatureSource(int badArg);
    }

    private interface FeatureSourceProbe {
        void ping();
    }

    private interface FeatureCollectionProbe {
        SimpleFeatureIterator features();

        void close();
    }
}
