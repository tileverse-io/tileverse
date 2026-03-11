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
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
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
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

class GeoParquetFileDataStoreFactoryTest {

    private static final FilterFactory ff = CommonFactoryFinder.getFilterFactory(null);
    private static final String FIXTURE_RESOURCE = "/geoparquet/sample-geoparquet.parquet";
    private static final String INVALID_GEOM_FIXTURE_RESOURCE = "/geoparquet/invalid-geometry-geoparquet.parquet";
    private static final String OVERTURE_BUILDINGS_FIXTURE_RESOURCE = "/geoparquet/rosario-center-buildings.parquet";
    private static final String DT_FEATURE_BUILDING_FIXTURE_RESOURCE = "/geoparquet/dt-feature-building.parquet";

    private final GeoParquetFileDataStoreFactory factory = new GeoParquetFileDataStoreFactory();

    @TempDir
    Path tmpdir;

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
        assertThat(GeoparquetContentDataStore.typeNameFrom(new URI("file:/tmp/sample-geoparquet.parquet")))
                .isEqualTo("sample-geoparquet");
        assertThat(GeoparquetContentDataStore.typeNameFrom(new URI("file:/tmp/without_extension")))
                .isEqualTo("without_extension");
        assertThat(GeoparquetContentDataStore.typeNameFrom(new URI("file:/tmp/.parquet")))
                .isEqualTo(".parquet");
        assertThat(GeoparquetContentDataStore.typeNameFrom(new URI("http://example.com")))
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
            assertThat(features.toArray()).hasSize(3);
            assertThat(features.sort(SortBy.NATURAL_ORDER).size()).isEqualTo(3);
            assertThat(features.subCollection(Filter.INCLUDE).size()).isEqualTo(3);
            assertThat(featureSource.getBounds()).isNull();

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
        try (FileRangeReader rangeReader = FileRangeReader.of(extractToTempFile(FIXTURE_RESOURCE))) {
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

            assertThatThrownBy(() -> dataStore.createSchema(dataStore.getSchema()))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("read-only");

            // ContentDataStore implements getFeatureReader
            var reader = dataStore.getFeatureReader(new Query("sample-geoparquet"), Transaction.AUTO_COMMIT);
            assertThat(reader).isNotNull();
            reader.close();

            // ContentDataStore writer methods throw IOException (from ensureFeatureStore) on read-only store
            assertThatThrownBy(() ->
                            dataStore.getFeatureWriter("sample-geoparquet", Filter.INCLUDE, Transaction.AUTO_COMMIT))
                    .isInstanceOf(IOException.class);
            assertThatThrownBy(() -> dataStore.getFeatureWriter("sample-geoparquet", Transaction.AUTO_COMMIT))
                    .isInstanceOf(IOException.class);
            assertThatThrownBy(() -> dataStore.getFeatureWriterAppend("sample-geoparquet", Transaction.AUTO_COMMIT))
                    .isInstanceOf(IOException.class);

            // ContentDataStore provides InProcessLockingManager
            assertThat(dataStore.getLockingManager()).isNotNull();
        } finally {
            dataStore.dispose();
        }
    }

    @Test
    void featureSourceAndCollection_workWithContentDataStore() throws Exception {
        URL url = fixtureUrl();
        DataStore dataStore = factory.createDataStore(url);
        try {
            SimpleFeatureSource source = dataStore.getFeatureSource("sample-geoparquet");
            // ContentFeatureSource provides these
            assertThat(source.getInfo()).isNotNull();
            assertThat(source.getQueryCapabilities()).isNotNull();
            assertThat(source.getSupportedHints()).isNotNull();
            assertThat(source.getBounds(Query.ALL)).isNull();
            assertThat(source.getName().getLocalPart()).isEqualTo("sample-geoparquet");
            assertThat(source.getDataStore()).isNotNull();

            // Listener methods are no-ops (no exception thrown)
            source.addFeatureListener(null);
            source.removeFeatureListener(null);

            SimpleFeatureCollection collection = source.getFeatures();
            assertThat(collection.size()).isEqualTo(3);
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
    void invalidGeometryFixture_throwsOnRead() throws Exception {
        URL url = invalidFixtureUrl();
        var dataStore = factory.createDataStore(url);
        try {
            SimpleFeatureSource featureSource = dataStore.getFeatureSource();
            SimpleFeatureCollection features = featureSource.getFeatures();
            // Error surfaces during iteration, not during getFeatures()
            assertThatThrownBy(() -> {
                        try (SimpleFeatureIterator it = features.features()) {
                            while (it.hasNext()) it.next();
                        }
                    })
                    .hasStackTraceContaining("Failed to parse geometry WKB");
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
            // This Overture subset has no GeoParquet GEOMETRY annotation, so geometry
            // remains BYTE_ARRAY.
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
        try (FileRangeReader rangeReader = FileRangeReader.of(extractToTempFile(OVERTURE_BUILDINGS_FIXTURE_RESOURCE))) {
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
                    validated++;
                }
                assertThat(validated).isGreaterThan(0);
            }

            Filter unsupportedSpatial = ff.bbox("geometry", -61.0, -33.0, -60.0, -32.0, "EPSG:4326");
            Query fallbackQuery = new Query("rosario-center-buildings", unsupportedSpatial, new String[] {"id"});
            int totalCount = featureSource.getFeatures().size();
            int filteredCount = featureSource.getFeatures(fallbackQuery).size();
            assertThat(totalCount).isGreaterThan(0);
            assertThat(filteredCount).isBetween(0, totalCount);
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
        try (FileRangeReader rangeReader =
                FileRangeReader.of(extractToTempFile(DT_FEATURE_BUILDING_FIXTURE_RESOURCE))) {
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
            int totalCount = featureSource.getFeatures().size();
            int filteredCount = featureSource.getFeatures(fallbackQuery).size();
            assertThat(totalCount).isGreaterThan(0);
            assertThat(filteredCount).isBetween(0, totalCount);
        } finally {
            dataStore.dispose();
        }
    }

    private URL fixtureUrl() {
        return extractToUrl(FIXTURE_RESOURCE);
    }

    private URL invalidFixtureUrl() {
        return extractToUrl(INVALID_GEOM_FIXTURE_RESOURCE);
    }

    private URL overtureBuildingsFixtureUrl() {
        return extractToUrl(OVERTURE_BUILDINGS_FIXTURE_RESOURCE);
    }

    private URL dtFeatureBuildingFixtureUrl() {
        return extractToUrl(DT_FEATURE_BUILDING_FIXTURE_RESOURCE);
    }

    private URL extractToUrl(String resource) {
        try {
            return extractToTempFile(resource).toUri().toURL();
        } catch (MalformedURLException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Path extractToTempFile(String resource) {
        try (InputStream in = getClass().getResourceAsStream(resource)) {
            assertThat(in).as("Missing fixture resource %s", resource).isNotNull();

            String filename = Paths.get(resource).getFileName().toString();
            Path tempFile = tmpdir.resolve(filename);
            Files.copy(in, tempFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            return tempFile;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
