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

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.geotools.api.data.FeatureReader;
import org.geotools.api.data.FeatureWriter;
import org.geotools.api.data.FileDataStore;
import org.geotools.api.data.Query;
import org.geotools.api.data.SimpleFeatureSource;
import org.geotools.api.data.Transaction;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.feature.type.Name;
import org.geotools.api.filter.Filter;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.NameImpl;
import org.geotools.util.logging.Logging;

/**
 * Read-only {@link ContentDataStore} backed by a single GeoParquet file.
 *
 * <p>Implements {@link FileDataStore} to provide single-type convenience methods expected by GeoTools'
 * {@code FileDataStoreFinder} and similar utilities.
 */
public class GeoparquetContentDataStore extends ContentDataStore implements FileDataStore {

    private static final Logger LOGGER = Logging.getLogger(GeoparquetContentDataStore.class);

    private final URI uri;
    private final String typeName;
    private final FilterPredicateBuilder filterPredicateBuilder = new FilterPredicateBuilder();

    private final Storage storage;
    private final RangeReader rangeReader;
    private final GeoParquetRecordSource recordSource;
    private final SimpleFeatureType featureType;
    private final GeoParquetMetadata geoParquetMetadata;
    private final Set<String> wkbGeometryColumns;

    private GeoparquetContentDataStore(
            URI uri, Storage storage, RangeReader rangeReader, GeoParquetRecordSource recordSource) {
        this.uri = Objects.requireNonNull(uri, "uri");
        this.typeName = typeNameFrom(uri);
        this.storage = Objects.requireNonNull(storage, "storage");
        this.rangeReader = Objects.requireNonNull(rangeReader, "rangeReader");
        this.recordSource = Objects.requireNonNull(recordSource, "recordSource");
        this.featureType = new SchemaBuilder().build(typeName, recordSource.getSchema());
        this.geoParquetMetadata = parseGeoParquetMetadata(recordSource.getKeyValueMetadata());
        this.wkbGeometryColumns = extractGeometryColumnNames(geoParquetMetadata);
    }

    static String typeNameFrom(URI uri) {
        String path = uri.getPath();
        if (path == null || path.isBlank()) {
            return "geoparquet";
        }
        int slash = path.lastIndexOf('/');
        String fileName = slash >= 0 ? path.substring(slash + 1) : path;
        int dot = fileName.toLowerCase(Locale.ROOT).lastIndexOf(".parquet");
        return dot > 0 ? fileName.substring(0, dot) : fileName;
    }

    public static GeoparquetContentDataStore open(URL url) throws IOException {
        return open(url, new Properties());
    }

    public static GeoparquetContentDataStore open(URL url, Properties rangeReaderConfig) throws IOException {
        return open(url, rangeReaderConfig, TileverseParquetRecordSource::new);
    }

    public static GeoparquetContentDataStore open(URL url, GeoParquetRecordSourceFactory recordSourceFactory)
            throws IOException {
        return open(url, new Properties(), recordSourceFactory);
    }

    static GeoparquetContentDataStore open(
            URL url, Properties rangeReaderConfig, GeoParquetRecordSourceFactory recordSourceFactory)
            throws IOException {
        Objects.requireNonNull(recordSourceFactory, "recordSourceFactory");
        URI uri;
        try {
            uri = url.toURI();
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
        Properties config = rangeReaderConfig == null ? new Properties() : rangeReaderConfig;
        // The Storage is rooted at the parent of the leaf URI; openRangeReader(uri) then resolves the leaf
        // back to a key relative to that root. Both objects need to live as long as the data store.
        URI parent = uri.resolve(".");
        Storage storage = StorageFactory.open(parent, config);
        try {
            RangeReader rangeReader = storage.openRangeReader(uri);
            try {
                GeoParquetRecordSource recordSource = recordSourceFactory.create(rangeReader);
                return new GeoparquetContentDataStore(uri, storage, rangeReader, recordSource);
            } catch (IOException | RuntimeException t) {
                try {
                    rangeReader.close();
                } catch (IOException closeError) {
                    t.addSuppressed(closeError);
                }
                throw t;
            }
        } catch (IOException | RuntimeException t) {
            try {
                storage.close();
            } catch (IOException closeError) {
                t.addSuppressed(closeError);
            }
            throw t;
        }
    }

    @Override
    protected List<Name> createTypeNames() throws IOException {
        return List.of(new NameImpl(typeName));
    }

    @Override
    protected ContentFeatureSource createFeatureSource(ContentEntry entry) throws IOException {
        return new GeoParquetFeatureSource(entry, this);
    }

    // FileDataStore single-type convenience methods

    @Override
    public SimpleFeatureType getSchema() {
        return featureType;
    }

    @Override
    public SimpleFeatureSource getFeatureSource() throws IOException {
        return getFeatureSource(typeName);
    }

    @Override
    public FeatureReader<SimpleFeatureType, SimpleFeature> getFeatureReader() throws IOException {
        return getFeatureReader(new Query(typeName), Transaction.AUTO_COMMIT);
    }

    @Override
    public void updateSchema(SimpleFeatureType featureType) throws IOException {
        throw new IOException("GeoParquet datastore is read-only");
    }

    @Override
    public void createSchema(SimpleFeatureType featureType) throws IOException {
        throw new IOException("GeoParquet datastore is read-only");
    }

    @Override
    public FeatureWriter<SimpleFeatureType, SimpleFeature> getFeatureWriter(Filter filter, Transaction transaction)
            throws IOException {
        throw new UnsupportedOperationException("GeoParquet datastore is read-only");
    }

    @Override
    public FeatureWriter<SimpleFeatureType, SimpleFeature> getFeatureWriter(Transaction transaction)
            throws IOException {
        throw new UnsupportedOperationException("GeoParquet datastore is read-only");
    }

    @Override
    public FeatureWriter<SimpleFeatureType, SimpleFeature> getFeatureWriterAppend(Transaction transaction)
            throws IOException {
        throw new UnsupportedOperationException("GeoParquet datastore is read-only");
    }

    @Override
    public void dispose() {
        super.dispose();
        try {
            rangeReader.close();
        } catch (IOException e) {
            LOGGER.fine(() -> "Error closing RangeReader: " + e.getMessage());
        }
        try {
            storage.close();
        } catch (IOException e) {
            LOGGER.fine(() -> "Error closing Storage: " + e.getMessage());
        }
    }

    // Accessors for FeatureSource use

    SimpleFeatureType getFeatureType() {
        return featureType;
    }

    GeoParquetRecordSource getRecordSource() {
        return recordSource;
    }

    FilterPredicateBuilder getFilterPredicateBuilder() {
        return filterPredicateBuilder;
    }

    URI getUri() {
        return uri;
    }

    Set<String> getWkbGeometryColumns() {
        return wkbGeometryColumns;
    }

    GeoParquetMetadata getGeoParquetMetadata() {
        return geoParquetMetadata;
    }

    static GeoParquetMetadata parseGeoParquetMetadata(Map<String, String> keyValueMetadata) {
        String geoJson = keyValueMetadata.get("geo");
        if (geoJson == null || geoJson.isBlank()) {
            return null;
        }
        try {
            return GeoParquetMetadata.readValue(geoJson);
        } catch (Exception e) {
            LOGGER.log(Level.FINE, "Failed to parse GeoParquet 'geo' metadata", e);
            return null;
        }
    }

    static Set<String> extractGeometryColumnNames(GeoParquetMetadata metadata) {
        if (metadata == null) {
            return Set.of();
        }
        Map<String, GeometryColumnMetadata> columns = metadata.getColumns();
        if (columns == null || columns.isEmpty()) {
            return Set.of();
        }
        return Set.copyOf(columns.keySet());
    }
}
