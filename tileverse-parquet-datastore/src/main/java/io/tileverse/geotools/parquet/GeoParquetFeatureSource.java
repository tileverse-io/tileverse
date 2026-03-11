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
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.geotools.api.data.FeatureReader;
import org.geotools.api.data.Query;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.filter.Filter;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.filter.FilterAttributeExtractor;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.util.logging.Logging;
import org.jspecify.annotations.Nullable;

/**
 * {@link ContentFeatureSource} for a single GeoParquet file.
 *
 * <p>Delegates Parquet record reading to the parent {@link GeoparquetContentDataStore} and converts
 * records to {@link SimpleFeature}s via {@link GeoparquetFeatureReader}.
 *
 * <p>Parquet filter pushdown is attempted as an optimization; the framework always applies
 * client-side filtering via {@code FilteringFeatureReader} for correctness (since
 * {@link #canFilter()} returns {@code false}).
 */
class GeoParquetFeatureSource extends ContentFeatureSource {

    private static final Logger LOGGER = Logging.getLogger(GeoParquetFeatureSource.class);

    private final GeoparquetContentDataStore store;

    GeoParquetFeatureSource(ContentEntry entry, GeoparquetContentDataStore store) {
        super(entry, Query.ALL);
        this.store = store;
    }

    @Override
    protected SimpleFeatureType buildFeatureType() throws IOException {
        return store.getFeatureType();
    }

    @Override
    protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {
        return null;
    }

    @Override
    protected int getCountInternal(Query query) throws IOException {
        return -1;
    }

    @Override
    protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query) throws IOException {
        Filter filter = query.getFilter();
        FilterPredicate filterPredicate = null;
        if (filter != null && filter != Filter.INCLUDE) {
            try {
                filterPredicate = store.getFilterPredicateBuilder()
                        .convert(filter, store.getRecordSource().getSchema());
            } catch (UnsupportedOperationException e) {
                LOGGER.log(Level.FINE, "Could not push down filter to parquet. Falling back to full scan.", e);
            }
        }

        Set<String> projectedColumns = resolveProjectedColumns(query);
        CloseableIterator<GenericRecord> records = openRecordIterator(filterPredicate, projectedColumns);
        return new GeoparquetFeatureReader(
                store.getFeatureType(), records, store.getWkbReader(), store.getWkbGeometryColumns());
    }

    private Set<String> resolveProjectedColumns(Query query) {
        String[] propertyNames = query.getPropertyNames();
        if (propertyNames == null || propertyNames.length == 0) {
            return Set.of();
        }
        Set<String> columns = new LinkedHashSet<>(Arrays.asList(propertyNames));
        // Include columns referenced by the filter so that FilteringFeatureReader
        // (applied by the framework when canFilter() returns false) can evaluate the filter.
        // Without this, ContentFeatureCollection.size() creates a minimal query with only
        // one attribute but keeps the filter, causing spatial filters to fail on null geometry.
        Filter filter = query.getFilter();
        if (filter != null && filter != Filter.INCLUDE) {
            FilterAttributeExtractor extractor = new FilterAttributeExtractor();
            filter.accept(extractor, null);
            columns.addAll(extractor.getAttributeNameSet());
        }
        return GeoparquetFeatureReader.toTopLevelColumns(columns);
    }

    private CloseableIterator<GenericRecord> openRecordIterator(
            @Nullable FilterPredicate filterPredicate, Set<String> projectedColumns) throws IOException {
        GeoParquetRecordSource recordSource = store.getRecordSource();
        boolean hasProjection = !projectedColumns.isEmpty();
        if (filterPredicate != null) {
            return hasProjection
                    ? recordSource.read(filterPredicate, projectedColumns)
                    : recordSource.read(filterPredicate);
        }
        return hasProjection ? recordSource.read(projectedColumns) : recordSource.read();
    }
}
