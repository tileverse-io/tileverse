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
import io.tileverse.parquet.ParquetDataset;
import io.tileverse.parquet.RangeReaderInputFile;
import io.tileverse.rangereader.RangeReader;
import io.tileverse.rangereader.RangeReaderFactory;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.geotools.api.data.FileDataStore;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.feature.type.AttributeDescriptor;
import org.geotools.api.feature.type.Name;
import org.geotools.api.filter.Filter;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.util.logging.Logging;
import org.jspecify.annotations.Nullable;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBReader;

final class GeoParquetFileDataStore {

    private static final Logger LOGGER = Logging.getLogger(GeoParquetFileDataStore.class);

    private final URL url;
    private final URI uri;
    private final String typeName;
    private final FilterPredicateBuilder filterPredicateBuilder = new FilterPredicateBuilder();
    private final WKBReader wkbReader = new WKBReader();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final RangeReader rangeReader;
    private final ParquetDataset dataset;
    private final SimpleFeatureType featureType;
    private final Name featureTypeName;
    private final FileDataStore dataStoreProxy;
    private final Object featureSourceProxy;

    private GeoParquetFileDataStore(URL url) throws IOException {
        this.url = Objects.requireNonNull(url, "url");
        this.uri = toUri(url);
        this.typeName = typeNameFrom(url);

        this.rangeReader = RangeReaderFactory.create(uri);
        this.dataset = ParquetDataset.open(new RangeReaderInputFile(rangeReader));
        this.featureType = new SchemaBuilder().build(typeName, dataset.getSchema());
        this.featureTypeName = createName(typeName);

        this.dataStoreProxy = (FileDataStore) Proxy.newProxyInstance(
                FileDataStore.class.getClassLoader(), new Class<?>[] {FileDataStore.class}, new DataStoreHandler());
        this.featureSourceProxy = createFeatureSourceProxy();
    }

    static FileDataStore open(URL url) throws IOException {
        return new GeoParquetFileDataStore(url).dataStoreProxy;
    }

    private final class DataStoreHandler implements InvocationHandler {

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (isObjectMethod(method)) {
                return handleObjectMethod(proxy, method, args);
            }
            ensureOpen();
            String name = method.getName();
            return switch (name) {
                case "getSchema" -> handleGetSchema(args);
                case "getTypeNames" -> new String[] {typeName};
                case "getNames" -> Collections.singletonList(featureTypeName);
                case "getFeatureSource" -> handleGetFeatureSource(args);
                case "dispose", "close" -> {
                    doClose();
                    yield null;
                }
                case "getFile" -> getFileIfLocal();
                case "getURL" -> url;
                case "createSchema", "updateSchema", "removeSchema", "createNewDataStore", "createFeatureType" ->
                    throw new IOException("GeoParquet datastore is read-only");
                default -> throw unsupported(method);
            };
        }
    }

    private Object createFeatureSourceProxy() {
        return Proxy.newProxyInstance(
                FileDataStore.class.getClassLoader(),
                new Class<?>[] {loadFeatureSourceClass()},
                new FeatureSourceHandler());
    }

    private final class FeatureSourceHandler implements InvocationHandler {

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (isObjectMethod(method)) {
                return handleObjectMethod(proxy, method, args);
            }
            ensureOpen();
            String name = method.getName();
            return switch (name) {
                case "getSchema" -> featureType;
                case "getName" -> featureTypeName;
                case "getDataStore" -> dataStoreProxy;
                case "getBounds" -> null;
                case "getCount" -> {
                    List<SimpleFeature> features = readFeatures(resolveFilter(args), resolveRequestedColumns(args));
                    yield features.size();
                }
                case "getFeatures" ->
                    createFeatureCollectionProxy(
                            method.getReturnType(), readFeatures(resolveFilter(args), resolveRequestedColumns(args)));
                case "addFeatureListener", "removeFeatureListener" -> null;
                default -> throw unsupported(method);
            };
        }
    }

    private Object createFeatureCollectionProxy(Class<?> returnType, List<SimpleFeature> features) {
        return Proxy.newProxyInstance(
                returnType.getClassLoader(),
                new Class<?>[] {returnType},
                new FeatureCollectionHandler(returnType, features));
    }

    private final class FeatureCollectionHandler implements InvocationHandler {

        private final Class<?> collectionType;
        private final List<SimpleFeature> features;

        FeatureCollectionHandler(Class<?> collectionType, List<SimpleFeature> features) {
            this.collectionType = collectionType;
            this.features = features;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (isObjectMethod(method)) {
                return handleObjectMethod(proxy, method, args);
            }
            String name = method.getName();
            return switch (name) {
                case "size" -> features.size();
                case "isEmpty" -> features.isEmpty();
                case "getSchema" -> featureType;
                case "getBounds" -> null;
                case "iterator", "features" -> createFeatureIteratorProxy(method.getReturnType(), features);
                case "toArray" -> features.toArray();
                case "close" -> null;
                case "subCollection", "sort" -> proxy;
                default -> throw unsupported(method);
            };
        }

        @Override
        public String toString() {
            return "FeatureCollection[" + collectionType.getSimpleName() + ", size=" + features.size() + "]";
        }
    }

    private Object createFeatureIteratorProxy(Class<?> returnType, List<SimpleFeature> features) {
        AtomicInteger index = new AtomicInteger(0);
        return Proxy.newProxyInstance(
                returnType.getClassLoader(), new Class<?>[] {returnType}, (proxy, method, args) -> {
                    if (isObjectMethod(method)) {
                        return handleObjectMethod(proxy, method, args);
                    }
                    String name = method.getName();
                    return switch (name) {
                        case "hasNext" -> index.get() < features.size();
                        case "next" -> {
                            int i = index.getAndIncrement();
                            if (i >= features.size()) {
                                throw new java.util.NoSuchElementException();
                            }
                            yield features.get(i);
                        }
                        case "close" -> null;
                        default -> throw unsupported(method);
                    };
                });
    }

    private List<SimpleFeature> readFeatures(@Nullable Filter filter, Set<String> requestedColumns) throws IOException {
        FilterPredicate filterPredicate = null;
        if (filter != null && filter != Filter.INCLUDE) {
            try {
                filterPredicate = filterPredicateBuilder.convert(filter, dataset.getSchema());
            } catch (UnsupportedOperationException e) {
                LOGGER.log(Level.FINE, "Could not push down filter to parquet. Falling back to full scan.", e);
            }
        }

        Set<String> projectedColumns = toTopLevelColumns(requestedColumns);
        List<SimpleFeature> result = new ArrayList<>();
        AtomicInteger fid = new AtomicInteger(0);

        try (CloseableIterator<GenericRecord> records = openRecordIterator(filterPredicate, projectedColumns)) {
            while (records.hasNext()) {
                GenericRecord record = records.next();
                result.add(toSimpleFeature(record, fid.getAndIncrement()));
            }
        }
        return result;
    }

    private CloseableIterator<GenericRecord> openRecordIterator(
            @Nullable FilterPredicate filterPredicate, Set<String> projectedColumns) throws IOException {

        boolean hasProjection = !projectedColumns.isEmpty();
        if (filterPredicate != null) {
            return hasProjection ? dataset.read(filterPredicate, projectedColumns) : dataset.read(filterPredicate);
        }
        return hasProjection ? dataset.read(projectedColumns) : dataset.read();
    }

    private SimpleFeature toSimpleFeature(GenericRecord record, int featureId) throws IOException {
        Map<String, Object> flat = new LinkedHashMap<>();
        flattenRecord("", record, flat);
        SimpleFeatureBuilder builder = new SimpleFeatureBuilder(featureType);
        for (AttributeDescriptor d : featureType.getAttributeDescriptors()) {
            String name = d.getLocalName();
            Object value = convertValue(flat.get(name), d.getType().getBinding());
            builder.set(name, value);
        }
        return builder.buildFeature(typeName + "." + featureId);
    }

    private void flattenRecord(String prefix, GenericRecord record, Map<String, Object> target) {
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

    private Object normalizeAvroValue(Object value) {
        if (value == null) return null;
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

    private Object convertValue(Object value, Class<?> binding) throws IOException {
        if (value == null) return null;
        if (Geometry.class.isAssignableFrom(binding)) {
            if (value instanceof byte[] bytes) {
                try {
                    return wkbReader.read(bytes);
                } catch (org.locationtech.jts.io.ParseException e) {
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
        if (binding == LocalDate.class && value instanceof Number n) return LocalDate.ofEpochDay(n.longValue());
        if (binding == Instant.class && value instanceof Number n) return Instant.ofEpochMilli(n.longValue());
        return value;
    }

    private Set<String> resolveRequestedColumns(Object[] args) {
        if (args == null || args.length == 0) {
            return Set.of();
        }
        Object arg = args[0];
        if (arg == null) {
            return Set.of();
        }
        if (arg instanceof String[] names) {
            return new LinkedHashSet<>(Arrays.asList(names));
        }
        // GeoTools Query (via api module): resolve with reflection to keep compile-time coupling low.
        try {
            Method m = arg.getClass().getMethod("getPropertyNames");
            Object propertyNames = m.invoke(arg);
            if (propertyNames instanceof String[] names) {
                return new LinkedHashSet<>(Arrays.asList(names));
            }
        } catch (ReflectiveOperationException e) {
            LOGGER.log(Level.FINER, "Could not resolve query projection via reflection", e);
        }
        return Set.of();
    }

    private @Nullable Filter resolveFilter(Object[] args) {
        if (args == null || args.length == 0 || args[0] == null) {
            return Filter.INCLUDE;
        }
        Object arg = args[0];
        if (arg instanceof Filter f) {
            return f;
        }
        try {
            Method m = arg.getClass().getMethod("getFilter");
            Object filter = m.invoke(arg);
            if (filter instanceof Filter f) {
                return f;
            }
        } catch (ReflectiveOperationException e) {
            LOGGER.log(Level.FINER, "Could not resolve query filter via reflection", e);
        }
        return Filter.INCLUDE;
    }

    private Set<String> toTopLevelColumns(Set<String> requestedColumns) {
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

    private Object handleGetSchema(Object[] args) throws IOException {
        if (args == null || args.length == 0) {
            return featureType;
        }
        if (args.length == 1 && args[0] instanceof String typeNameArg) {
            if (!typeName.equals(typeNameArg)) {
                throw new IOException("Unknown type name: " + typeNameArg + ", expected: " + typeName);
            }
            return featureType;
        }
        throw new IOException("Unsupported getSchema signature");
    }

    private Object handleGetFeatureSource(Object[] args) throws IOException {
        if (args == null || args.length == 0) {
            return featureSourceProxy;
        }
        if (args.length == 1 && args[0] instanceof String typeNameArg) {
            if (!typeName.equals(typeNameArg)) {
                throw new IOException("Unknown type name: " + typeNameArg + ", expected: " + typeName);
            }
            return featureSourceProxy;
        }
        throw new IOException("Unsupported getFeatureSource signature");
    }

    private static Class<?> loadFeatureSourceClass() {
        try {
            return Class.forName("org.geotools.api.data.SimpleFeatureSource");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("GeoTools SimpleFeatureSource API not found", e);
        }
    }

    private File getFileIfLocal() {
        String scheme = uri.getScheme();
        if (scheme == null || "file".equalsIgnoreCase(scheme)) {
            return new File(uri);
        }
        return null;
    }

    private static Name createName(String localPart) {
        try {
            Class<?> nameImpl = Class.forName("org.geotools.feature.NameImpl");
            return (Name) nameImpl.getConstructor(String.class).newInstance(localPart);
        } catch (ReflectiveOperationException e) {
            // Fallback proxy implementing only commonly used Name methods.
            return (Name) Proxy.newProxyInstance(
                    Name.class.getClassLoader(),
                    new Class<?>[] {Name.class},
                    (proxy, method, args) -> switch (method.getName()) {
                        case "getLocalPart", "toString" -> localPart;
                        case "getNamespaceURI" -> null;
                        default -> {
                            if (isObjectMethod(method)) {
                                yield handleObjectMethod(proxy, method, args);
                            }
                            throw unsupported(method);
                        }
                    });
        }
    }

    private void ensureOpen() throws IOException {
        if (closed.get()) {
            throw new IOException("GeoParquet datastore is closed: " + url);
        }
    }

    private void doClose() throws IOException {
        if (closed.compareAndSet(false, true)) {
            IOException closeError = null;
            try {
                rangeReader.close();
            } catch (IOException e) {
                closeError = e;
            }
            if (closeError != null) {
                throw closeError;
            }
        }
    }

    private static URI toUri(URL url) throws IOException {
        try {
            return url.toURI();
        } catch (URISyntaxException e) {
            throw new IOException("Invalid URL: " + url, e);
        }
    }

    static String typeNameFrom(URL url) {
        String path = url.getPath();
        if (path == null || path.isBlank()) {
            return "geoparquet";
        }
        int slash = path.lastIndexOf('/');
        String fileName = slash >= 0 ? path.substring(slash + 1) : path;
        int dot = fileName.toLowerCase(Locale.ROOT).lastIndexOf(".parquet");
        return dot > 0 ? fileName.substring(0, dot) : fileName;
    }

    private static boolean isObjectMethod(Method method) {
        return method.getDeclaringClass() == Object.class;
    }

    private static Object handleObjectMethod(Object proxy, Method method, Object[] args) {
        return switch (method.getName()) {
            case "toString" -> proxy.getClass().getSimpleName();
            case "hashCode" -> System.identityHashCode(proxy);
            case "equals" -> args != null && args.length == 1 && proxy == args[0];
            default -> throw new IllegalStateException("Unhandled Object method: " + method);
        };
    }

    private static UnsupportedOperationException unsupported(Method method) {
        return new UnsupportedOperationException("Method not implemented: " + method);
    }
}
