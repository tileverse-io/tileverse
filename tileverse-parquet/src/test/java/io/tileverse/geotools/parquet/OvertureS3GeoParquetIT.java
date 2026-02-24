/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 */
package io.tileverse.geotools.parquet;

import static org.assertj.core.api.Assertions.assertThat;

import io.tileverse.parquet.ParquetDataset;
import io.tileverse.parquet.RangeReaderInputFile;
import io.tileverse.rangereader.RangeReader;
import io.tileverse.rangereader.RangeReaderFactory;
import java.io.BufferedWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.geotools.api.data.Query;
import org.geotools.api.filter.Filter;
import org.geotools.api.filter.FilterFactory;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.factory.CommonFactoryFinder;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

/**
 * Online integration tests against real Overture GeoParquet objects hosted on S3.
 *
 * <p>Execution is opt-in and skipped by default. Configure one or more URLs with:
 *
 * <ul>
 *   <li>system property {@code -Doverture.s3.geoparquet.urls=URL1,URL2}</li>
 *   <li>or environment variable {@code OVERTURE_S3_GEOPARQUET_URLS}</li>
 * </ul>
 *
 * <p>Example:
 *
 * <pre>
 * ./mvnw -pl tileverse-parquet -Dfailsafe.skip=false \
 *   -Doverture.s3.geoparquet.urls=https://&lt;bucket&gt;.s3.amazonaws.com/.../part-....parquet verify
 * </pre>
 */
class OvertureS3GeoParquetIT {

    private static final String URLS_PROPERTY = "overture.s3.geoparquet.urls";
    private static final String URLS_ENV = "OVERTURE_S3_GEOPARQUET_URLS";
    private static final String BBOX_PROPERTY = "overture.s3.bbox";
    private static final String MAX_FEATURES_PROPERTY = "overture.s3.maxFeatures";
    private static final String BRAZIL_COAST_BBOX = "-33.85,-39.2,-27.79,-27.7";
    private static final String BATHYMETRY_PART0 =
            "s3://overturemaps-us-west-2/release/2026-02-18.0/theme=base/type=bathymetry/"
                    + "part-00000-78f26a81-6a8f-4536-b468-fdc24c8fab33-c000.zstd.parquet";
    private static final String BBOX_SAMPLE_RESOURCE = "geojson/bathymetry-overturemaps-bboxsample.geojson";
    private static final FilterFactory FF = CommonFactoryFinder.getFilterFactory();
    private static final WKBReader WKB_READER = new WKBReader();

    @Test
    void parquetDataset_opensAndReadsMetadataFromFirstS3Url() throws Exception {
        URI readableUri = toReadableUri(configuredFirstUrl());
        try (RangeReader rangeReader = RangeReaderFactory.create(readableUri)) {
            ParquetDataset dataset = ParquetDataset.open(new RangeReaderInputFile(rangeReader));
            Map<String, String> metadata = dataset.getKeyValueMetadata();
            assertThat(metadata).containsKey("geo");
            assertThat(dataset.getSchema().getFieldCount()).isGreaterThan(0);
            assertThat(dataset.getRecordCount()).isGreaterThan(0);
        }
    }

    @Test
    void datastore_readsFirst100FeaturesFromFirstS3Url() throws Exception {
        URI readableUri = toReadableUri(configuredFirstUrl());
        GeoParquetFileDataStoreFactory factory = new GeoParquetFileDataStoreFactory();
        var dataStore = factory.createDataStore(readableUri.toURL());
        try {
            String typeName = dataStore.getTypeNames()[0];
            Query query = new Query(typeName, configuredBboxFilter(), new String[] {"id", "geometry"});
            int maxFeatures = configuredMaxFeatures();
            if (maxFeatures > 0) {
                query.setMaxFeatures(maxFeatures);
            }
            Path outputDir = Paths.get("target", "overture-it-output");
            Files.createDirectories(outputDir);
            Path geoJsonOutput = outputDir.resolve("first100.geojson");
            BuildResult result = buildGeoJson(dataStore, typeName, query, maxFeatures);
            try (BufferedWriter geojson = Files.newBufferedWriter(geoJsonOutput, StandardCharsets.UTF_8)) {
                geojson.write(result.geoJson());
            }
            int count = result.count();
            assertThat(count).isGreaterThan(0);
            if (maxFeatures > 0) {
                assertThat(count).isLessThanOrEqualTo(maxFeatures);
            }
        } finally {
            dataStore.dispose();
        }
    }

    @Test
    void datastore_brazilCoastBbox_matchesSampleGeoJson() throws Exception {
        URI readableUri = toReadableUri(BATHYMETRY_PART0);
        GeoParquetFileDataStoreFactory factory = new GeoParquetFileDataStoreFactory();
        var dataStore = factory.createDataStore(readableUri.toURL());
        try {
            String typeName = dataStore.getTypeNames()[0];
            Filter bboxFilter = parseBbox(BRAZIL_COAST_BBOX);
            Query query = new Query(typeName, bboxFilter, new String[] {"id", "geometry"});
            query.setMaxFeatures(100);

            BuildResult result = buildGeoJson(dataStore, typeName, query, 100);
            String expected = loadResourceText(BBOX_SAMPLE_RESOURCE);
            assertThat(canonicalJson(result.geoJson())).isEqualTo(canonicalJson(expected));
        } finally {
            dataStore.dispose();
        }
    }

    private static Filter configuredBboxFilter() {
        String raw = System.getProperty(BBOX_PROPERTY);
        if (raw == null || raw.isBlank()) {
            return Filter.INCLUDE;
        }

        String[] parts = raw.split("[,\\s]+");
        if (parts.length != 4) {
            throw new IllegalArgumentException("Invalid " + BBOX_PROPERTY + " value. Expected minX,minY,maxX,maxY");
        }
        double minX = Double.parseDouble(parts[0]);
        double minY = Double.parseDouble(parts[1]);
        double maxX = Double.parseDouble(parts[2]);
        double maxY = Double.parseDouble(parts[3]);
        return FF.bbox(FF.property("geometry"), minX, minY, maxX, maxY, "EPSG:4326");
    }

    private static int configuredMaxFeatures() {
        String raw = System.getProperty(MAX_FEATURES_PROPERTY, "100");
        return Integer.parseInt(raw);
    }

    private static BuildResult buildGeoJson(
            org.geotools.api.data.DataStore dataStore, String typeName, Query query, int maxFeatures)
            throws IOException {
        StringBuilder geojson = new StringBuilder();
        geojson.append("{\"type\":\"FeatureCollection\",\"features\":[\n");
        int count = 0;
        boolean first = true;
        try (SimpleFeatureIterator it =
                dataStore.getFeatureSource(typeName).getFeatures(query).features()) {
            while (it.hasNext() && (maxFeatures <= 0 || count < maxFeatures)) {
                var feature = it.next();
                Object id = feature.getAttribute("id");
                assertThat(id).isNotNull();
                Geometry geometry = toGeometry(feature.getAttribute("geometry"));
                assertThat(geometry).isNotNull();

                if (!first) {
                    geojson.append(",\n");
                }
                geojson.append("{\"type\":\"Feature\",\"properties\":{\"id\":");
                geojson.append(jsonValue(id));
                geojson.append("},\"geometry\":");
                geojson.append(geometryToGeoJson(geometry));
                geojson.append("}");
                first = false;
                count++;
            }
        }
        geojson.append("\n]}\n");
        return new BuildResult(geojson.toString(), count);
    }

    private static Filter parseBbox(String raw) {
        String[] parts = raw.split("[,\\s]+");
        if (parts.length != 4) {
            throw new IllegalArgumentException("Invalid bbox value. Expected minX,minY,maxX,maxY");
        }
        double minX = Double.parseDouble(parts[0]);
        double minY = Double.parseDouble(parts[1]);
        double maxX = Double.parseDouble(parts[2]);
        double maxY = Double.parseDouble(parts[3]);
        return FF.bbox(FF.property("geometry"), minX, minY, maxX, maxY, "EPSG:4326");
    }

    private static String loadResourceText(String resourcePath) throws IOException {
        try (var in = OvertureS3GeoParquetIT.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (in == null) {
                throw new IllegalArgumentException("Missing resource: " + resourcePath);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static String canonicalJson(String json) {
        return json.replaceAll("\\s+", "");
    }

    private record BuildResult(String geoJson, int count) {}

    private static String geometryToGeoJson(Geometry geometry) {
        if (geometry == null) {
            return "null";
        }
        if (geometry instanceof Point point) {
            return "{\"type\":\"Point\",\"coordinates\":" + coordinateToJson(point.getCoordinate()) + "}";
        }
        if (geometry instanceof LineString lineString) {
            return "{\"type\":\"LineString\",\"coordinates\":" + lineStringToJson(lineString) + "}";
        }
        if (geometry instanceof Polygon polygon) {
            return "{\"type\":\"Polygon\",\"coordinates\":" + polygonToJson(polygon) + "}";
        }
        if (geometry instanceof MultiPoint multiPoint) {
            StringBuilder sb = new StringBuilder();
            sb.append("{\"type\":\"MultiPoint\",\"coordinates\":[");
            for (int i = 0; i < multiPoint.getNumGeometries(); i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(coordinateToJson(((Point) multiPoint.getGeometryN(i)).getCoordinate()));
            }
            sb.append("]}");
            return sb.toString();
        }
        if (geometry instanceof MultiLineString multiLineString) {
            StringBuilder sb = new StringBuilder();
            sb.append("{\"type\":\"MultiLineString\",\"coordinates\":[");
            for (int i = 0; i < multiLineString.getNumGeometries(); i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(lineStringToJson((LineString) multiLineString.getGeometryN(i)));
            }
            sb.append("]}");
            return sb.toString();
        }
        if (geometry instanceof MultiPolygon multiPolygon) {
            StringBuilder sb = new StringBuilder();
            sb.append("{\"type\":\"MultiPolygon\",\"coordinates\":[");
            for (int i = 0; i < multiPolygon.getNumGeometries(); i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(polygonToJson((Polygon) multiPolygon.getGeometryN(i)));
            }
            sb.append("]}");
            return sb.toString();
        }
        if (geometry instanceof GeometryCollection geometryCollection) {
            StringBuilder sb = new StringBuilder();
            sb.append("{\"type\":\"GeometryCollection\",\"geometries\":[");
            for (int i = 0; i < geometryCollection.getNumGeometries(); i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(geometryToGeoJson(geometryCollection.getGeometryN(i)));
            }
            sb.append("]}");
            return sb.toString();
        }
        throw new IllegalArgumentException("Unsupported geometry type: " + geometry.getGeometryType());
    }

    private static Geometry toGeometry(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Geometry geometry) {
            return geometry;
        }
        if (value instanceof byte[] wkb) {
            try {
                return WKB_READER.read(wkb);
            } catch (ParseException e) {
                throw new IllegalArgumentException("Invalid WKB geometry payload", e);
            }
        }
        throw new IllegalArgumentException(
                "Unsupported geometry payload: " + value.getClass().getName());
    }

    private static String lineStringToJson(LineString lineString) {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (int i = 0; i < lineString.getNumPoints(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(coordinateToJson(lineString.getCoordinateN(i)));
        }
        sb.append(']');
        return sb.toString();
    }

    private static String polygonToJson(Polygon polygon) {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        sb.append(lineStringToJson(polygon.getExteriorRing()));
        for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
            sb.append(',');
            sb.append(lineStringToJson(polygon.getInteriorRingN(i)));
        }
        sb.append(']');
        return sb.toString();
    }

    private static String coordinateToJson(Coordinate c) {
        if (c == null) {
            return "null";
        }
        if (Double.isFinite(c.getZ())) {
            return "[" + c.getX() + "," + c.getY() + "," + c.getZ() + "]";
        }
        return "[" + c.getX() + "," + c.getY() + "]";
    }

    private static String jsonValue(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        }
        return "\"" + escapeJson(value.toString()) + "\"";
    }

    private static String escapeJson(String raw) {
        return raw.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static Set<String> configuredUrls() {
        String configured = System.getProperty(URLS_PROPERTY);
        if (configured == null || configured.isBlank()) {
            configured = System.getenv(URLS_ENV);
        }
        Assumptions.assumeTrue(
                configured != null && !configured.isBlank(), () -> "Missing " + URLS_PROPERTY + " or " + URLS_ENV);

        Set<String> urls = Arrays.stream(configured.split("[,\\s]+"))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Assumptions.assumeTrue(!urls.isEmpty(), "No Overture S3 URL configured");
        return urls;
    }

    private static String configuredFirstUrl() {
        return configuredUrls().iterator().next();
    }

    private static URI toReadableUri(String raw) {
        URI uri = URI.create(raw);
        String scheme = uri.getScheme();
        if (scheme == null || scheme.isBlank()) {
            return uri;
        }
        if (!"s3".equalsIgnoreCase(scheme)) {
            return uri;
        }

        String bucket = uri.getHost();
        if (bucket == null || bucket.isBlank()) {
            // tolerate s3://bucket/key parsed as path-only in odd cases
            String ssp = uri.getSchemeSpecificPart();
            int slash = ssp.indexOf('/');
            if (slash > 0) {
                bucket = ssp.substring(0, slash);
                String key = ssp.substring(slash + 1);
                return URI.create("https://s3.us-west-2.amazonaws.com/" + bucket + "/" + key);
            }
            throw new IllegalArgumentException("Invalid S3 URI (missing bucket): " + raw);
        }

        String key = uri.getPath();
        if (key == null) {
            key = "";
        }
        if (key.startsWith("/")) {
            key = key.substring(1);
        }
        String converted = "https://s3.us-west-2.amazonaws.com/" + bucket + "/" + key;
        try {
            return new URI(converted);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Could not convert S3 URI to HTTPS: " + raw, e);
        }
    }
}
