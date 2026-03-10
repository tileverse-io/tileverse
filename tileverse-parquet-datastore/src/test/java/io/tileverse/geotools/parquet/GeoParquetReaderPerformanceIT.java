/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 */
package io.tileverse.geotools.parquet;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.geotools.api.data.Query;
import org.geotools.api.filter.FilterFactory;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.factory.CommonFactoryFinder;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

class GeoParquetReaderPerformanceIT {

    private static final String PERF_ENABLED = "perf.parquet.enabled";
    private static final String BRAZIL_BATHYMETRY_URL =
            "s3://overturemaps-us-west-2/release/2026-02-18.0/theme=base/type=bathymetry/"
                    + "part-00000-78f26a81-6a8f-4536-b468-fdc24c8fab33-c000.zstd.parquet";
    private static final String BRAZIL_BATHYMETRY_BBOX = "-33.85,-39.2,-27.79,-27.7";

    private static final String INFRASTRUCTURE_URL =
            "s3://overturemaps-us-west-2/release/2026-02-18.0/theme=base/type=infrastructure/"
                    + "part-00000-18804dd8-7c47-4c2b-9b31-630467cfcf99-c000.zstd.parquet";
    private static final String INFRASTRUCTURE_BBOX = "-73,-55,-53,-21";
    private static final List<Integer> MAX_FEATURES_SWEEP = List.of(10, 100, 1000, 10000);
    private static final int ITERATIONS = 3;
    private static final int WARMUPS = 1;

    private static final FilterFactory FF = CommonFactoryFinder.getFilterFactory();

    @Test
    void compare_core_vs_hadoop_reader_brazil_bathymetry_bbox_feature_sweep() throws Exception {
        compare_core_vs_hadoop_reader_with_bbox_feature_sweep(
                "brazil-bathymetry", BRAZIL_BATHYMETRY_URL, BRAZIL_BATHYMETRY_BBOX, List.of(100), 1, WARMUPS);
    }

    @Test
    void compare_core_vs_hadoop_reader_infrastructure_bbox_feature_sweep() throws Exception {
        compare_core_vs_hadoop_reader_with_bbox_feature_sweep(
                "infrastructure-south-america",
                INFRASTRUCTURE_URL,
                INFRASTRUCTURE_BBOX,
                MAX_FEATURES_SWEEP,
                ITERATIONS,
                WARMUPS);
    }

    void compare_core_vs_hadoop_reader_with_bbox_feature_sweep(
            String scenarioName,
            String rawUrl,
            String bbox,
            List<Integer> maxFeaturesSweep,
            int iterations,
            int warmups)
            throws Exception {
        Assumptions.assumeTrue(
                Boolean.parseBoolean(System.getProperty(PERF_ENABLED, "false")),
                "Set -D" + PERF_ENABLED + "=true to run this performance IT");

        URI uri = toReadableUri(rawUrl);

        System.out.printf(
                Locale.ROOT,
                "PERF scenario name=%s url=%s bbox=%s sweep=%s iterations=%d warmups=%d%n",
                scenarioName,
                rawUrl,
                bbox,
                maxFeaturesSweep,
                iterations,
                warmups);

        for (int maxFeatures : maxFeaturesSweep) {
            Query query = createQuery(uri, bbox, maxFeatures);
            runWarmups(uri, query, warmups);
            List<Result> results = runIterations(uri, query, iterations);
            printAndAssertStats(scenarioName, maxFeatures, results);
        }
    }

    private List<Result> runIterations(URI uri, Query query, int iterations) throws Exception {
        List<Result> results = new ArrayList<>();
        for (int i = 0; i < iterations; i++) {
            results.add(runOnce("core", uri, query, Backend.CORE));
            results.add(runOnce("hadoop", uri, query, Backend.HADOOP));
        }
        return results;
    }

    private void printAndAssertStats(String scenarioName, int maxFeatures, List<Result> results) {
        Map<String, List<Result>> byBackend = results.stream()
                .collect(Collectors.groupingBy(Result::backend, java.util.LinkedHashMap::new, Collectors.toList()));

        Result coreFirst = byBackend.get("core").get(0);
        Result hadoopFirst = byBackend.get("hadoop").get(0);
        assertThat(coreFirst.count()).isGreaterThan(0);
        assertThat(hadoopFirst.count()).isEqualTo(coreFirst.count());
        assertThat(hadoopFirst.idHash()).isEqualTo(coreFirst.idHash());

        Stats coreStats = stats(byBackend.get("core"));
        Stats hadoopStats = stats(byBackend.get("hadoop"));
        long deltaMs = coreStats.avgMs() - hadoopStats.avgMs();
        double deltaPct = hadoopStats.avgMs() == 0 ? 0.0d : (deltaMs * 100.0d / hadoopStats.avgMs());

        System.out.printf(
                Locale.ROOT,
                "PERF compare scenario=%s maxFeatures=%d | core_avg_ms=%d core_min_ms=%d core_max_ms=%d "
                        + "| hadoop_avg_ms=%d hadoop_min_ms=%d hadoop_max_ms=%d "
                        + "| delta_ms=%d delta_pct=%.2f%%%n",
                scenarioName,
                maxFeatures,
                coreStats.avgMs(),
                coreStats.minMs(),
                coreStats.maxMs(),
                hadoopStats.avgMs(),
                hadoopStats.minMs(),
                hadoopStats.maxMs(),
                deltaMs,
                deltaPct);

        results.stream()
                .sorted(Comparator.comparing(Result::backend).thenComparing(Result::durationMs))
                .forEach(r -> System.out.printf(
                        Locale.ROOT,
                        "PERF run scenario=%s maxFeatures=%d backend=%s duration_ms=%d count=%d hash=%d%n",
                        scenarioName,
                        maxFeatures,
                        r.backend(),
                        r.durationMs(),
                        r.count(),
                        r.idHash()));
    }

    private static Query createQuery(URI uri, String bbox, int maxFeatures) throws Exception {
        Query query = new Query(
                GeoParquetFileDataStore.typeNameFrom(uri.toURL()), parseBbox(bbox), new String[] {"id", "geometry"});
        query.setMaxFeatures(maxFeatures);
        return query;
    }

    private void runWarmups(URI uri, Query query, int warmups) throws Exception {
        for (int i = 0; i < warmups; i++) {
            runOnce("core", uri, query, Backend.CORE);
            runOnce("hadoop", uri, query, Backend.HADOOP);
        }
    }

    private Result runOnce(String backendName, URI uri, Query query, Backend backend) throws Exception {
        long t0 = System.nanoTime();
        int count = 0;
        long idHash = 1L;

        var dataStore =
                switch (backend) {
                    case CORE -> new GeoParquetFileDataStoreFactory().createDataStore(uri.toURL());
                    case HADOOP -> GeoParquetFileDataStore.open(uri.toURL(), HadoopGeoParquetRecordSource::new);
                };
        try {
            String typeName = dataStore.getTypeNames()[0];
            try (SimpleFeatureIterator it =
                    dataStore.getFeatureSource(typeName).getFeatures(query).features()) {
                while (it.hasNext()) {
                    var f = it.next();
                    Object id = f.getAttribute("id");
                    idHash = 31L * idHash + (id == null ? 0 : id.toString().hashCode());
                    count++;
                }
            }
        } finally {
            dataStore.dispose();
        }
        long durationMs = (System.nanoTime() - t0) / 1_000_000L;
        return new Result(backendName, durationMs, count, idHash);
    }

    private static Stats stats(List<Result> results) {
        long avg = Math.round(
                results.stream().mapToLong(Result::durationMs).average().orElse(0.0d));
        long min = results.stream().mapToLong(Result::durationMs).min().orElse(0L);
        long max = results.stream().mapToLong(Result::durationMs).max().orElse(0L);
        return new Stats(avg, min, max);
    }

    private static org.geotools.api.filter.Filter parseBbox(String raw) {
        String[] parts = raw.split("[,\\s]+");
        if (parts.length != 4) {
            throw new IllegalArgumentException("Invalid bbox. Expected minX,minY,maxX,maxY");
        }
        double minX = Double.parseDouble(parts[0]);
        double minY = Double.parseDouble(parts[1]);
        double maxX = Double.parseDouble(parts[2]);
        double maxY = Double.parseDouble(parts[3]);
        return FF.bbox(FF.property("geometry"), minX, minY, maxX, maxY, "EPSG:4326");
    }

    private static URI toReadableUri(String raw) throws URISyntaxException {
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
        return new URI(converted);
    }

    private enum Backend {
        CORE,
        HADOOP
    }

    private record Stats(long avgMs, long minMs, long maxMs) {}

    private record Result(String backend, long durationMs, int count, long idHash) {}
}
