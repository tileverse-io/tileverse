/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 */
package io.tileverse.geotools.parquet;

import static org.assertj.core.api.Assertions.assertThat;

import io.tileverse.parquet.ParquetDataset;
import io.tileverse.parquet.RangeReaderInputFile;
import io.tileverse.rangereader.RangeReader;
import io.tileverse.rangereader.RangeReaderFactory;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Utility IT to inspect GeoParquet metadata for an arbitrary file provided by property.
 *
 * <p>Usage:
 *
 * <pre>
 * ./mvnw -pl tileverse-parquet \
 *   -Dsurefire.skip=true -Dfailsafe.skip=false \
 *   -Dit.test=GeoParquetMetadataDumpIT \
 *   -Dgeoparquet.meta.url="s3://bucket/path/file.parquet" \
 *   -Dgpg.skip=true verify
 * </pre>
 */
class GeoParquetMetadataDumpIT {

    private static final String URL_PROPERTY = "geoparquet.meta.url";

    @Test
    void printGeoMetadataAndGlobalBbox() throws Exception {
        String configured = System.getProperty(URL_PROPERTY);
        Assumptions.assumeTrue(
                configured != null && !configured.isBlank(),
                "Set -" + URL_PROPERTY + "=<file|http|s3 url> to run this IT");

        URI readableUri = toReadableUri(configured);
        try (RangeReader reader = RangeReaderFactory.create(readableUri)) {
            ParquetDataset dataset = ParquetDataset.open(new RangeReaderInputFile(reader));
            Map<String, String> metadata = dataset.getKeyValueMetadata();
            String geo = metadata.get("geo");

            assertThat(geo).as("GeoParquet metadata key 'geo'").isNotBlank();

            System.out.println("METADATA url=" + readableUri);
            System.out.println("METADATA geo=" + geo);

            String bbox = extractFirstBboxArray(geo);
            if (bbox == null) {
                System.out.println("METADATA bbox=not-found");
            } else {
                System.out.println("METADATA bbox=" + bbox);
            }
        }
    }

    private static String extractFirstBboxArray(String geoJson) {
        int bboxKey = geoJson.indexOf("\"bbox\"");
        if (bboxKey < 0) {
            return null;
        }
        int open = geoJson.indexOf('[', bboxKey);
        if (open < 0) {
            return null;
        }
        int depth = 0;
        for (int i = open; i < geoJson.length(); i++) {
            char c = geoJson.charAt(i);
            if (c == '[') {
                depth++;
            } else if (c == ']') {
                depth--;
                if (depth == 0) {
                    return geoJson.substring(open, i + 1);
                }
            }
        }
        return null;
    }

    private static URI toReadableUri(String raw) throws URISyntaxException {
        URI uri = URI.create(raw);
        String scheme = uri.getScheme();
        if (scheme == null || scheme.isBlank()) {
            return Path.of(raw).toUri();
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
        return URI.create("https://s3.us-west-2.amazonaws.com/" + bucket + "/" + key);
    }
}
