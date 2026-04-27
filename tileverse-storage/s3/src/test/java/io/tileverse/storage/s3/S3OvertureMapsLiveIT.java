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
package io.tileverse.storage.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tileverse.storage.ListOptions;
import io.tileverse.storage.NotFoundException;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.ReadHandle;
import io.tileverse.storage.ReadOptions;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageEntry;
import io.tileverse.storage.StorageFactory;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Live IT exercising recursive glob listing against Overture Maps' public {@code overturemaps-us-west-2} S3 bucket. The
 * Hive-style partition layout ({@code theme=X/type=Y/part-NNNNN-*.zstd.parquet}) is the same case duckdb's
 * {@code glob('s3://overturemaps-us-west-2/release/<latest>/theme=addresses/**\/*.parquet')} hits.
 *
 * <p>The release version is resolved at runtime from {@code https://stac.overturemaps.org/catalog.json}'s
 * {@code latest} field so the test stays current as Overture cuts new releases.
 */
class S3OvertureMapsLiveIT {

    private static final URI STAC_CATALOG = URI.create("https://stac.overturemaps.org/catalog.json");
    private static final Pattern LATEST_FIELD = Pattern.compile("\"latest\"\\s*:\\s*\"([^\"]+)\"");

    private static URI baseUri;

    @BeforeAll
    static void check() throws IOException, InterruptedException {
        String latest = fetchLatestRelease();
        baseUri = URI.create("s3://overturemaps-us-west-2/release/" + latest + "/");
    }

    private static String fetchLatestRelease() throws IOException, InterruptedException {
        HttpClient http =
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        HttpRequest req = HttpRequest.newBuilder(STAC_CATALOG)
                .timeout(Duration.ofSeconds(10))
                .header("Accept", "application/json")
                .GET()
                .build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new IOException("STAC catalog returned HTTP " + resp.statusCode());
        }
        Matcher m = LATEST_FIELD.matcher(resp.body());
        if (!m.find()) {
            throw new IOException("STAC catalog has no 'latest' field");
        }
        return m.group(1);
    }

    @Test
    void recursiveGlobListsAddressesParquetFiles() throws IOException {
        try (Storage storage = StorageFactory.open(baseUri, anonymous());
                Stream<StorageEntry> stream = storage.list("theme=addresses/**/*.parquet")) {
            List<StorageEntry> first = stream.limit(5).toList();
            assertThat(first).hasSize(5);
            assertThat(first).allSatisfy(e -> {
                assertThat(e).isInstanceOf(StorageEntry.File.class);
                assertThat(e.key()).startsWith("theme=addresses/").endsWith(".parquet");
            });
        }
    }

    /**
     * Uses a glob with a leading {@code **} so the parsed prefix is empty: the storage backend lists from the release
     * root and the client-side matcher must reject every theme other than {@code theme=addresses} (the only theme whose
     * layout includes {@code type=address/}). Covers the {@code prefix=""} branch of
     * {@link io.tileverse.storage.StoragePattern} and validates the {@code java.nio.file.PathMatcher}-driven filter
     * against keys with {@code =} in path components.
     */
    @Test
    void leadingDoubleStarGlobFiltersOutOtherThemes() throws IOException {
        try (Storage storage = StorageFactory.open(baseUri, anonymous());
                Stream<StorageEntry> stream = storage.list("**/type=address/*.parquet")) {
            List<StorageEntry> first = stream.limit(5).toList();
            assertThat(first).hasSize(5);
            assertThat(first).allSatisfy(e -> {
                assertThat(e).isInstanceOf(StorageEntry.File.class);
                assertThat(e.key())
                        .startsWith("theme=addresses/")
                        .contains("/type=address/")
                        .endsWith(".parquet");
            });
        }
    }

    @Test
    void listBadRegion() throws IOException {
        Properties props = anonymous();
        props.setProperty(S3StorageProvider.S3_REGION.key(), "us-east-2");
        try (Storage storage = StorageFactory.open(baseUri, props)) {
            Stream<StorageEntry> list = storage.list("theme=addresses/**/*.parquet");
            assertThatThrownBy(() -> list.limit(1).count())
                    .isInstanceOf(io.tileverse.storage.StorageException.class)
                    .hasCauseInstanceOf(software.amazon.awssdk.services.s3.model.S3Exception.class)
                    .hasMessageContaining("Service: S3, Status Code: 301");
        }
    }

    @Test
    void hierarchicalListReturnsThemePrefixes() throws IOException {
        try (Storage storage = StorageFactory.open(baseUri, anonymous());
                Stream<StorageEntry> stream = storage.list("")) {
            Set<String> prefixes = stream.filter(e -> e instanceof StorageEntry.Prefix)
                    .map(StorageEntry::key)
                    .collect(java.util.stream.Collectors.toSet());
            // Overture's release layout has theme=addresses, theme=base, theme=buildings, theme=places,
            // theme=transportation, theme=divisions
            assertThat(prefixes).contains("theme=addresses/", "theme=base/", "theme=buildings/");
            assertThat(prefixes)
                    .allSatisfy(p -> assertThat(p).startsWith("theme=").endsWith("/"));
        }
    }

    @Test
    void statRoundTripAgreesWithList() throws IOException {
        try (Storage storage = StorageFactory.open(baseUri, anonymous())) {
            StorageEntry.File listed = firstParquetFile(storage);
            Optional<StorageEntry.File> stat = storage.stat(listed.key());
            assertThat(stat).isPresent();
            assertThat(stat.get().key()).isEqualTo(listed.key());
            assertThat(stat.get().size()).isEqualTo(listed.size());
        }
    }

    @Test
    void statReturnsEmptyForMissingKey() throws IOException {
        try (Storage storage = StorageFactory.open(baseUri, anonymous())) {
            assertThat(storage.stat("does/not/exist.parquet")).isEmpty();
            assertThat(storage.exists("does/not/exist.parquet")).isFalse();
        }
    }

    @Test
    void openRangeReaderThrowsNotFoundForMissingKey() throws IOException {
        try (Storage storage = StorageFactory.open(baseUri, anonymous())) {
            assertThatThrownBy(() -> storage.openRangeReader("does/not/exist.parquet"))
                    .isInstanceOf(NotFoundException.class);
        }
    }

    @Test
    void rangeReaderReadsParquetMagicAtHeaderAndFooter() throws IOException {
        try (Storage storage = StorageFactory.open(baseUri, anonymous())) {
            StorageEntry.File file = firstParquetFile(storage);
            assertThat(file.size()).isGreaterThan(8L);
            try (RangeReader reader = storage.openRangeReader(file.key())) {
                assertThat(magicAt(reader, 0)).isEqualTo("PAR1");
                assertThat(magicAt(reader, file.size() - 4)).isEqualTo("PAR1");
            }
        }
    }

    @Test
    void openInputStreamWithOffsetReadsFooterMagic() throws IOException {
        try (Storage storage = StorageFactory.open(baseUri, anonymous())) {
            StorageEntry.File file = firstParquetFile(storage);
            ReadOptions opts = ReadOptions.range(file.size() - 4, 4);
            try (ReadHandle r = storage.read(file.key(), opts)) {
                byte[] tail = r.content().readAllBytes();
                assertThat(tail).hasSize(4);
                assertThat(new String(tail, StandardCharsets.US_ASCII)).isEqualTo("PAR1");
            }
        }
    }

    @Test
    void paginationAcrossMultiplePages() throws IOException {
        // pageSize=10, limit=40 forces at least 4 pages; tolerates releases where addresses has
        // fewer overall files by asserting only that the page boundary was crossed.
        try (Storage storage = StorageFactory.open(baseUri, anonymous())) {
            ListOptions opts = ListOptions.builder().pageSize(10).build();
            try (Stream<StorageEntry> stream = storage.list("theme=addresses/**/*.parquet", opts)) {
                long count = stream.filter(e -> e instanceof StorageEntry.File)
                        .limit(40)
                        .count();
                assertThat(count).isGreaterThan(10L); // crossed at least one page boundary
            }
        }
    }

    private static StorageEntry.File firstParquetFile(Storage storage) {
        try (Stream<StorageEntry> stream = storage.list("theme=addresses/**/*.parquet")) {
            return stream.filter(e -> e instanceof StorageEntry.File f && f.size() > 8L)
                    .map(e -> (StorageEntry.File) e)
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("no parquet files found in theme=addresses/"));
        }
    }

    private static String magicAt(RangeReader reader, long offset) {
        ByteBuffer buf = reader.readRange(offset, 4);
        buf.flip();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        return new String(bytes, StandardCharsets.US_ASCII);
    }

    /**
     * Properties for unsigned access to the public Overture Maps bucket. Without this the SDK falls through to the
     * default credential chain (env vars, ~/.aws/credentials, IMDS, etc.) and fails on a clean CI box.
     */
    private static Properties anonymous() {
        Properties props = new Properties();
        props.setProperty(S3StorageProvider.S3_ANONYMOUS.key(), "true");
        return props;
    }
}
