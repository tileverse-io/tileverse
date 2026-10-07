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
package io.tileverse.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tileverse.storage.http.HttpStorageProvider;
import io.tileverse.storage.spi.StorageProvider;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.util.ReadsSystemProperty;
import org.junit.jupiter.api.util.SetSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Provider selection from the config alone: no test here needs a network or a container.
 *
 * <p>Provider availability is read from system properties, and tests run concurrently: the read lock keeps these
 * selections from running while {@link #anHttpUrlFindsNoProviderWhenTheHttpProviderIsDisabled()} holds the write lock.
 */
@ReadsSystemProperty
class StorageProviderSelectionTest {

    @ParameterizedTest(name = "{0} -> {1}")
    @MethodSource("schemeSelections")
    void theUriSchemeAloneSelectsTheProvider(String uri, String expectedProviderId) {
        StorageConfig config = new StorageConfig(uri);

        StorageProvider selected = StorageFactory.findProvider(config);

        assertThat(selected.getId()).isEqualTo(expectedProviderId);
    }

    static Stream<Arguments> schemeSelections() {
        return Stream.of(
                Arguments.of("s3://overturemaps-us-west-2/release/", "s3"),
                Arguments.of("gs://bucket/key", "gcs"),
                Arguments.of("az://account/container/blob", "azure"),
                Arguments.of("abfs://fs@account.dfs.core.windows.net/path", "azure-datalake"),
                Arguments.of("abfss://fs@account.dfs.core.windows.net/path", "azure-datalake"),
                Arguments.of("file:///data/x.tif", "file"),
                Arguments.of("/data/x.tif", "file"),
                Arguments.of("https://overturemaps-us-west-2.s3.amazonaws.com/release/x.parquet", "http"),
                Arguments.of("https://s3.us-west-2.amazonaws.com/overturemaps-us-west-2/release/x.parquet", "http"),
                Arguments.of("https://overturemaps-us-west-2.s3.amazonaws.com/", "http"),
                Arguments.of("https://overturemapswestus2.blob.core.windows.net/release/x.parquet", "http"),
                Arguments.of("https://account.dfs.core.windows.net/fs/path", "http"),
                Arguments.of("https://storage.googleapis.com/gcp-public-data-landsat/index.csv.gz", "http"),
                Arguments.of("https://raw.githubusercontent.com/org/repo/main/file.bin", "http"),
                // nothing listens on the discard port, and the .invalid top-level domain never resolves
                Arguments.of("http://localhost:9/bucket/key.tif", "http"),
                Arguments.of("https://storage.invalid/bucket/key.tif", "http"),
                // signed URLs: only the HTTP backend sends the signature of the query string
                Arguments.of(
                        "https://bucket.s3.us-west-2.amazonaws.com/key.parquet"
                                + "?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=abc",
                        "http"),
                Arguments.of("https://account.blob.core.windows.net/container/blob.bin?sv=2020-08-04&sig=abc", "http"));
    }

    @ParameterizedTest(name = "{0} naming {1}")
    @MethodSource("namedProviders")
    void aNamedProviderServesItsHttpUrlForms(String uri, String providerId) {
        StorageConfig config = new StorageConfig(uri).providerId(providerId);

        StorageProvider selected = StorageFactory.findProvider(config);

        assertThat(selected.getId()).isEqualToIgnoringCase(providerId);
        assertThat(selected.canProcess(config))
                .as("canProcess of the named provider")
                .isTrue();
    }

    static Stream<Arguments> namedProviders() {
        return Stream.of(
                Arguments.of("http://minio:9000/bucket/dir/key.tif", "s3"),
                Arguments.of("https://bucket.s3.amazonaws.com/key.tif", "S3"),
                Arguments.of("http://localhost:10000/devstoreaccount1/container/blob.tif", "azure"),
                Arguments.of("https://account.dfs.core.windows.net/fs/path", "azure-datalake"),
                Arguments.of("http://localhost:4443/storage/v1/b/bucket/o/key.tif", "gcs"),
                Arguments.of("https://bucket.s3.amazonaws.com/key.tif", "http"));
    }

    /** A bucket root has no key: the factory serves it for the named provider without asking {@code canProcess}. */
    @Test
    void aNamedProviderIsSelectedForTheUrlOfABucketRoot() {
        StorageConfig config = new StorageConfig("http://minio:9000/bucket/").providerId("s3");

        StorageProvider selected = StorageFactory.findProvider(config);

        assertThat(selected.getId()).isEqualTo("s3");
    }

    @Test
    void theLegacyProviderKeyNamesTheProvider() {
        Properties props = new Properties();
        props.setProperty("io.tileverse.rangereader.uri", "http://minio:9000/bucket/dir/key.tif");
        props.setProperty("io.tileverse.rangereader.provider", "s3");
        StorageConfig config = StorageConfig.fromProperties(props);

        StorageProvider selected = StorageFactory.findProvider(config);

        assertThat(selected.getId()).isEqualTo("s3");
        assertThat(selected.canProcess(config))
                .as("canProcess of the named provider")
                .isTrue();
    }

    @Test
    void aBlankProviderIdLeavesTheSchemeToSelect() {
        Properties props = new Properties();
        props.setProperty("storage.uri", "https://bucket.s3.amazonaws.com/key.tif");
        props.setProperty("storage.provider", "");

        StorageProvider selected = StorageFactory.findProvider(StorageConfig.fromProperties(props));

        assertThat(selected.getId()).isEqualTo("http");
    }

    @Test
    @SetSystemProperty(key = HttpStorageProvider.ENABLED_KEY, value = "false")
    void anHttpUrlFindsNoProviderWhenTheHttpProviderIsDisabled() {
        StorageConfig config = new StorageConfig("https://bucket.s3.amazonaws.com/key.tif");

        assertThatThrownBy(() -> StorageFactory.findProvider(config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No suitable provider found");
    }

    @Test
    void anHttpUriIsResolvedWithoutContactingItsHost() throws IOException {
        InetAddress loopback = InetAddress.getByName("127.0.0.1");
        try (ServerSocket server = new ServerSocket(0, 1, loopback)) {
            server.setSoTimeout(250);
            URI uri = URI.create("http://127.0.0.1:%d/bucket/key.tif".formatted(server.getLocalPort()));

            StorageProvider selected = StorageFactory.findProvider(new StorageConfig(uri));

            assertThat(selected.getId()).isEqualTo("http");
            assertThatThrownBy(server::accept)
                    .as("a connection to the host of the URI")
                    .isInstanceOf(SocketTimeoutException.class);
        }
    }
}
