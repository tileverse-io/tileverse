/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.storage.azure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tileverse.storage.StorageConfig;
import io.tileverse.storage.StorageParameter;
import io.tileverse.storage.spi.StorageProvider;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AzureBlobStorageProviderTest {

    private final AzureBlobStorageProvider provider = new AzureBlobStorageProvider();

    @Test
    void canProcessAcceptsContainerOnlyUri() {
        assertThat(provider.canProcess(new StorageConfig("https://acct.blob.core.windows.net/my-container")))
                .isTrue();
        assertThat(provider.canProcess(new StorageConfig("https://acct.blob.core.windows.net/my-container/prefix/")))
                .isTrue();
        assertThat(provider.canProcess(new StorageConfig("https://acct.blob.core.windows.net/my-container/blob.bin")))
                .isTrue();
    }

    @Test
    void canProcessRejectsHostWithoutContainer() {
        assertThat(provider.canProcess(new StorageConfig("https://acct.blob.core.windows.net/")))
                .isFalse();
    }

    @Test
    void testFactoryLookup() {
        assertThat(StorageProvider.findProvider(AzureBlobStorageProvider.ID)).isPresent();
        assertThat(StorageProvider.getProvider(AzureBlobStorageProvider.ID, true))
                .isNotNull();
    }

    @Test
    void testBuildParameters() {
        List<StorageParameter<?>> parameters = provider.buildParameters();
        assertThat(parameters)
                .containsExactly(
                        AzureBlobStorageProvider.AZURE_BLOB_NAME,
                        AzureBlobStorageProvider.AZURE_ANONYMOUS,
                        AzureBlobStorageProvider.AZURE_ACCOUNT_KEY,
                        AzureBlobStorageProvider.AZURE_SAS_TOKEN,
                        AzureBlobStorageProvider.AZURE_CONNECTION_STRING,
                        AzureBlobStorageProvider.AZURE_MAX_RETRIES,
                        AzureBlobStorageProvider.AZURE_RETRY_DELAY,
                        AzureBlobStorageProvider.AZURE_MAX_RETRY_DELAY,
                        AzureBlobStorageProvider.AZURE_TRY_TIMEOUT);
    }

    @Test
    void testCanProcess() {
        StorageConfig config = provider.getDefaultConfig();

        assertThatThrownBy(() -> provider.canProcess(null)).isInstanceOf(NullPointerException.class);

        assertThat(provider.canProcess(config.baseUri("https://account.blob.core.windows.net/container/blob.pmtiles")))
                .isTrue();
        assertThat(provider.canProcess(config.baseUri("http://127.0.0.1:10000/account/container/blob.pmtiles")))
                .isTrue();

        // Container-only URIs on real Azure blob hosts are now accepted (Storage use case).
        // The AZURE_BLOB_NAME parameter is no longer required to claim these URIs.
        assertThat(provider.canProcess(config.baseUri("https://account.blob.core.windows.net/container")))
                .isTrue();
        assertThat(provider.canProcess(config.baseUri("https://account.blob.core.windows.net/container")
                        .setParameter(AzureBlobStorageProvider.AZURE_BLOB_NAME.key(), "root-blob.pmtiles")))
                .isTrue();

        assertThat(provider.canProcess(config.baseUri("ftp://account.blob.core.windows.net/container/blob.pmtiles")))
                .isFalse();
        assertThat(provider.canProcess(config.baseUri("https://example.com"))).isFalse();
    }

    @Test
    void canProcessAcceptsAzScheme() {
        assertThat(provider.canProcess(new StorageConfig("az://overturemapswestus2/release")))
                .isTrue();
        assertThat(provider.canProcess(new StorageConfig("az://overturemapswestus2/release/2026-03-18.0/")))
                .isTrue();
        // Account without container is rejected
        assertThat(provider.canProcess(new StorageConfig("az://overturemapswestus2")))
                .isFalse();
        assertThat(provider.canProcess(new StorageConfig("az://overturemapswestus2/")))
                .isFalse();
    }

    @Test
    void azSchemeRoundTripsThroughLocationParser() {
        AzureBlobLocation loc = AzureBlobLocation.parse(URI.create("az://overturemapswestus2/release/2026-03-18.0/"));
        assertThat(loc.accountName()).isEqualTo("overturemapswestus2");
        assertThat(loc.container()).isEqualTo("release");
        assertThat(loc.prefix()).isEqualTo("2026-03-18.0/");
        assertThat(loc.endpoint()).isEqualTo("https://overturemapswestus2.blob.core.windows.net");
    }

    @Test
    void azSchemeWithoutPrefixYieldsEmptyPrefix() {
        AzureBlobLocation loc = AzureBlobLocation.parse(URI.create("az://acct/container"));
        assertThat(loc.accountName()).isEqualTo("acct");
        assertThat(loc.container()).isEqualTo("container");
        assertThat(loc.prefix()).isEmpty();
    }

    @Test
    void testCanProcessHeaders() {
        assertThat(provider.canProcessHeaders(
                        URI.create("https://account.blob.core.windows.net/container/blob.pmtiles"),
                        Map.of("x-ms-request-id", List.of("123"))))
                .isTrue();
        assertThat(provider.canProcessHeaders(
                        URI.create("https://account.blob.core.windows.net/container/blob.pmtiles"),
                        Map.of("x-custom", List.of("value"))))
                .isFalse();
    }
}
