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
package io.tileverse.rangereader.azure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.tileverse.rangereader.spi.RangeReaderConfig;
import io.tileverse.rangereader.spi.RangeReaderParameter;
import io.tileverse.rangereader.spi.RangeReaderProvider;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AzureBlobRangeReaderProviderTest {

    private final AzureBlobRangeReaderProvider provider = new AzureBlobRangeReaderProvider();

    @Test
    void testFactoryLookup() {
        assertThat(RangeReaderProvider.findProvider(AzureBlobRangeReaderProvider.ID))
                .isPresent();
        assertThat(RangeReaderProvider.getProvider(AzureBlobRangeReaderProvider.ID, true))
                .isNotNull();
    }

    @Test
    void testBuildParameters() {
        List<RangeReaderParameter<?>> parameters = provider.buildParameters();
        assertThat(parameters)
                .containsExactly(
                        AzureBlobRangeReaderProvider.AZURE_BLOB_NAME,
                        AzureBlobRangeReaderProvider.AZURE_ACCOUNT_KEY,
                        AzureBlobRangeReaderProvider.AZURE_SAS_TOKEN);
    }

    @Test
    void testCanProcess() {
        RangeReaderConfig config = provider.getDefaultConfig();

        assertThrows(NullPointerException.class, () -> provider.canProcess(null));

        assertThat(provider.canProcess(config.uri("https://account.blob.core.windows.net/container/blob.pmtiles")))
                .isTrue();
        assertThat(provider.canProcess(config.uri("http://127.0.0.1:10000/account/container/blob.pmtiles")))
                .isTrue();

        assertThat(provider.canProcess(config.uri("https://account.blob.core.windows.net/container")))
                .isFalse();
        assertThat(provider.canProcess(config.uri("https://account.blob.core.windows.net/container")
                        .setParameter(AzureBlobRangeReaderProvider.AZURE_BLOB_NAME.key(), "root-blob.pmtiles")))
                .isTrue();

        assertThat(provider.canProcess(config.uri("ftp://account.blob.core.windows.net/container/blob.pmtiles")))
                .isFalse();
        assertThat(provider.canProcess(config.uri("https://example.com"))).isFalse();
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
