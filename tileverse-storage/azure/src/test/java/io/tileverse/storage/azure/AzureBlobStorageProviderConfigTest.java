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
package io.tileverse.storage.azure;

import static org.assertj.core.api.Assertions.assertThat;

import io.tileverse.storage.StorageConfig;
import java.net.URI;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Verifies {@link AzureBlobStorageProvider#keyFor(StorageConfig, AzureBlobLocation)} resolution: account-key, SAS,
 * connection-string, and retry-tuning parameters all flow into the cache discriminator. Replaces the reflection-based
 * {@code testBuilderSettersPopulateFields} that lived in the now-removed {@code AzureBlobRangeReader.Builder}.
 */
class AzureBlobStorageProviderConfigTest {

    private static final URI BLOB_URI =
            URI.create("https://devstoreaccount1.blob.core.windows.net/my-container/file.bin");

    private static AzureBlobLocation locationOf(URI uri) {
        return AzureBlobLocation.parse(uri);
    }

    @Test
    void accountKey_carriedOnTheKey() {
        StorageConfig config =
                new StorageConfig(BLOB_URI).setParameter(AzureBlobStorageProvider.AZURE_ACCOUNT_KEY, "base64-secret");

        assertThat(AzureBlobStorageProvider.keyFor(config, locationOf(BLOB_URI)).accountKey())
                .hasValue("base64-secret");
    }

    @Test
    void sasToken_carriedOnTheKey() {
        StorageConfig config = new StorageConfig(BLOB_URI)
                .setParameter(AzureBlobStorageProvider.AZURE_SAS_TOKEN, "sv=2020-08-04&sig=abc");

        assertThat(AzureBlobStorageProvider.keyFor(config, locationOf(BLOB_URI)).sasToken())
                .hasValue("sv=2020-08-04&sig=abc");
    }

    @Test
    void connectionString_carriedOnTheKey() {
        StorageConfig config = new StorageConfig(BLOB_URI)
                .setParameter(
                        AzureBlobStorageProvider.AZURE_CONNECTION_STRING,
                        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=base64;EndpointSuffix=localhost");

        assertThat(AzureBlobStorageProvider.keyFor(config, locationOf(BLOB_URI)).connectionString())
                .isPresent();
    }

    @Test
    void noCredentials_resultInEmptyOptionals() {
        StorageConfig config = new StorageConfig(BLOB_URI);
        AzureClientCache.Key key = AzureBlobStorageProvider.keyFor(config, locationOf(BLOB_URI));

        assertThat(key.accountKey()).isEmpty();
        assertThat(key.sasToken()).isEmpty();
        assertThat(key.connectionString()).isEmpty();
        assertThat(key.anonymous()).isFalse();
    }

    @Test
    void anonymousFlag_carriedOnTheKey() {
        StorageConfig config = new StorageConfig(BLOB_URI).setParameter(AzureBlobStorageProvider.AZURE_ANONYMOUS, true);

        assertThat(AzureBlobStorageProvider.keyFor(config, locationOf(BLOB_URI)).anonymous())
                .isTrue();
    }

    @Test
    void differentAnonymousFlagsProduceDifferentCacheKeys() {
        StorageConfig a = new StorageConfig(BLOB_URI);
        StorageConfig b = new StorageConfig(BLOB_URI).setParameter(AzureBlobStorageProvider.AZURE_ANONYMOUS, true);

        assertThat(AzureBlobStorageProvider.keyFor(a, locationOf(BLOB_URI)))
                .isNotEqualTo(AzureBlobStorageProvider.keyFor(b, locationOf(BLOB_URI)));
    }

    @Test
    void retryDefaults_appliedWhenAbsent() {
        StorageConfig config = new StorageConfig(BLOB_URI);
        AzureRetryConfig retry =
                AzureBlobStorageProvider.keyFor(config, locationOf(BLOB_URI)).retry();

        assertThat(retry.maxTries()).isEqualTo(3);
        assertThat(retry.retryDelay()).isEqualTo(Duration.ofSeconds(4));
        assertThat(retry.maxRetryDelay()).isEqualTo(Duration.ofMinutes(2));
        assertThat(retry.tryTimeout()).isEqualTo(Duration.ofSeconds(60));
    }

    @Test
    void retryParameters_overrideDefaults() {
        StorageConfig config = new StorageConfig(BLOB_URI)
                .setParameter(AzureBlobStorageProvider.AZURE_MAX_RETRIES, 5)
                .setParameter(AzureBlobStorageProvider.AZURE_RETRY_DELAY, Duration.ofSeconds(10))
                .setParameter(AzureBlobStorageProvider.AZURE_MAX_RETRY_DELAY, Duration.ofMinutes(5))
                .setParameter(AzureBlobStorageProvider.AZURE_TRY_TIMEOUT, Duration.ofSeconds(120));

        AzureRetryConfig retry =
                AzureBlobStorageProvider.keyFor(config, locationOf(BLOB_URI)).retry();
        assertThat(retry.maxTries()).isEqualTo(5);
        assertThat(retry.retryDelay()).isEqualTo(Duration.ofSeconds(10));
        assertThat(retry.maxRetryDelay()).isEqualTo(Duration.ofMinutes(5));
        assertThat(retry.tryTimeout()).isEqualTo(Duration.ofSeconds(120));
    }

    @Test
    void differentRetryConfigsProduceDifferentCacheKeys() {
        StorageConfig a = new StorageConfig(BLOB_URI);
        StorageConfig b = new StorageConfig(BLOB_URI).setParameter(AzureBlobStorageProvider.AZURE_MAX_RETRIES, 5);

        assertThat(AzureBlobStorageProvider.keyFor(a, locationOf(BLOB_URI)))
                .isNotEqualTo(AzureBlobStorageProvider.keyFor(b, locationOf(BLOB_URI)));
    }

    @Test
    void differentAccountKeysProduceDifferentCacheKeys() {
        StorageConfig a = new StorageConfig(BLOB_URI).setParameter(AzureBlobStorageProvider.AZURE_ACCOUNT_KEY, "key-a");
        StorageConfig b = new StorageConfig(BLOB_URI).setParameter(AzureBlobStorageProvider.AZURE_ACCOUNT_KEY, "key-b");

        assertThat(AzureBlobStorageProvider.keyFor(a, locationOf(BLOB_URI)))
                .isNotEqualTo(AzureBlobStorageProvider.keyFor(b, locationOf(BLOB_URI)));
    }
}
