/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageConfig;
import io.tileverse.storage.batch.BatchProviderHelper;
import io.tileverse.storage.batch.BatchSettings;
import java.io.IOException;
import java.net.URI;
import org.junit.jupiter.api.Test;

/**
 * The {@code storage.batch.*} parameters of a config reach every reader the S3 Storage opens. Opening a reader issues
 * no request, which lets these tests build real SDK clients against a bucket that does not exist.
 */
class S3StorageBatchSettingsTest {

    private final S3StorageProvider provider = new S3StorageProvider();

    @Test
    void explicitParametersReachTheReader() throws IOException {
        StorageConfig config = anonymousConfig()
                .setParameter(BatchProviderHelper.BATCH_MAX_GAP, 100)
                .setParameter(BatchProviderHelper.BATCH_MAX_FETCH, 2048)
                .setParameter(BatchProviderHelper.BATCH_MAX_IN_FLIGHT_FETCHES, 2);

        assertThat(settingsOf(config)).isEqualTo(new BatchSettings(100, 2048, 2));
    }

    @Test
    void withoutParametersTheObjectStoreDefaultsReachTheReader() throws IOException {
        assertThat(settingsOf(anonymousConfig())).isEqualTo(BatchSettings.objectStoreDefaults());
    }

    @Test
    void anInvalidParameterFailsTheOpenWithoutKeepingAClientSet() {
        S3ClientCache clientCache = new S3ClientCache();
        S3StorageProvider leasingProvider =
                new S3StorageProvider(clientCache, EnvironmentEndpoint.INSTANCE, EnvironmentRegion.INSTANCE);
        StorageConfig config = anonymousConfig().setParameter(BatchProviderHelper.BATCH_MAX_FETCH, 0);

        assertThatThrownBy(() -> leasingProvider.createStorage(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxFetchBytes");

        assertThat(clientCache.entryCount()).isZero();
    }

    @Test
    void providerDeclaresTheBatchParameters() {
        assertThat(provider.getParameters()).containsAll(BatchProviderHelper.configParameters());
    }

    private static StorageConfig anonymousConfig() {
        return new StorageConfig(URI.create("s3://bucket/prefix/")).setParameter(S3StorageProvider.S3_ANONYMOUS, true);
    }

    private BatchSettings settingsOf(StorageConfig config) throws IOException {
        try (Storage storage = provider.createStorage(config);
                RangeReader reader = storage.openRangeReader("tiles/file.pmtiles")) {
            return ((S3RangeReader) reader).batchSettings();
        }
    }
}
