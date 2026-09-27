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
