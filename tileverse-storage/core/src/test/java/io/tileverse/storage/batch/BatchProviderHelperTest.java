/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tileverse.storage.batch;

import static org.assertj.core.api.Assertions.assertThat;

import io.tileverse.storage.StorageConfig;
import io.tileverse.storage.StorageParameter;
import java.net.URI;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Resolution of the {@code storage.batch.*} parameters: the parameter on the config, else the computed default. */
class BatchProviderHelperTest {

    private static final URI BASE = URI.create("s3://bucket/prefix/");

    @Test
    void explicitParametersWinOverTheComputedDefaults() {
        StorageConfig config = new StorageConfig(BASE)
                .setParameter(BatchProviderHelper.BATCH_MAX_GAP, 100)
                .setParameter(BatchProviderHelper.BATCH_MAX_FETCH, 2048)
                .setParameter(BatchProviderHelper.BATCH_MAX_IN_FLIGHT_FETCHES, 2);

        assertThat(BatchProviderHelper.objectStoreSettings(config)).isEqualTo(new BatchSettings(100, 2048, 2));
    }

    @Test
    void withoutParametersTheComputedDefaultsApply() {
        StorageConfig config = new StorageConfig(BASE);

        BatchSettings objectStore = BatchProviderHelper.objectStoreSettings(config);
        BatchSettings http = BatchProviderHelper.httpSettings(config);

        assertThat(objectStore.maxGapBytes()).isBetween(340_000, 360_000);
        assertThat(http.maxGapBytes()).isBetween(225_000, 240_000);
        assertThat(objectStore.maxFetchBytes()).isEqualTo(CoalescingPolicy.DEFAULT_MAX_FETCH_BYTES);
        assertThat(objectStore.maxInFlightFetches()).isEqualTo(BatchSettings.DEFAULT_MAX_IN_FLIGHT_FETCHES);
    }

    @Test
    void stringValuesFromPropertiesConvert() {
        StorageConfig config = new StorageConfig(BASE)
                .setParameter(BatchProviderHelper.BATCH_MAX_GAP.key(), "-1")
                .setParameter(BatchProviderHelper.BATCH_MAX_IN_FLIGHT_FETCHES.key(), "0");

        BatchSettings settings = BatchProviderHelper.httpSettings(config);

        assertThat(settings.maxGapBytes()).isEqualTo(-1);
        assertThat(settings.maxInFlightFetches()).isZero();
    }

    @Test
    void batchParametersFollowTheProviderOwnParameters() {
        StorageParameter<String> own = StorageParameter.builder()
                .key("storage.test.own")
                .title("own")
                .description("own")
                .type(String.class)
                .group("test")
                .build();

        List<StorageParameter<?>> params = BatchProviderHelper.withBatchParameters(List.of(own));

        assertThat(params)
                .containsExactly(
                        own,
                        BatchProviderHelper.BATCH_MAX_GAP,
                        BatchProviderHelper.BATCH_MAX_FETCH,
                        BatchProviderHelper.BATCH_MAX_IN_FLIGHT_FETCHES)
                .filteredOn(param -> param != own)
                .allSatisfy(param -> assertThat(param.group()).isEqualTo(StorageParameter.GROUP_BATCH));
    }
}
