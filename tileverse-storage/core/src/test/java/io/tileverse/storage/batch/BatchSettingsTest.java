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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class BatchSettingsTest {

    @Test
    void rejectsANonPositiveFetchCap() {
        assertThatThrownBy(() -> new BatchSettings(0, 0, 8)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BatchSettings(0, -1, 8)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsANegativeInFlightBound() {
        assertThatThrownBy(() -> new BatchSettings(0, 1024, -1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void negativeGapMeansNoMerging() {
        BatchSettings settings = new BatchSettings(-1, 1024, 8);
        assertThat(settings.coalescingPolicy().maxGapBytes()).isNegative();
    }

    @Test
    void zeroInFlightRemovesTheBound() {
        assertThat(new BatchSettings(0, 1024, 0).concurrencyCap()).isEqualTo(Integer.MAX_VALUE);
        assertThat(new BatchSettings(0, 1024, 3).concurrencyCap()).isEqualTo(3);
    }

    @Test
    void coalescingPolicyReflectsTheGapAndTheFetchCap() {
        assertThat(new BatchSettings(100, 2048, 8).coalescingPolicy()).isEqualTo(new CoalescingPolicy(100, 2048));
    }

    @Test
    void defaultsCombineTheBackendPolicyWithEightInFlight() {
        assertThat(BatchSettings.objectStoreDefaults())
                .isEqualTo(BatchSettings.of(CoalescingPolicy.objectStoreDefaults(), 8));
        assertThat(BatchSettings.httpDefaults()).isEqualTo(BatchSettings.of(CoalescingPolicy.httpDefaults(), 8));
    }
}
