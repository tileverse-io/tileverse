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

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

class CoalescingPolicyTest {

    @Test
    void noneDisablesMergingViaNegativeGap() {
        assertThat(CoalescingPolicy.NONE.maxGapBytes()).isNegative();
        assertThat(CoalescingPolicy.NONE.maxFetchBytes()).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void constructorRejectsNonPositiveMaxFetch() {
        assertThatThrownBy(() -> new CoalescingPolicy(0, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CoalescingPolicy(0, -5)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void fromNetworkMetricsComputesTheGapBudget() {
        CoalescingPolicy policy = CoalescingPolicy.fromNetworkMetrics(Duration.ofMillis(30), 100L * 1024 * 1024, 0.9);
        assertThat(policy.maxGapBytes()).isBetween(340_000, 360_000);
        assertThat(policy.maxFetchBytes()).isEqualTo(CoalescingPolicy.DEFAULT_MAX_FETCH_BYTES);
    }

    @Test
    void fromNetworkMetricsValidatesInputs() {
        assertThatThrownBy(() -> CoalescingPolicy.fromNetworkMetrics(null, 1000, 0.9))
                .isInstanceOf(NullPointerException.class);
        Duration negative = Duration.ofMillis(-1);
        assertThatThrownBy(() -> CoalescingPolicy.fromNetworkMetrics(negative, 1000, 0.9))
                .isInstanceOf(IllegalArgumentException.class);
        Duration one = Duration.ofMillis(1);
        assertThatThrownBy(() -> CoalescingPolicy.fromNetworkMetrics(one, 0, 0.9))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CoalescingPolicy.fromNetworkMetrics(one, 1000, 0.0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CoalescingPolicy.fromNetworkMetrics(one, 1000, 1.0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @ResourceLock(Resources.SYSTEM_PROPERTIES)
    void objectStoreAndHttpDefaultsDifferByLatency() {
        assertThat(CoalescingPolicy.objectStoreDefaults().maxGapBytes()).isBetween(340_000, 360_000);
        assertThat(CoalescingPolicy.httpDefaults().maxGapBytes()).isBetween(225_000, 240_000);
        assertThat(CoalescingPolicy.objectStoreDefaults().maxFetchBytes())
                .isEqualTo(CoalescingPolicy.DEFAULT_MAX_FETCH_BYTES);
    }

    @Test
    @ResourceLock(Resources.SYSTEM_PROPERTIES)
    void systemPropertiesOverrideTheDefaults() {
        System.setProperty("io.tileverse.storage.batch.objectstore.maxgap", "1024");
        System.setProperty("io.tileverse.storage.batch.http.maxgap", "2048");
        System.setProperty("io.tileverse.storage.batch.maxfetch", "4096");
        try {
            assertThat(CoalescingPolicy.objectStoreDefaults()).isEqualTo(new CoalescingPolicy(1024, 4096));
            assertThat(CoalescingPolicy.httpDefaults()).isEqualTo(new CoalescingPolicy(2048, 4096));
        } finally {
            System.clearProperty("io.tileverse.storage.batch.objectstore.maxgap");
            System.clearProperty("io.tileverse.storage.batch.http.maxgap");
            System.clearProperty("io.tileverse.storage.batch.maxfetch");
        }
    }
}
