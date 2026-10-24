/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
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
