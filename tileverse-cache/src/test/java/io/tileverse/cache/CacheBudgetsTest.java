/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CacheBudgetsTest {

    @Test
    void fromSettingsReadsBothPools() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CacheSettings.DATA_MAX_SIZE, "25%");
        properties.put(CacheSettings.METADATA_MAX_SIZE, "64M");
        CacheSettings settings = new CacheSettings(properties::get, variable -> null, 1_000_000L);

        CacheBudgets budgets = CacheBudgets.fromSettings(settings);

        assertThat(budgets).isEqualTo(new CacheBudgets(250_000L, 67_108_864L));
        assertThat(budgets.of(CachePool.DATA)).isEqualTo(250_000L);
        assertThat(budgets.of(CachePool.METADATA)).isEqualTo(67_108_864L);
    }

    @Test
    void negativeBudgetRejected() {
        assertThatThrownBy(() -> new CacheBudgets(-1, 0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void poolNamesAreReserved() {
        assertThat(CachePool.DATA.cacheName()).isEqualTo("tileverse-cache-data");
        assertThat(CachePool.METADATA.cacheName()).isEqualTo("tileverse-cache-metadata");
    }
}
