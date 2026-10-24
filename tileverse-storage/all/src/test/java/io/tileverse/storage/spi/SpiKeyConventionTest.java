/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.spi;

import static org.assertj.core.api.Assertions.assertThat;

import io.tileverse.storage.StorageConfig;
import io.tileverse.storage.StorageParameter;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Guard test: every {@link StorageParameter} declared by any {@link StorageProvider} on the classpath must use the
 * canonical {@value StorageConfig#KEY_PREFIX} prefix and must NOT use the legacy
 * {@value StorageConfig#LEGACY_KEY_PREFIX} prefix.
 */
class SpiKeyConventionTest {

    @Test
    void allProviderParameterKeysUseStoragePrefix() {
        List<StorageProvider> providers = StorageProvider.getProviders();
        assertThat(providers).as("expected SPI providers to be discovered").isNotEmpty();

        for (StorageProvider provider : providers) {
            for (StorageParameter<?> p : provider.getParameters()) {
                assertThat(p.key())
                        .as("%s parameter key must start with %s", provider.getId(), StorageConfig.KEY_PREFIX)
                        .startsWith(StorageConfig.KEY_PREFIX);
                assertThat(p.key())
                        .as(
                                "%s parameter key must not use the legacy %s prefix",
                                provider.getId(), StorageConfig.LEGACY_KEY_PREFIX)
                        .doesNotStartWith(StorageConfig.LEGACY_KEY_PREFIX);
            }
        }
    }
}
