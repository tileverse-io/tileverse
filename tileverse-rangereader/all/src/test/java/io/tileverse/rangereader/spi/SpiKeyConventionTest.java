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
package io.tileverse.rangereader.spi;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Guard test: every {@link RangeReaderParameter} declared by any {@link RangeReaderProvider}
 * on the classpath must use the canonical {@value RangeReaderConfig#KEY_PREFIX} prefix and must
 * NOT use the legacy {@value RangeReaderConfig#LEGACY_KEY_PREFIX} prefix.
 */
class SpiKeyConventionTest {

    @Test
    void allProviderParameterKeysUseStoragePrefix() {
        List<RangeReaderProvider> providers = RangeReaderProvider.getProviders();
        assertThat(providers).as("expected SPI providers to be discovered").isNotEmpty();

        for (RangeReaderProvider provider : providers) {
            for (RangeReaderParameter<?> p : provider.getParameters()) {
                assertThat(p.key())
                        .as("%s parameter key must start with %s", provider.getId(), RangeReaderConfig.KEY_PREFIX)
                        .startsWith(RangeReaderConfig.KEY_PREFIX);
                assertThat(p.key())
                        .as(
                                "%s parameter key must not use the legacy %s prefix",
                                provider.getId(), RangeReaderConfig.LEGACY_KEY_PREFIX)
                        .doesNotStartWith(RangeReaderConfig.LEGACY_KEY_PREFIX);
            }
        }
    }
}
