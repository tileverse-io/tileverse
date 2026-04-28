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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link RangeReaderConfig} accepts the {@code storage.*} parameter prefix used by tileverse 2.x and
 * translates it to the canonical {@code io.tileverse.rangereader.*} prefix used by 1.x. This forward compatibility lets
 * a tileverse 1.4+ runtime read configuration persisted by a future 2.x consumer.
 */
class RangeReaderConfigForwardCompatTest {

    @Test
    void normalizeKeyTranslatesFuturePrefix() {
        assertThat(RangeReaderConfig.normalizeKey("storage.s3.region")).isEqualTo("io.tileverse.rangereader.s3.region");
        assertThat(RangeReaderConfig.normalizeKey("storage.uri")).isEqualTo("io.tileverse.rangereader.uri");
        assertThat(RangeReaderConfig.normalizeKey("storage.provider")).isEqualTo("io.tileverse.rangereader.provider");
    }

    @Test
    void normalizeKeyLeavesCanonicalKeysUnchanged() {
        assertThat(RangeReaderConfig.normalizeKey("io.tileverse.rangereader.s3.region"))
                .isEqualTo("io.tileverse.rangereader.s3.region");
        assertThat(RangeReaderConfig.normalizeKey("custom.unrelated.key")).isEqualTo("custom.unrelated.key");
    }

    @Test
    void normalizeKeyHandlesNull() {
        assertThat(RangeReaderConfig.normalizeKey(null)).isNull();
    }

    @Test
    void normalizeKeysRewritesAllFutureEntries() {
        Map<String, Object> in = new LinkedHashMap<>();
        in.put("storage.uri", "file:///tmp/x.pmtiles");
        in.put("storage.provider", "file");
        in.put("storage.caching.enabled", Boolean.TRUE);
        in.put("io.tileverse.rangereader.s3.region", "us-west-2");

        Map<String, Object> out = RangeReaderConfig.normalizeKeys(in);

        assertThat(out)
                .containsEntry("io.tileverse.rangereader.uri", "file:///tmp/x.pmtiles")
                .containsEntry("io.tileverse.rangereader.provider", "file")
                .containsEntry("io.tileverse.rangereader.caching.enabled", Boolean.TRUE)
                .containsEntry("io.tileverse.rangereader.s3.region", "us-west-2")
                .doesNotContainKey("storage.uri")
                .doesNotContainKey("storage.provider")
                .doesNotContainKey("storage.caching.enabled");
    }

    @Test
    void setParameterStoresUnderCanonicalKey() {
        RangeReaderConfig config = new RangeReaderConfig().setParameter("storage.s3.region", "us-east-1");

        assertThat(config.getParameter("io.tileverse.rangereader.s3.region", String.class))
                .contains("us-east-1");
        assertThat(config.getParameter("storage.s3.region", String.class)).contains("us-east-1");
    }

    @Test
    void setParameterWithFutureProviderKeyUpdatesProviderId() {
        RangeReaderConfig config = new RangeReaderConfig().setParameter("storage.provider", "s3");

        assertThat(config.providerId()).contains("s3");
    }

    @Test
    void fromPropertiesAcceptsFutureUriAndProviderKeys() {
        Properties props = new Properties();
        props.setProperty("storage.uri", "file:///tmp/x.pmtiles");
        props.setProperty("storage.provider", "file");
        props.setProperty("storage.caching.enabled", "true");

        RangeReaderConfig config = RangeReaderConfig.fromProperties(props);

        assertThat(config.uri()).hasToString("file:///tmp/x.pmtiles");
        assertThat(config.providerId()).contains("file");
        assertThat(config.getParameter("io.tileverse.rangereader.caching.enabled", Boolean.class))
                .contains(Boolean.TRUE);
    }

    @Test
    void fromPropertiesPrefersCanonicalUriWhenBothFormsPresent() {
        Properties props = new Properties();
        props.setProperty("io.tileverse.rangereader.uri", "file:///tmp/canonical.pmtiles");
        props.setProperty("storage.uri", "file:///tmp/future.pmtiles");

        RangeReaderConfig config = RangeReaderConfig.fromProperties(props);

        assertThat(config.uri()).hasToString("file:///tmp/canonical.pmtiles");
    }

    @Test
    void toPropertiesEmitsCanonicalKeysOnly() {
        RangeReaderConfig config = new RangeReaderConfig()
                .uri("file:///tmp/x.pmtiles")
                .setParameter("storage.s3.region", "us-east-1")
                .setParameter("storage.caching.enabled", "true");

        Properties out = config.toProperties();

        assertThat(out.stringPropertyNames())
                .contains("io.tileverse.rangereader.uri")
                .contains("io.tileverse.rangereader.s3.region")
                .contains("io.tileverse.rangereader.caching.enabled")
                .noneMatch(k -> k.startsWith("storage."));
    }
}
