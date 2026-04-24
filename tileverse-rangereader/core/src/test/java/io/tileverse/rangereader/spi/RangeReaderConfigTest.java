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

import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/**
 * Forward-compatibility tests: the 1.3.x series must accept configuration keys written by the
 * 1.4 series (shaped as {@code storage.<group>.<name>}) so that data directories remain
 * interoperable in either direction.
 */
class RangeReaderConfigTest {

    @Test
    void normalizeKey_canonicalPrefix_passthrough() {
        assertThat(RangeReaderConfig.normalizeKey("io.tileverse.rangereader.s3.region"))
                .isEqualTo("io.tileverse.rangereader.s3.region");
        assertThat(RangeReaderConfig.normalizeKey("io.tileverse.rangereader.uri"))
                .isEqualTo("io.tileverse.rangereader.uri");
    }

    @Test
    void normalizeKey_storagePrefix_rewrittenToCanonical() {
        assertThat(RangeReaderConfig.normalizeKey("storage.s3.region")).isEqualTo("io.tileverse.rangereader.s3.region");
        assertThat(RangeReaderConfig.normalizeKey("storage.uri")).isEqualTo("io.tileverse.rangereader.uri");
        assertThat(RangeReaderConfig.normalizeKey("storage.provider")).isEqualTo("io.tileverse.rangereader.provider");
    }

    @Test
    void normalizeKey_unrelated_passthrough() {
        assertThat(RangeReaderConfig.normalizeKey("pmtiles")).isEqualTo("pmtiles");
        assertThat(RangeReaderConfig.normalizeKey("namespace")).isEqualTo("namespace");
    }

    @Test
    void normalizeKey_null_passthrough() {
        assertThat(RangeReaderConfig.normalizeKey(null)).isNull();
    }

    @Test
    void normalizeKeys_mapRewrite_preservesValues() {
        Map<String, Object> in = Map.of(
                "storage.s3.region", "us-west-2",
                "io.tileverse.rangereader.azure.blob-name", "foo.pmtiles",
                "pmtiles", "file:///tmp/x.pmtiles");
        Map<String, Object> out = RangeReaderConfig.normalizeKeys(in);
        assertThat(out)
                .containsOnlyKeys(
                        "io.tileverse.rangereader.s3.region", "io.tileverse.rangereader.azure.blob-name", "pmtiles");
        assertThat(out).containsEntry("io.tileverse.rangereader.s3.region", "us-west-2");
        assertThat(out).containsEntry("io.tileverse.rangereader.azure.blob-name", "foo.pmtiles");
        assertThat(out).containsEntry("pmtiles", "file:///tmp/x.pmtiles");
    }

    @Test
    void setParameter_storageKey_readableAsCanonical() {
        RangeReaderConfig config = new RangeReaderConfig().uri("file:///tmp/x.pmtiles");
        config.setParameter("storage.s3.region", "eu-west-1");
        assertThat(config.getParameter("io.tileverse.rangereader.s3.region", String.class))
                .contains("eu-west-1");
    }

    @Test
    void setParameter_canonicalKey_readableAsStorage() {
        RangeReaderConfig config = new RangeReaderConfig().uri("file:///tmp/x.pmtiles");
        config.setParameter("io.tileverse.rangereader.azure.blob-name", "foo.pmtiles");
        assertThat(config.getParameter("storage.azure.blob-name", String.class)).contains("foo.pmtiles");
    }

    @Test
    void setParameter_storageProviderKey_populatesProviderId() {
        RangeReaderConfig config = new RangeReaderConfig().uri("file:///tmp/x.pmtiles");
        config.setParameter("storage.provider", "s3");
        assertThat(config.providerId()).contains("s3");
    }

    @Test
    void fromProperties_storageUriAndProviderKeys_parseCorrectly() {
        Properties p = new Properties();
        p.setProperty("storage.uri", "file:///tmp/x.pmtiles");
        p.setProperty("storage.provider", "s3");
        p.setProperty("storage.s3.region", "us-west-2");

        RangeReaderConfig config = RangeReaderConfig.fromProperties(p);

        assertThat(config.uri().toString()).isEqualTo("file:///tmp/x.pmtiles");
        assertThat(config.providerId()).contains("s3");
        assertThat(config.getParameter("io.tileverse.rangereader.s3.region", String.class))
                .contains("us-west-2");
    }

    @Test
    void fromProperties_canonicalUriAndProviderKeys_parseCorrectly() {
        Properties p = new Properties();
        p.setProperty("io.tileverse.rangereader.uri", "file:///tmp/x.pmtiles");
        p.setProperty("io.tileverse.rangereader.provider", "s3");
        p.setProperty("io.tileverse.rangereader.s3.region", "us-west-2");

        RangeReaderConfig config = RangeReaderConfig.fromProperties(p);

        assertThat(config.uri().toString()).isEqualTo("file:///tmp/x.pmtiles");
        assertThat(config.providerId()).contains("s3");
        assertThat(config.getParameter("io.tileverse.rangereader.s3.region", String.class))
                .contains("us-west-2");
    }

    @Test
    void toProperties_emitsOnlyCanonicalKeys() {
        RangeReaderConfig config = new RangeReaderConfig().uri("file:///tmp/x.pmtiles");
        config.setParameter("storage.s3.region", "us-west-2");
        config.providerId("s3");

        Properties out = config.toProperties();

        assertThat(out.stringPropertyNames()).allMatch(k -> !k.startsWith("storage."));
        assertThat(out.getProperty("io.tileverse.rangereader.uri")).isEqualTo("file:///tmp/x.pmtiles");
        assertThat(out.getProperty("io.tileverse.rangereader.provider")).isEqualTo("s3");
        assertThat(out.getProperty("io.tileverse.rangereader.s3.region")).isEqualTo("us-west-2");
    }
}
