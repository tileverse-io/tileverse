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
package io.tileverse.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class RangeReaderConfigTest {

    private static final String GCS_CREDENTIALS_CHAIN = "storage.gcs.default-credentials-chain";
    private static final String GCS_ANONYMOUS = "storage.gcs.anonymous";

    @Test
    void normalizeKey_canonicalPrefix_passthrough() {
        assertThat(StorageConfig.normalizeKey("storage.s3.region")).isEqualTo("storage.s3.region");
        assertThat(StorageConfig.normalizeKey("storage.uri")).isEqualTo("storage.uri");
    }

    @Test
    void normalizeKey_legacyPrefix_rewritten() {
        assertThat(StorageConfig.normalizeKey("io.tileverse.rangereader.s3.region"))
                .isEqualTo("storage.s3.region");
        assertThat(StorageConfig.normalizeKey("io.tileverse.rangereader.uri")).isEqualTo("storage.uri");
        assertThat(StorageConfig.normalizeKey("io.tileverse.rangereader.provider"))
                .isEqualTo("storage.provider");
    }

    @Test
    void normalizeKey_unrelated_passthrough() {
        assertThat(StorageConfig.normalizeKey("pmtiles")).isEqualTo("pmtiles");
        assertThat(StorageConfig.normalizeKey("namespace")).isEqualTo("namespace");
    }

    @Test
    void normalizeKey_renamedGcsHost_promotedToEndpoint() {
        assertThat(StorageConfig.normalizeKey("storage.gcs.host")).isEqualTo("storage.gcs.endpoint");
    }

    @Test
    void normalizeKey_legacyGcsHost_promotedToEndpoint() {
        assertThat(StorageConfig.normalizeKey("io.tileverse.rangereader.gcs.host"))
                .isEqualTo("storage.gcs.endpoint");
    }

    @Test
    void setParameter_gcsHostKey_storedAsEndpoint() {
        StorageConfig config =
                new StorageConfig("gs://bucket/o").setParameter("storage.gcs.host", "http://localhost:4443");

        assertThat(config.getParameter("storage.gcs.endpoint")).hasValue("http://localhost:4443");
    }

    /** A saved {@code storage.gcs.default-credentials-chain=false} meant anonymous access. */
    @Test
    void setParameter_gcsCredentialsChainFalse_storedAsAnonymous() {
        StorageConfig config = new StorageConfig("gs://bucket/o").setParameter(GCS_CREDENTIALS_CHAIN, "false");

        assertThat(config.getParameter(GCS_ANONYMOUS, Boolean.class)).hasValue(true);
        assertThat(config.getParameter(GCS_CREDENTIALS_CHAIN)).isEmpty();
    }

    @Test
    void setParameter_gcsCredentialsChainTrue_storedAsNotAnonymous() {
        StorageConfig config = new StorageConfig("gs://bucket/o").setParameter(GCS_CREDENTIALS_CHAIN, Boolean.TRUE);

        assertThat(config.getParameter(GCS_ANONYMOUS, Boolean.class)).hasValue(false);
    }

    @Test
    void setParameter_legacyGcsCredentialsChain_storedAsAnonymous() {
        StorageConfig config = new StorageConfig("gs://bucket/o")
                .setParameter("io.tileverse.rangereader.gcs.default-credentials-chain", Boolean.FALSE);

        assertThat(config.getParameter(GCS_ANONYMOUS, Boolean.class)).hasValue(true);
    }

    @Test
    void setParameter_explicitGcsAnonymous_winsOverCredentialsChainInEitherOrder() {
        StorageConfig anonymousFirst = new StorageConfig("gs://bucket/o")
                .setParameter(GCS_ANONYMOUS, false)
                .setParameter(GCS_CREDENTIALS_CHAIN, false);
        StorageConfig credentialsChainFirst = new StorageConfig("gs://bucket/o")
                .setParameter(GCS_CREDENTIALS_CHAIN, false)
                .setParameter(GCS_ANONYMOUS, false);

        assertThat(anonymousFirst.getParameter(GCS_ANONYMOUS, Boolean.class)).hasValue(false);
        assertThat(credentialsChainFirst.getParameter(GCS_ANONYMOUS, Boolean.class))
                .hasValue(false);
    }

    @Test
    void normalizeKeys_gcsCredentialsChain_rewrittenAsAnonymous() {
        Map<String, Object> out = StorageConfig.normalizeKeys(Map.of(GCS_CREDENTIALS_CHAIN, "false"));

        assertThat(out).containsOnlyKeys(GCS_ANONYMOUS).containsEntry(GCS_ANONYMOUS, true);
    }

    @Test
    void normalizeKeys_explicitGcsAnonymous_winsOverCredentialsChainInEitherOrder() {
        Map<String, Object> anonymousFirst = new LinkedHashMap<>();
        anonymousFirst.put(GCS_ANONYMOUS, "false");
        anonymousFirst.put(GCS_CREDENTIALS_CHAIN, "false");
        Map<String, Object> credentialsChainFirst = new LinkedHashMap<>();
        credentialsChainFirst.put(GCS_CREDENTIALS_CHAIN, "false");
        credentialsChainFirst.put(GCS_ANONYMOUS, "false");

        assertThat(StorageConfig.normalizeKeys(anonymousFirst)).containsOnly(Map.entry(GCS_ANONYMOUS, "false"));
        assertThat(StorageConfig.normalizeKeys(credentialsChainFirst)).containsOnly(Map.entry(GCS_ANONYMOUS, "false"));
    }

    @Test
    void normalizeKey_null_passthrough() {
        assertThat(StorageConfig.normalizeKey(null)).isNull();
    }

    @Test
    void normalizeKeys_mapRewrite_preservesValues() {
        Map<String, Object> in = Map.of(
                "io.tileverse.rangereader.s3.region", "us-west-2",
                "storage.azure.blob-name", "foo.pmtiles",
                "pmtiles", "file:///tmp/x.pmtiles");
        Map<String, Object> out = StorageConfig.normalizeKeys(in);
        assertThat(out)
                .containsOnlyKeys("storage.s3.region", "storage.azure.blob-name", "pmtiles")
                .containsEntry("storage.s3.region", "us-west-2")
                .containsEntry("storage.azure.blob-name", "foo.pmtiles")
                .containsEntry("pmtiles", "file:///tmp/x.pmtiles");
    }

    @Test
    void setParameter_legacyKey_readableAsCanonical() {
        StorageConfig config = new StorageConfig("file:///tmp/x.pmtiles");
        config.setParameter("io.tileverse.rangereader.s3.region", "eu-west-1");
        assertThat(config.getParameter("storage.s3.region", String.class)).contains("eu-west-1");
    }

    @Test
    void setParameter_canonicalKey_readableAsLegacy() {
        StorageConfig config = new StorageConfig("file:///tmp/x.pmtiles");
        config.setParameter("storage.azure.blob-name", "foo.pmtiles");
        assertThat(config.getParameter("io.tileverse.rangereader.azure.blob-name", String.class))
                .contains("foo.pmtiles");
    }

    @Test
    void setParameter_legacyProviderKey_populatesProviderId() {
        StorageConfig config = new StorageConfig("file:///tmp/x.pmtiles");
        config.setParameter("io.tileverse.rangereader.provider", "s3");
        assertThat(config.providerId()).contains("s3");
    }

    @Test
    void fromProperties_legacyUriAndProviderKeys_parseCorrectly() {
        Properties p = new Properties();
        p.setProperty("io.tileverse.rangereader.uri", "file:///tmp/x.pmtiles");
        p.setProperty("io.tileverse.rangereader.provider", "s3");
        p.setProperty("io.tileverse.rangereader.s3.region", "us-west-2");

        StorageConfig config = StorageConfig.fromProperties(p);

        assertThat(config.baseUri()).hasToString("file:///tmp/x.pmtiles");
        assertThat(config.providerId()).contains("s3");
        assertThat(config.getParameter("storage.s3.region", String.class)).contains("us-west-2");
    }

    @Test
    void fromProperties_canonicalUriAndProviderKeys_parseCorrectly() {
        Properties p = new Properties();
        p.setProperty("storage.uri", "file:///tmp/x.pmtiles");
        p.setProperty("storage.provider", "s3");
        p.setProperty("storage.s3.region", "us-west-2");

        StorageConfig config = StorageConfig.fromProperties(p);

        assertThat(config.baseUri()).hasToString("file:///tmp/x.pmtiles");
        assertThat(config.providerId()).contains("s3");
        assertThat(config.getParameter("storage.s3.region", String.class)).contains("us-west-2");
    }

    @Test
    void toProperties_emitsOnlyCanonicalKeys() {
        StorageConfig config = new StorageConfig("file:///tmp/x.pmtiles");
        config.setParameter("io.tileverse.rangereader.s3.region", "us-west-2");
        config.providerId("s3");

        Properties out = config.toProperties();

        assertThat(out.stringPropertyNames()).allMatch(k -> !k.startsWith("io.tileverse.rangereader."));
        assertThat(out.getProperty("storage.uri")).isEqualTo("file:///tmp/x.pmtiles");
        assertThat(out.getProperty("storage.provider")).isEqualTo("s3");
        assertThat(out.getProperty("storage.s3.region")).isEqualTo("us-west-2");
    }

    @ParameterizedTest
    @MethodSource("blankProviderIds")
    void providerId_blank_namesNoProvider(String blankId) {
        StorageConfig config = new StorageConfig("http://example.com/data/").providerId(blankId);

        assertThat(config.providerId()).isEmpty();
    }

    static Stream<String> blankProviderIds() {
        return Stream.of("", "   ");
    }

    @Test
    void fromProperties_blankProviderKey_namesNoProvider() {
        Properties props = new Properties();
        props.setProperty("storage.uri", "http://example.com/data/");
        props.setProperty("storage.provider", "");

        StorageConfig config = StorageConfig.fromProperties(props);

        assertThat(config.providerId()).isEmpty();
        assertThat(config.toProperties()).doesNotContainKey("storage.provider");
    }
}
