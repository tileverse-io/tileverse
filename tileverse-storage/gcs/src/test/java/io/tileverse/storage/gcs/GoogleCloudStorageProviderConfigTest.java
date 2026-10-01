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
package io.tileverse.storage.gcs;

import static org.assertj.core.api.Assertions.assertThat;

import io.tileverse.storage.StorageConfig;
import java.net.URI;
import org.junit.jupiter.api.Test;

/**
 * Verifies {@link GoogleCloudStorageProvider#keyFor(StorageConfig)} resolution: host-override (explicit / URI-derived),
 * project-id, and credential mode all flow into the cache discriminator. Replaces the reflection-based
 * {@code testBuilderWith*} cases that lived in the now-removed {@code GoogleCloudStorageRangeReader.Builder}.
 */
class GoogleCloudStorageProviderConfigTest {

    private static final URI GS_URI = URI.create("gs://my-bucket/file.bin");

    @Test
    void endpoint_explicitParameterWins() {
        StorageConfig config = new StorageConfig(GS_URI)
                .setParameter(GoogleCloudStorageProvider.GCS_ENDPOINT, URI.create("http://localhost:4443"));

        assertThat(GoogleCloudStorageProvider.keyFor(config).hostOverride()).hasValue("http://localhost:4443");
    }

    @Test
    void legacyHostKey_promotedToEndpoint() {
        StorageConfig config = new StorageConfig(GS_URI).setParameter("storage.gcs.host", "http://localhost:4443");

        assertThat(GoogleCloudStorageProvider.keyFor(config).hostOverride()).hasValue("http://localhost:4443");
    }

    @Test
    void blankEndpointIgnored() {
        StorageConfig config = new StorageConfig(GS_URI).setParameter("storage.gcs.host", "  ");

        assertThat(GoogleCloudStorageProvider.keyFor(config).hostOverride()).isEmpty();
    }

    @Test
    void hostOverride_derivedFromEmulatorStyleUri() {
        StorageConfig config = new StorageConfig("http://localhost:4443/storage/v1/b/my-bucket/o/file.bin");

        assertThat(GoogleCloudStorageProvider.keyFor(config).hostOverride()).hasValue("http://localhost:4443");
    }

    @Test
    void hostOverride_emptyForCanonicalGsUri() {
        StorageConfig config = new StorageConfig(GS_URI);

        assertThat(GoogleCloudStorageProvider.keyFor(config).hostOverride()).isEmpty();
    }

    @Test
    void projectId_carriedOnTheKey() {
        StorageConfig config =
                new StorageConfig(GS_URI).setParameter(GoogleCloudStorageProvider.GCS_PROJECT_ID, "my-project");

        assertThat(GoogleCloudStorageProvider.keyFor(config).projectId()).hasValue("my-project");
    }

    @Test
    void anonymousParameterSendsUnsignedRequests() {
        StorageConfig config = new StorageConfig(GS_URI).setParameter(GoogleCloudStorageProvider.GCS_ANONYMOUS, true);

        assertThat(GoogleCloudStorageProvider.keyFor(config).anonymous()).isTrue();
    }

    @Test
    void withNothingSetTheCredentialsAreUsed() {
        assertThat(GoogleCloudStorageProvider.keyFor(new StorageConfig(GS_URI)).anonymous())
                .isFalse();
    }

    /** StorageFactory layers the declared defaults under the caller's parameters. */
    @Test
    void theDeclaredDefaultsUseTheCredentials() {
        StorageConfig config = new GoogleCloudStorageProvider().getDefaultConfig();
        config.baseUri(GS_URI);

        assertThat(GoogleCloudStorageProvider.keyFor(config).anonymous()).isFalse();
    }

    @Test
    void endpointOverrideKeepsTheCredentials() {
        StorageConfig config = new StorageConfig(GS_URI)
                .setParameter(GoogleCloudStorageProvider.GCS_ENDPOINT, URI.create("http://localhost:4443"));

        assertThat(GoogleCloudStorageProvider.keyFor(config).anonymous()).isFalse();
    }

    /** GeoServer catalogs saved by tileverse 2.0 hold the replaced key; false meant anonymous access. */
    @Test
    void savedDefaultCredentialsChainFalseReadsAnonymously() {
        StorageConfig config = new StorageConfig(GS_URI).setParameter("storage.gcs.default-credentials-chain", false);

        assertThat(GoogleCloudStorageProvider.keyFor(config).anonymous()).isTrue();
    }

    @Test
    void savedDefaultCredentialsChainTrueUsesTheCredentials() {
        StorageConfig config = new StorageConfig(GS_URI).setParameter("storage.gcs.default-credentials-chain", true);

        assertThat(GoogleCloudStorageProvider.keyFor(config).anonymous()).isFalse();
    }

    @Test
    void differentProjectIdsProduceDifferentCacheKeys() {
        StorageConfig a =
                new StorageConfig(GS_URI).setParameter(GoogleCloudStorageProvider.GCS_PROJECT_ID, "project-a");
        StorageConfig b =
                new StorageConfig(GS_URI).setParameter(GoogleCloudStorageProvider.GCS_PROJECT_ID, "project-b");

        assertThat(GoogleCloudStorageProvider.keyFor(a)).isNotEqualTo(GoogleCloudStorageProvider.keyFor(b));
    }

    @Test
    void differentEndpointsProduceDifferentCacheKeys() {
        StorageConfig a = new StorageConfig(GS_URI)
                .setParameter(GoogleCloudStorageProvider.GCS_ENDPOINT, URI.create("http://localhost:4443"));
        StorageConfig b = new StorageConfig(GS_URI)
                .setParameter(GoogleCloudStorageProvider.GCS_ENDPOINT, URI.create("http://localhost:4444"));

        assertThat(GoogleCloudStorageProvider.keyFor(a)).isNotEqualTo(GoogleCloudStorageProvider.keyFor(b));
    }
}
