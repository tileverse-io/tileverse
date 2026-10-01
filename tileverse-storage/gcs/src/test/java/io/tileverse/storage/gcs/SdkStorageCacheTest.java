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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import io.tileverse.storage.StorageException;
import java.io.IOException;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class SdkStorageCacheTest {

    @Test
    void sameKeyReturnsSameLease() {
        SdkStorageCache cache = new SdkStorageCache();
        SdkStorageCache.Key key = new SdkStorageCache.Key(
                Optional.empty(), Optional.empty(), Optional.empty(), true, Optional.empty(), Optional.empty());
        try (SdkStorageCache.Lease a = cache.acquire(key);
                SdkStorageCache.Lease b = cache.acquire(key)) {
            assertThat(a.client()).isSameAs(b.client());
        }
    }

    @Test
    void differentKeysReturnDifferentClients() {
        SdkStorageCache cache = new SdkStorageCache();
        SdkStorageCache.Key k1 = new SdkStorageCache.Key(
                Optional.empty(), Optional.of("proj-a"), Optional.empty(), true, Optional.empty(), Optional.empty());
        SdkStorageCache.Key k2 = new SdkStorageCache.Key(
                Optional.empty(), Optional.of("proj-b"), Optional.empty(), true, Optional.empty(), Optional.empty());
        try (SdkStorageCache.Lease a = cache.acquire(k1);
                SdkStorageCache.Lease b = cache.acquire(k2)) {
            assertThat(a.client()).isNotSameAs(b.client());
        }
    }

    @Test
    void releasedAtZeroRefcount() {
        SdkStorageCache cache = new SdkStorageCache();
        SdkStorageCache.Key key = new SdkStorageCache.Key(
                Optional.empty(), Optional.empty(), Optional.empty(), true, Optional.empty(), Optional.empty());
        SdkStorageCache.Lease a = cache.acquire(key);
        SdkStorageCache.Lease b = cache.acquire(key);
        a.close();
        assertThat(cache.entryCount()).isEqualTo(1);
        b.close();
        assertThat(cache.entryCount()).isZero();
    }

    @Test
    void differentUserProjectsProduceDifferentKeys() {
        SdkStorageCache.Key k1 = new SdkStorageCache.Key(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                false,
                Optional.of("billing-a"),
                Optional.empty());
        SdkStorageCache.Key k2 = new SdkStorageCache.Key(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                false,
                Optional.of("billing-b"),
                Optional.empty());
        assertThat(k1).isNotEqualTo(k2);
    }

    @Test
    void quotaProjectReachesTheClient() {
        SdkStorageCache cache = new SdkStorageCache();
        SdkStorageCache.Key key = new SdkStorageCache.Key(
                Optional.of("http://localhost:4443"),
                Optional.of("test"),
                Optional.empty(),
                true,
                Optional.empty(),
                Optional.of("my-quota-project"));
        try (SdkStorageCache.Lease lease = cache.acquire(key)) {
            assertThat(lease.client().getOptions().getQuotaProjectId()).isEqualTo("my-quota-project");
        }
    }

    @Test
    void missingApplicationDefaultCredentialsFailTheOpen() {
        IOException notFound = new IOException("Your default credentials were not found");
        SdkStorageCache cache = new SdkStorageCache(() -> {
            throw notFound;
        });
        SdkStorageCache.Key key = authenticatedKey();

        assertThatThrownBy(() -> cache.acquire(key))
                .isInstanceOf(StorageException.class)
                .hasMessageContaining("storage.gcs.anonymous=true")
                .hasCause(notFound);
        assertThat(cache.entryCount()).isZero();
    }

    @Test
    void applicationDefaultCredentialsReachTheClient() {
        GoogleCredentials credentials = GoogleCredentials.create(new AccessToken("token", null));
        SdkStorageCache cache = new SdkStorageCache(() -> credentials);

        try (SdkStorageCache.Lease lease = cache.acquire(authenticatedKey())) {
            assertThat(lease.client().getOptions().getCredentials()).isSameAs(credentials);
        }
    }

    private static SdkStorageCache.Key authenticatedKey() {
        return new SdkStorageCache.Key(
                Optional.empty(), Optional.of("test"), Optional.empty(), false, Optional.empty(), Optional.empty());
    }

    @Test
    void anonymousModeBuildsClient() {
        SdkStorageCache cache = new SdkStorageCache();
        SdkStorageCache.Key key = new SdkStorageCache.Key(
                Optional.of("http://localhost:4443"),
                Optional.of("test"),
                Optional.empty(),
                true,
                Optional.empty(),
                Optional.empty());
        try (SdkStorageCache.Lease lease = cache.acquire(key)) {
            assertThat(lease.client()).isNotNull();
        }
    }
}
