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
package io.tileverse.storage.azure;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class AzureClientCacheTest {

    private static final AzureRetryConfig DEFAULT_RETRY =
            new AzureRetryConfig(3, Duration.ofSeconds(4), Duration.ofMinutes(2), Duration.ofSeconds(60));

    @Test
    void sameKeyReturnsSameLease() {
        AzureClientCache cache = new AzureClientCache();
        AzureClientCache.Key key = AzureClientCache.key(
                "https://tileversetest.blob.core.windows.net", "tileversetest", null, null, null, false, DEFAULT_RETRY);
        try (AzureClientCache.Lease a = cache.acquire(key);
                AzureClientCache.Lease b = cache.acquire(key)) {
            assertThat(a.blobServiceClient()).isSameAs(b.blobServiceClient());
        }
    }

    @Test
    void differentKeysReturnDifferentClients() {
        AzureClientCache cache = new AzureClientCache();
        AzureClientCache.Key k1 =
                AzureClientCache.key("https://a.blob.core.windows.net", "a", null, null, null, false, DEFAULT_RETRY);
        AzureClientCache.Key k2 =
                AzureClientCache.key("https://b.blob.core.windows.net", "b", null, null, null, false, DEFAULT_RETRY);
        try (AzureClientCache.Lease a = cache.acquire(k1);
                AzureClientCache.Lease b = cache.acquire(k2)) {
            assertThat(a.blobServiceClient()).isNotSameAs(b.blobServiceClient());
        }
    }

    @Test
    void clientReleasedWhenLastLeaseClosed() {
        AzureClientCache cache = new AzureClientCache();
        AzureClientCache.Key key = AzureClientCache.key(
                "https://tileversetest.blob.core.windows.net", "tileversetest", null, null, null, false, DEFAULT_RETRY);
        AzureClientCache.Lease a = cache.acquire(key);
        AzureClientCache.Lease b = cache.acquire(key);
        a.close();
        assertThat(cache.entryCount()).isEqualTo(1);
        b.close();
        assertThat(cache.entryCount()).isZero();
    }

    @Test
    void leaseExposesDataLakeServiceClient() {
        AzureClientCache cache = new AzureClientCache();
        AzureClientCache.Key key = AzureClientCache.key(
                "https://tileversetest.dfs.core.windows.net", "tileversetest", null, null, null, false, DEFAULT_RETRY);
        try (AzureClientCache.Lease lease = cache.acquire(key)) {
            assertThat(lease.dataLakeServiceClient()).isNotNull();
        }
    }

    @Test
    void anonymousFlagDiscriminatesCacheEntries() {
        AzureClientCache cache = new AzureClientCache();
        AzureClientCache.Key authed = AzureClientCache.key(
                "https://tileversetest.blob.core.windows.net", "tileversetest", null, null, null, false, DEFAULT_RETRY);
        AzureClientCache.Key anon = AzureClientCache.key(
                "https://tileversetest.blob.core.windows.net", "tileversetest", null, null, null, true, DEFAULT_RETRY);
        try (AzureClientCache.Lease a = cache.acquire(authed);
                AzureClientCache.Lease b = cache.acquire(anon)) {
            assertThat(a.blobServiceClient()).isNotSameAs(b.blobServiceClient());
        }
    }
}
