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

import java.util.Optional;
import org.junit.jupiter.api.Test;

class SdkStorageCacheTest {

    @Test
    void sameKeyReturnsSameLease() {
        SdkStorageCache cache = new SdkStorageCache();
        SdkStorageCache.Key key = new SdkStorageCache.Key(Optional.empty(), Optional.empty(), Optional.empty(), false);
        try (SdkStorageCache.Lease a = cache.acquire(key);
                SdkStorageCache.Lease b = cache.acquire(key)) {
            assertThat(a.client()).isSameAs(b.client());
        }
    }

    @Test
    void differentKeysReturnDifferentClients() {
        SdkStorageCache cache = new SdkStorageCache();
        SdkStorageCache.Key k1 =
                new SdkStorageCache.Key(Optional.empty(), Optional.of("proj-a"), Optional.empty(), false);
        SdkStorageCache.Key k2 =
                new SdkStorageCache.Key(Optional.empty(), Optional.of("proj-b"), Optional.empty(), false);
        try (SdkStorageCache.Lease a = cache.acquire(k1);
                SdkStorageCache.Lease b = cache.acquire(k2)) {
            assertThat(a.client()).isNotSameAs(b.client());
        }
    }

    @Test
    void releasedAtZeroRefcount() {
        SdkStorageCache cache = new SdkStorageCache();
        SdkStorageCache.Key key = new SdkStorageCache.Key(Optional.empty(), Optional.empty(), Optional.empty(), false);
        SdkStorageCache.Lease a = cache.acquire(key);
        SdkStorageCache.Lease b = cache.acquire(key);
        a.close();
        assertThat(cache.entryCount()).isEqualTo(1);
        b.close();
        assertThat(cache.entryCount()).isZero();
    }

    @Test
    void anonymousModeBuildsClient() {
        SdkStorageCache cache = new SdkStorageCache();
        SdkStorageCache.Key key = new SdkStorageCache.Key(
                Optional.of("http://localhost:4443"), Optional.of("test"), Optional.empty(), true);
        try (SdkStorageCache.Lease lease = cache.acquire(key)) {
            assertThat(lease.client()).isNotNull();
        }
    }
}
