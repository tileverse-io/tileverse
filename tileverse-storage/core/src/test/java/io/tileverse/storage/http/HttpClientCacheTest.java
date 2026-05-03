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
package io.tileverse.storage.http;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class HttpClientCacheTest {

    @Test
    void sameKeyReturnsSameClient() {
        HttpClientCache cache = new HttpClientCache();
        HttpClientCache.Key key = new HttpClientCache.Key(5_000, false);
        try (HttpClientCache.Lease a = cache.acquire(key);
                HttpClientCache.Lease b = cache.acquire(key)) {
            assertThat(a.client()).isSameAs(b.client());
        }
    }

    @Test
    void differentKeysReturnDifferentClients() {
        HttpClientCache cache = new HttpClientCache();
        HttpClientCache.Key fast = new HttpClientCache.Key(1_000, false);
        HttpClientCache.Key slow = new HttpClientCache.Key(30_000, false);
        try (HttpClientCache.Lease a = cache.acquire(fast);
                HttpClientCache.Lease b = cache.acquire(slow)) {
            assertThat(a.client()).isNotSameAs(b.client());
        }
    }

    @Test
    void trustAllFlagAffectsCacheKey() {
        HttpClientCache cache = new HttpClientCache();
        HttpClientCache.Key strict = new HttpClientCache.Key(5_000, false);
        HttpClientCache.Key trustAll = new HttpClientCache.Key(5_000, true);
        try (HttpClientCache.Lease a = cache.acquire(strict);
                HttpClientCache.Lease b = cache.acquire(trustAll)) {
            assertThat(a.client()).isNotSameAs(b.client());
        }
    }

    @Test
    void clientIsClosedWhenLastLeaseReleased() {
        HttpClientCache cache = new HttpClientCache();
        HttpClientCache.Key key = new HttpClientCache.Key(5_000, false);
        HttpClientCache.Lease a = cache.acquire(key);
        HttpClientCache.Lease b = cache.acquire(key);
        a.close();
        assertThat(cache.entryCount()).isEqualTo(1);
        b.close();
        assertThat(cache.entryCount()).isZero();
    }

    @Test
    void closingTheSameLeaseTwiceIsHarmless() {
        HttpClientCache cache = new HttpClientCache();
        HttpClientCache.Key key = new HttpClientCache.Key(5_000, false);
        HttpClientCache.Lease lease = cache.acquire(key);
        lease.close();
        lease.close();
        assertThat(cache.entryCount()).isZero();
    }
}
