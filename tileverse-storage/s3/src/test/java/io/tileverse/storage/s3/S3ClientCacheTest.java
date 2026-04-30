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
package io.tileverse.storage.s3;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class S3ClientCacheTest {

    @Test
    void sameKeyReturnsSameLeaseHandle() {
        S3ClientCache cache = new S3ClientCache();
        S3ClientCache.Key key = S3ClientCache.key("us-east-1", null, false, null, null, null, false);
        try (S3ClientCache.Lease a = cache.acquire(key);
                S3ClientCache.Lease b = cache.acquire(key)) {
            assertThat(a.client()).isSameAs(b.client());
        }
    }

    @Test
    void differentKeysReturnDifferentClients() {
        S3ClientCache cache = new S3ClientCache();
        S3ClientCache.Key k1 = S3ClientCache.key("us-east-1", null, false, null, null, null, false);
        S3ClientCache.Key k2 = S3ClientCache.key("us-west-2", null, false, null, null, null, false);
        try (S3ClientCache.Lease a = cache.acquire(k1);
                S3ClientCache.Lease b = cache.acquire(k2)) {
            assertThat(a.client()).isNotSameAs(b.client());
        }
    }

    @Test
    void clientIsClosedWhenLastLeaseReleased() {
        S3ClientCache cache = new S3ClientCache();
        S3ClientCache.Key key = S3ClientCache.key("us-east-1", null, false, null, null, null, false);
        S3ClientCache.Lease a = cache.acquire(key);
        S3ClientCache.Lease b = cache.acquire(key);
        a.close();
        assertThat(cache.entryCount()).isEqualTo(1);
        b.close();
        assertThat(cache.entryCount()).isZero();
    }

    @Test
    void leaseExposesPresigner() {
        S3ClientCache cache = new S3ClientCache();
        S3ClientCache.Key key = S3ClientCache.key("us-east-1", null, false, null, null, null, false);
        try (S3ClientCache.Lease lease = cache.acquire(key)) {
            assertThat(lease.presigner()).isNotNull();
        }
    }

    @Test
    void leaseExposesAsyncClientAndTransferManager() {
        S3ClientCache cache = new S3ClientCache();
        S3ClientCache.Key key = S3ClientCache.key("us-east-1", null, false, null, null, null, false);
        try (S3ClientCache.Lease lease = cache.acquire(key)) {
            assertThat(lease.asyncClient()).isNotNull();
            assertThat(lease.transferManager()).isNotNull();
        }
    }
}
