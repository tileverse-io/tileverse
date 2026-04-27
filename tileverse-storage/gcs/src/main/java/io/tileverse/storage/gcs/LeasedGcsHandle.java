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

import com.google.cloud.storage.Storage;

/**
 * {@link GcsClientHandle} that wraps an {@link SdkStorageCache.Lease}. The lease is released on close, decrementing the
 * cache refcount.
 */
final class LeasedGcsHandle implements GcsClientHandle {

    private final SdkStorageCache.Lease lease;

    LeasedGcsHandle(SdkStorageCache.Lease lease) {
        this.lease = lease;
    }

    @Override
    public Storage client() {
        return lease.client();
    }

    @Override
    public void close() {
        lease.close();
    }
}
