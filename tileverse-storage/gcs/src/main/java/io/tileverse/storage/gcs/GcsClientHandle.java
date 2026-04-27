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
 * Package-private indirection between {@link GoogleCloudStorage} and the GCS SDK {@link Storage} client. Two
 * implementations:
 *
 * <ul>
 *   <li>{@code LeasedGcsHandle} (SPI path): wraps {@link SdkStorageCache.Lease}; {@link #close()} releases the lease.
 *   <li>{@code BorrowedGcsHandle} (escape-hatch path, added in step 3): wraps a caller-supplied {@code Storage} client;
 *       {@link #close()} is a no-op.
 * </ul>
 */
interface GcsClientHandle extends AutoCloseable {

    Storage client();

    @Override
    void close();
}
