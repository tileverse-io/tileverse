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
import java.util.Objects;

/**
 * {@link GcsClientHandle} that wraps a caller-supplied {@link Storage} client. {@link #close()} is a no-op so the
 * caller retains ownership of the SDK client.
 */
record BorrowedGcsHandle(Storage client) implements GcsClientHandle {

    BorrowedGcsHandle {
        Objects.requireNonNull(client, "client");
    }

    @Override
    public void close() {
        // borrowed; caller owns the SDK client
    }
}
