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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.jspecify.annotations.NullMarked;

/**
 * Per-provider reference-counted cache of {@link com.google.cloud.storage.Storage} GCS SDK client instances. Multiple
 * {@link GoogleCloudStorage} instances against the same (host, projectId, credentials, anonymous) key share one
 * underlying client; the SDK client closes when the last lease releases.
 */
@NullMarked
final class SdkStorageCache {

    private final Map<SdkStorageCache.Key, SdkStorageCache.Entry> entries = new ConcurrentHashMap<>();

    int entryCount() {
        return entries.size();
    }

    record Key(
            Optional<String> hostOverride,
            Optional<String> projectId,
            Optional<String> credentialsSource,
            boolean anonymous) {

        Key {
            Objects.requireNonNull(hostOverride, "hostOverride");
            Objects.requireNonNull(projectId, "projectId");
            Objects.requireNonNull(credentialsSource, "credentialsSource");
        }
    }

    final class Lease implements AutoCloseable {
        private final Key key;
        private final Entry entry;
        private boolean closed;

        Lease(Key key, Entry entry) {
            this.key = key;
            this.entry = entry;
        }

        Storage client() {
            return entry.client;
        }

        @Override
        public synchronized void close() {
            if (closed) return;
            closed = true;
            release(key);
        }

        private synchronized void release(Key key) {
            entries.compute(key, (k, e) -> {
                if (e == null) return null;
                e.refCount--;
                if (e.refCount <= 0) {
                    e.closeAll();
                    return null;
                }
                return e;
            });
        }
    }

    private static final class Entry {
        final Storage client;
        int refCount;

        Entry(Storage client) {
            this.client = client;
        }

        void closeAll() {
            try {
                client.close();
            } catch (Exception ignored) {
                // GCS SDK Storage close is best-effort.
            }
        }
    }

    Lease acquire(Key key) {
        Entry entry = entries.compute(key, (k, existing) -> {
            Entry e = existing == null ? build(k) : existing;
            e.refCount++;
            return e;
        });
        return new Lease(key, entry);
    }

    private static Entry build(Key key) {
        StorageOptions.Builder b = StorageOptions.newBuilder();
        key.projectId().ifPresent(b::setProjectId);
        key.hostOverride().ifPresent(b::setHost);
        if (key.anonymous()) {
            b.setCredentials(NoCredentials.getInstance());
        } else {
            try {
                b.setCredentials(GoogleCredentials.getApplicationDefault());
            } catch (IOException e) {
                // Fall back to no credentials if the default chain fails (e.g. local dev without ADC).
                b.setCredentials(NoCredentials.getInstance());
            }
        }
        return new Entry(b.build().getService());
    }
}
