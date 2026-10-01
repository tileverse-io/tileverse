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

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.tileverse.storage.StorageException;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.jspecify.annotations.NullMarked;

/**
 * Reference-counted cache of {@link com.google.cloud.storage.Storage} GCS SDK client instances. Multiple
 * {@link GoogleCloudStorage} instances against the same (host, projectId, credentials, anonymous, userProject,
 * quotaProjectId) key share one underlying client; the SDK client closes when the last lease releases.
 */
@NullMarked
@SuppressWarnings("java:S6548") // one cache per process; tests build caches of their own
final class SdkStorageCache {

    /**
     * The cache of the process, used by every provider created with its public constructor. StorageFactory instantiates
     * a provider at every lookup: a cache per provider would build a client at every open.
     */
    static final SdkStorageCache INSTANCE = new SdkStorageCache();

    private final Map<SdkStorageCache.Key, SdkStorageCache.Entry> entries = new ConcurrentHashMap<>();
    private final ApplicationDefaultCredentials applicationDefaultCredentials;

    /** Loads the Application Default Credentials, replaced by tests to run the same on every machine. */
    @FunctionalInterface
    interface ApplicationDefaultCredentials {
        GoogleCredentials load() throws IOException;
    }

    SdkStorageCache() {
        this(GoogleCredentials::getApplicationDefault);
    }

    SdkStorageCache(ApplicationDefaultCredentials applicationDefaultCredentials) {
        this.applicationDefaultCredentials =
                Objects.requireNonNull(applicationDefaultCredentials, "applicationDefaultCredentials");
    }

    int entryCount() {
        return entries.size();
    }

    record Key(
            Optional<String> hostOverride,
            Optional<String> projectId,
            Optional<String> credentialsSource,
            boolean anonymous,
            Optional<String> userProject,
            Optional<String> quotaProjectId) {

        Key {
            Objects.requireNonNull(hostOverride, "hostOverride");
            Objects.requireNonNull(projectId, "projectId");
            Objects.requireNonNull(credentialsSource, "credentialsSource");
            Objects.requireNonNull(userProject, "userProject");
            Objects.requireNonNull(quotaProjectId, "quotaProjectId");
        }
    }

    final class Lease implements AutoCloseable {
        private final Key key;
        private final Entry entry;
        private final AtomicBoolean closed = new AtomicBoolean();

        Lease(Key key, Entry entry) {
            this.key = key;
            this.entry = entry;
        }

        Storage client() {
            return entry.client;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                release(key);
            }
        }

        private void release(Key key) {
            entries.compute(key, (k, e) -> {
                if (e == null) {
                    return null;
                }
                int refCount = e.refCount.decrementAndGet();
                if (refCount <= 0) {
                    e.closeAll();
                    return null;
                }
                return e;
            });
        }
    }

    private static final class Entry {
        final Storage client;
        final AtomicInteger refCount = new AtomicInteger();

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
            e.refCount.incrementAndGet();
            return e;
        });
        return new Lease(key, entry);
    }

    private Entry build(Key key) {
        StorageOptions.Builder b = StorageOptions.newBuilder();
        key.projectId().ifPresent(b::setProjectId);
        key.hostOverride().ifPresent(b::setHost);
        key.quotaProjectId().ifPresent(b::setQuotaProjectId);
        b.setCredentials(credentialsFor(key));
        return new Entry(b.build().getService());
    }

    /** Returns no credentials for an anonymous key, and the Application Default Credentials otherwise. */
    private Credentials credentialsFor(Key key) {
        if (key.anonymous()) {
            return NoCredentials.getInstance();
        }
        try {
            return applicationDefaultCredentials.load();
        } catch (IOException notFound) {
            throw new StorageException(
                    "No Application Default Credentials found for Google Cloud Storage; set them up "
                            + "(https://cloud.google.com/docs/authentication/external/set-up-adc), "
                            + "or set storage.gcs.anonymous=true for a public bucket",
                    notFound);
        }
    }
}
