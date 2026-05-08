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

import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

/**
 * Per-provider reference-counted cache of {@link BlobServiceClient} and {@link DataLakeServiceClient} instances.
 * Multiple {@link AzureBlobStorage} and {@link AzureDataLakeStorage} instances against the same account share one
 * entry; the underlying clients close when the last lease releases.
 */
@NullMarked
final class AzureClientCache {

    /**
     * Cache discriminator. {@code accountKey} and {@code sasToken} appear here only as presence indicators and source
     * identifiers; secrets themselves come from the Azure default credential chain or are passed inline via per-Storage
     * config. {@code anonymous} discriminates the no-credentials path used for public containers.
     */
    record Key(
            String endpoint,
            String accountName,
            Optional<String> accountKey,
            Optional<String> sasToken,
            Optional<String> connectionString,
            boolean anonymous,
            AzureRetryConfig retry) {}

    /**
     * Convenience factory that wraps each nullable credential argument with {@link Optional#ofNullable}. Prefer this
     * over the canonical record constructor at call sites that already have raw nullable values, to avoid sprinkling
     * {@code Optional.of}/{@code Optional.empty()} boilerplate.
     */
    static Key key(
            String endpoint,
            String accountName,
            @Nullable String accountKey,
            @Nullable String sasToken,
            @Nullable String connectionString,
            boolean anonymous,
            AzureRetryConfig retry) {
        return new Key(
                endpoint,
                accountName,
                Optional.ofNullable(accountKey),
                Optional.ofNullable(sasToken),
                Optional.ofNullable(connectionString),
                anonymous,
                retry);
    }

    final class Lease implements AutoCloseable {
        private final Key key;
        private final Entry entry;
        private final AtomicBoolean closed = new AtomicBoolean();

        Lease(Key key, Entry entry) {
            this.key = key;
            this.entry = entry;
        }

        BlobServiceClient blobServiceClient() {
            return entry.blobServiceClient;
        }

        DataLakeServiceClient dataLakeServiceClient() {
            DataLakeServiceClient client = entry.dataLakeServiceClient;
            if (client == null) {
                throw new UnsupportedOperationException(
                        "DataLakeServiceClient is unavailable when storage.azure.anonymous=true; "
                                + "Azure DataLake Gen2 (HNS) does not support anonymous access. "
                                + "Use the .blob.core. endpoint with the AzureBlobStorage provider for public containers.");
            }
            return client;
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
                // Azure SDK clients may be Reactor-backed; they don't have an explicit close().
                // Forgetting the references lets GC collect the underlying http pipeline.
                return refCount <= 0 ? null : e;
            });
        }
    }

    private static final class Entry {
        final BlobServiceClient blobServiceClient;

        @Nullable
        final DataLakeServiceClient dataLakeServiceClient;

        final AtomicInteger refCount = new AtomicInteger();

        Entry(BlobServiceClient blobClient, @Nullable DataLakeServiceClient dlClient) {
            this.blobServiceClient = blobClient;
            this.dataLakeServiceClient = dlClient;
        }
    }

    private final Map<Key, Entry> entries = new ConcurrentHashMap<>();

    int entryCount() {
        return entries.size();
    }

    Lease acquire(Key key) {
        Entry entry = entries.compute(key, (k, existing) -> {
            Entry e = existing == null ? build(k) : existing;
            e.refCount.incrementAndGet();
            return e;
        });
        return new Lease(key, entry);
    }

    private static Entry build(Key key) {
        // Build the blob endpoint and the dfs endpoint from the cache-key endpoint string.
        String blobEndpoint = key.endpoint().replace(".dfs.core.", ".blob.core.");
        String dfsEndpoint = key.endpoint().replace(".blob.core.", ".dfs.core.");

        BlobServiceClientBuilder blobBuilder = new BlobServiceClientBuilder().endpoint(blobEndpoint);

        com.azure.storage.common.policy.RequestRetryOptions retryOptions =
                key.retry().toSdkOptions();
        blobBuilder.retryOptions(retryOptions);

        if (key.anonymous()) {
            // Public-container access: leave the builder without a credential. The Azure SDK
            // emits unsigned requests, which only succeed against containers configured with
            // anonymous (Container- or Blob-level) read access. DataLakeServiceClientBuilder
            // rejects anonymous access at buildClient() time, so the DataLake client is omitted,
            // and attempts to use it via the lease throw UnsupportedOperationException.
            return new Entry(blobBuilder.buildClient(), null);
        }

        DataLakeServiceClientBuilder dlBuilder = new DataLakeServiceClientBuilder().endpoint(dfsEndpoint);
        dlBuilder.retryOptions(retryOptions);

        if (key.connectionString().isPresent()) {
            String connectionString = key.connectionString().orElseThrow();
            blobBuilder.connectionString(connectionString);
            dlBuilder.connectionString(connectionString);
        } else if (key.accountKey().isPresent()) {
            String accountName = key.accountName();
            String accountKey = key.accountKey().orElseThrow();
            StorageSharedKeyCredential cred = new StorageSharedKeyCredential(accountName, accountKey);
            blobBuilder.credential(cred);
            dlBuilder.credential(cred);
        } else if (key.sasToken().isPresent()) {
            String sas = key.sasToken().orElseThrow();
            String sasWithQ = sas.startsWith("?") ? sas : "?" + sas;
            blobBuilder.sasToken(sasWithQ);
            dlBuilder.sasToken(sasWithQ);
        } else {
            DefaultAzureCredential defaultCred = new DefaultAzureCredentialBuilder().build();
            blobBuilder.credential(defaultCred);
            dlBuilder.credential(defaultCred);
        }

        return new Entry(blobBuilder.buildClient(), dlBuilder.buildClient());
    }
}
