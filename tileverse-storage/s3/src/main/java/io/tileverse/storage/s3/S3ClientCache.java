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

import java.net.URI;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.utils.SdkAutoCloseable;

/**
 * Reference-counted cache of {@link S3Client}, {@link S3AsyncClient}, {@link S3TransferManager}, and
 * {@link S3Presigner} instances, keyed by (region, endpoint, credentials profile, forcePathStyle). Multiple
 * {@link S3Storage} instances sharing the same key share one underlying client set; the SDK clients are closed when the
 * last lease is released.
 *
 * <p>Every async client runs on the HTTP client of {@link S3SharedHttpClient}, regardless of its key: native memory and
 * event loop threads stay independent of the number of client sets. That HTTP client and the one of every sync client
 * take their connection settings from {@link S3HttpClientSettings}.
 */
@NullMarked
final class S3ClientCache {

    /**
     * The cache of the process, used by every provider created with its public constructor. StorageFactory instantiates
     * a provider at every lookup: a cache per provider would build a client set at every open.
     */
    static final S3ClientCache INSTANCE = new S3ClientCache();

    record Key(
            String region,
            Optional<URI> endpointOverride,
            boolean anonymous,
            Optional<String> accessKeyId,
            Optional<String> secretAccessKey,
            Optional<String> profile,
            boolean forcePathStyle) {}

    /**
     * Convenience factory that wraps each nullable argument with {@link Optional#ofNullable}. Prefer this over the
     * canonical record constructor at call sites that already have raw nullable values, to avoid sprinkling
     * {@code Optional.of}/{@code Optional.empty()} boilerplate.
     */
    static Key key(
            String region,
            @Nullable URI endpointOverride,
            boolean anonymous,
            @Nullable String accessKeyId,
            @Nullable String secretAccessKey,
            @Nullable String profile,
            boolean forcePathStyle) {
        return new Key(
                region,
                Optional.ofNullable(endpointOverride),
                anonymous,
                Optional.ofNullable(accessKeyId),
                Optional.ofNullable(secretAccessKey),
                Optional.ofNullable(profile),
                forcePathStyle);
    }

    /** A reference-counted handle. Closing decrements the refcount; when zero, the SDK clients close. */
    final class Lease implements AutoCloseable {
        private final Key key;
        private final Entry entry;
        private final AtomicBoolean closed = new AtomicBoolean();

        Lease(Key key, Entry entry) {
            this.key = key;
            this.entry = entry;
        }

        S3Client client() {
            return entry.syncClient;
        }

        S3AsyncClient asyncClient() {
            return entry.asyncClient;
        }

        S3TransferManager transferManager() {
            return entry.transferManager;
        }

        S3Presigner presigner() {
            return entry.presigner;
        }

        EndpointEtags endpointEtags() {
            return entry.endpointEtags;
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            boolean lastLease = release();
            if (lastLease) {
                entry.closeAll();
            }
        }

        /**
         * Returns whether this lease held the last reference. The entry then leaves the cache before it closes: a
         * failing close must not leave a closed client set in the cache.
         */
        private boolean release() {
            Entry remaining = entries.computeIfPresent(key, (k, e) -> e.refCount.decrementAndGet() > 0 ? e : null);
            return remaining == null;
        }
    }

    private static final class Entry {
        final S3Client syncClient;
        final S3AsyncClient asyncClient;
        final S3TransferManager transferManager;
        final S3Presigner presigner;
        final OpenedClients opened;
        final AtomicInteger refCount = new AtomicInteger();
        final EndpointEtags endpointEtags = new EndpointEtags();

        Entry(S3Client sync, S3AsyncClient async, S3TransferManager tm, S3Presigner ps, OpenedClients opened) {
            this.syncClient = sync;
            this.asyncClient = async;
            this.transferManager = tm;
            this.presigner = ps;
            this.opened = opened;
        }

        void closeAll() {
            opened.closeAll();
        }
    }

    /**
     * The clients of one client set and its HTTP client lease, closed in the reverse order of their opening. The lease
     * closes last, after the async client running on it.
     */
    private static final class OpenedClients {

        private final Deque<SdkAutoCloseable> clients = new ArrayDeque<>();

        <T extends SdkAutoCloseable> T add(T client) {
            clients.push(client);
            return client;
        }

        void closeAll() {
            for (SdkAutoCloseable client : clients) {
                client.close();
            }
        }
    }

    private final Map<Key, Entry> entries = new ConcurrentHashMap<>();
    private final S3SharedHttpClient sharedHttpClient;
    private final Supplier<SdkHttpClient.Builder<?>> syncHttpClientBuilders;

    S3ClientCache() {
        this(S3SharedHttpClient.INSTANCE);
    }

    S3ClientCache(S3SharedHttpClient sharedHttpClient) {
        this(sharedHttpClient, S3ClientCache::syncHttpClientBuilderOfTheProcess);
    }

    /**
     * @param syncHttpClientBuilders supplies the HTTP client builder of the sync client, each time a client set is
     *     built
     */
    S3ClientCache(S3SharedHttpClient sharedHttpClient, Supplier<SdkHttpClient.Builder<?>> syncHttpClientBuilders) {
        this.sharedHttpClient = Objects.requireNonNull(sharedHttpClient, "sharedHttpClient");
        this.syncHttpClientBuilders = Objects.requireNonNull(syncHttpClientBuilders, "syncHttpClientBuilders");
    }

    private static SdkHttpClient.Builder<?> syncHttpClientBuilderOfTheProcess() {
        S3HttpClientSettings settings = S3HttpClientSettings.ofProcess();
        return settings.syncHttpClientBuilder();
    }

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

    private Entry build(Key key) {
        OpenedClients opened = new OpenedClients();
        boolean built = false;
        try {
            S3SharedHttpClient.Lease httpClientLease = opened.add(sharedHttpClient.acquire());
            Entry entry = build(key, httpClientLease, syncHttpClientBuilders.get(), opened);
            built = true;
            return entry;
        } finally {
            if (!built) {
                opened.closeAll();
            }
        }
    }

    private static Entry build(
            Key key,
            S3SharedHttpClient.Lease httpClientLease,
            SdkHttpClient.Builder<?> syncHttpClientBuilder,
            OpenedClients opened) {
        S3Configuration syncConfig = S3Configuration.builder()
                .pathStyleAccessEnabled(key.forcePathStyle())
                .build();

        // Both builders use WHEN_REQUIRED for request and response checksums. Since SDK 2.30
        // the default WHEN_SUPPORTED adds x-amz-checksum-mode: ENABLED to every GetObject and
        // switches PutObject to aws-chunked streaming with a trailing checksum. Strict
        // S3-compatible endpoints reject both (s3proxy answers 501 on the read header and 400
        // on the streaming payload); lenient ones (LocalStack, older MinIO) don't always echo
        // the checksum headers back. WHEN_REQUIRED keeps checksums whenever the operation
        // requires them or the caller opts in via GetObjectRequest#checksumMode, while plain
        // reads and writes stay compatible with emulators.
        S3ClientBuilder syncBuilder = S3Client.builder()
                .httpClientBuilder(syncHttpClientBuilder)
                .region(Region.of(key.region()))
                .serviceConfiguration(syncConfig)
                .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED);
        // Multipart stays on for the transfer manager: without it an upload is one PutObject,
        // capped by S3 at 5 GiB.
        SdkAsyncHttpClient sharedHttpClient = httpClientLease.client();
        S3AsyncClientBuilder asyncBuilder = S3AsyncClient.builder()
                .httpClient(sharedHttpClient)
                .multipartEnabled(true)
                .region(Region.of(key.region()))
                .serviceConfiguration(syncConfig)
                .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED);
        S3Presigner.Builder presignerBuilder =
                S3Presigner.builder().region(Region.of(key.region())).serviceConfiguration(syncConfig);
        key.endpointOverride().ifPresent(uri -> {
            syncBuilder.endpointOverride(uri);
            asyncBuilder.endpointOverride(uri);
            presignerBuilder.endpointOverride(uri);
        });
        // Credential resolution order:
        //   1. Anonymous flag (AnonymousCredentialsProvider) for public buckets like Overture
        //      Maps. Wins over everything else; the SDK still requires *a* credential provider
        //      to sign nothing.
        //   2. Explicit access-key + secret in the cache key (StaticCredentialsProvider).
        //      Inline secrets in the StorageConfig take precedence over the chains below.
        //   3. Explicit profile name (ProfileCredentialsProvider).
        //   4. The AWS default chain (system props, env vars, web identity, ~/.aws files,
        //      container and EC2 instance roles), failing with a pointer to the anonymous flag.
        AwsCredentialsProvider creds;
        if (key.anonymous()) {
            creds = AnonymousCredentialsProvider.create();
        } else if (key.accessKeyId().isPresent() && key.secretAccessKey().isPresent()) {
            String accessKeyId = key.accessKeyId().orElseThrow();
            String accessKey = key.secretAccessKey().orElseThrow();
            AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKeyId, accessKey);
            creds = StaticCredentialsProvider.create(credentials);
        } else if (key.profile().isPresent()) {
            String profileName = key.profile().orElseThrow();
            creds = ProfileCredentialsProvider.create(profileName);
        } else {
            creds = new DefaultCredentialsChain();
        }
        syncBuilder.credentialsProvider(creds);
        asyncBuilder.credentialsProvider(creds);
        presignerBuilder.credentialsProvider(creds);

        S3Client sync = opened.add(syncBuilder.build());
        S3AsyncClient async = opened.add(asyncBuilder.build());
        S3TransferManager.Builder transferManagerBuilder =
                S3TransferManager.builder().s3Client(async);
        S3TransferManager tm = opened.add(transferManagerBuilder.build());
        S3Presigner ps = opened.add(presignerBuilder.build());
        return new Entry(sync, async, tm, ps, opened);
    }
}
