/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
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
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.multipart.MultipartConfiguration;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.utils.SdkAutoCloseable;

/**
 * Reference-counted cache of client sets, keyed by (region, endpoint, credentials, path style). A client set holds an
 * {@link S3AsyncClient} for the Storage and reader operations; an {@link S3TransferManager} over a multipart-enabled
 * client of its own, and an {@link S3Presigner}, open on first use. Storages of one key share one client set, closed
 * when the last lease is released.
 *
 * <p>The clients run on the HTTP client of {@link S3SharedHttpClient}, whatever their key: native memory, event loop
 * threads and the connection pool of a host stay independent of the number of client sets.
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
            boolean forcePathStyle) {

        CredentialsSource credentialsSource() {
            return CredentialsSource.of(anonymous, accessKeyId, secretAccessKey, profile);
        }

        /**
         * Whether a request reaching the wrong region is sent again to the region of the bucket. AWS names that region
         * in its answer. Behind an endpoint override there is no other region to reach.
         */
        boolean crossRegionAccess() {
            return endpointOverride.isEmpty();
        }
    }

    /** The source of the credentials of a store. A store setting several uses the first of them, in this order. */
    enum CredentialsSource {
        /** No credentials: requests are unsigned, for public buckets. */
        ANONYMOUS,
        /** The access key and its secret. Either alone is ignored. */
        ACCESS_KEYS,
        /** The profile named by the store. */
        NAMED_PROFILE,
        /**
         * The AWS default chain: system properties, environment variables, web identity, the AWS profile files,
         * container and EC2 instance roles.
         */
        DEFAULT_CHAIN;

        static CredentialsSource of(
                boolean anonymous,
                Optional<String> accessKeyId,
                Optional<String> secretAccessKey,
                Optional<String> profile) {
            if (anonymous) {
                return ANONYMOUS;
            }
            if (accessKeyId.isPresent() && secretAccessKey.isPresent()) {
                return ACCESS_KEYS;
            }
            if (profile.isPresent()) {
                return NAMED_PROFILE;
            }
            return DEFAULT_CHAIN;
        }
    }

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

        S3AsyncClient client() {
            return entry.client();
        }

        S3TransferManager transferManager() {
            return entry.transferManager();
        }

        S3Presigner presigner() {
            return entry.presigner();
        }

        /** The HTTP client lease plus the clients opened so far; tests read it. */
        int openedClients() {
            return entry.openedCount();
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

    /**
     * One client set. Its client opens with it; the multipart client with the transfer manager, and the presigner, open
     * on first use: range-read workloads never need them.
     */
    private static final class Entry {

        final AtomicInteger refCount = new AtomicInteger();

        private final ClientFactory factory;
        private final OpenedClients opened;
        private final S3AsyncClient client;

        @Nullable
        private S3TransferManager transferManager; // guarded by this

        @Nullable
        private S3Presigner presigner; // guarded by this

        private boolean closed; // guarded by this

        Entry(ClientFactory factory, OpenedClients opened) {
            this.factory = factory;
            this.opened = opened;
            this.client = opened.add(factory.plainClient());
        }

        S3AsyncClient client() {
            return client;
        }

        synchronized S3TransferManager transferManager() {
            requireOpen();
            if (transferManager == null) {
                transferManager = openTransferManager();
            }
            return transferManager;
        }

        synchronized S3Presigner presigner() {
            requireOpen();
            if (presigner == null) {
                presigner = opened.add(factory.presigner());
            }
            return presigner;
        }

        synchronized int openedCount() {
            return opened.size();
        }

        synchronized void closeAll() {
            closed = true;
            opened.closeAll();
        }

        private void requireOpen() {
            // must be called under the monitor
            if (closed) {
                throw new IllegalStateException("S3 client set is closed");
            }
        }

        /**
         * The multipart client and the transfer manager over it, opened together: neither is used without the other.
         */
        private S3TransferManager openTransferManager() {
            // must be called under the monitor
            S3AsyncClient multipartClient = factory.multipartClient();
            boolean registered = false;
            try {
                S3TransferManager.Builder builder = S3TransferManager.builder().s3Client(multipartClient);
                S3TransferManager built = builder.build();
                opened.add(multipartClient);
                opened.add(built);
                registered = true;
                return built;
            } finally {
                if (!registered) {
                    multipartClient.close();
                }
            }
        }
    }

    /**
     * Builds the clients of one client set: one key, one credentials provider, the HTTP client of the process and the
     * cap on the parts in flight of an upload.
     */
    private record ClientFactory(
            Key key, AwsCredentialsProvider credentials, SdkAsyncHttpClient httpClient, int uploadPartsInFlight) {

        /** The client of the Storage and reader operations: whole-object GETs, copies and puts stay one request. */
        S3AsyncClient plainClient() {
            S3AsyncClientBuilder builder = asyncClientBuilder();
            return builder.build();
        }

        /** The client behind the transfer manager: without multipart an upload is one PutObject, capped at 5 GiB. */
        S3AsyncClient multipartClient() {
            S3AsyncClientBuilder builder =
                    asyncClientBuilder().multipartEnabled(true).multipartConfiguration(this::capUploadParts);
            return builder.build();
        }

        private void capUploadParts(MultipartConfiguration.Builder multipart) {
            multipart.parallelConfiguration(parallelism -> parallelism.maxInFlightParts(uploadPartsInFlight));
        }

        S3Presigner presigner() {
            S3Presigner.Builder builder = S3Presigner.builder()
                    .region(Region.of(key.region()))
                    .serviceConfiguration(serviceConfiguration())
                    .credentialsProvider(credentials);
            key.endpointOverride()
                    .ifPresentOrElse(
                            endpoint -> useEndpoint(endpoint, builder),
                            () -> key.profile().ifPresent(profile -> readSettingsFrom(profile, builder)));
            return builder.build();
        }

        /** An endpoint override wins over FIPS and dualstack: the SDK refuses a custom endpoint with either one on. */
        private static void useEndpoint(URI endpoint, S3Presigner.Builder builder) {
            builder.endpointOverride(endpoint).fipsEnabled(false).dualstackEnabled(false);
        }

        /** Gives the presigner the settings of the profile of the store: it takes no profile name. */
        private static void readSettingsFrom(String profile, S3Presigner.Builder builder) {
            ProfileEndpointSettings settings =
                    ProfileEndpointSettings.resolve(ProfileFile::defaultProfileFile, profile);
            builder.fipsEnabled(settings.fips()).dualstackEnabled(settings.dualstack());
        }

        /**
         * WHEN_REQUIRED keeps checksums for the operations requiring them. Since SDK 2.30 the default WHEN_SUPPORTED
         * adds x-amz-checksum-mode: ENABLED to GetObject and switches PutObject to aws-chunked streaming with a
         * trailing checksum: strict S3-compatible endpoints reject both (s3proxy answers 501 and 400), and lenient ones
         * (LocalStack, older MinIO) do not always echo the checksum headers back.
         */
        private S3AsyncClientBuilder asyncClientBuilder() {
            S3AsyncClientBuilder builder = S3AsyncClient.builder()
                    .httpClient(httpClient)
                    .region(Region.of(key.region()))
                    .serviceConfiguration(serviceConfiguration())
                    .credentialsProvider(credentials)
                    .crossRegionAccessEnabled(key.crossRegionAccess())
                    .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                    .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED);
            key.endpointOverride().ifPresent(endpoint -> useEndpoint(endpoint, builder));
            key.profile().ifPresent(profile -> readSettingsFrom(profile, builder));
            return builder;
        }

        /** An endpoint override wins over FIPS and dualstack: the SDK refuses a custom endpoint with either one on. */
        private static void useEndpoint(URI endpoint, S3AsyncClientBuilder builder) {
            builder.endpointOverride(endpoint).fipsEnabled(false).dualstackEnabled(false);
        }

        /**
         * Makes the client read its settings from the profile of the store. Without it, the SDK reads endpoint, retry
         * and FIPS settings from the profile named by {@code AWS_PROFILE}, or {@code default}.
         */
        private static void readSettingsFrom(String profile, S3AsyncClientBuilder builder) {
            builder.overrideConfiguration(override -> override.defaultProfileName(profile));
        }

        private S3Configuration serviceConfiguration() {
            return S3Configuration.builder()
                    .pathStyleAccessEnabled(key.forcePathStyle())
                    .build();
        }
    }

    /**
     * The clients of one client set and its HTTP client lease, closed in the reverse order of their opening. The lease
     * closes last, after the clients running on it.
     */
    private static final class OpenedClients {

        private final Deque<SdkAutoCloseable> clients = new ArrayDeque<>();

        <T extends SdkAutoCloseable> T add(T client) {
            clients.push(client);
            return client;
        }

        int size() {
            return clients.size();
        }

        void closeAll() {
            for (SdkAutoCloseable client : clients) {
                client.close();
            }
        }
    }

    private final Map<Key, Entry> entries = new ConcurrentHashMap<>();
    private final S3SharedHttpClient sharedHttpClient;

    S3ClientCache() {
        this(S3SharedHttpClient.INSTANCE);
    }

    S3ClientCache(S3SharedHttpClient sharedHttpClient) {
        this.sharedHttpClient = Objects.requireNonNull(sharedHttpClient, "sharedHttpClient");
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
            ClientFactory factory = clientFactory(key, httpClientLease);
            Entry entry = new Entry(factory, opened);
            built = true;
            return entry;
        } finally {
            if (!built) {
                opened.closeAll();
            }
        }
    }

    private static ClientFactory clientFactory(Key key, S3SharedHttpClient.Lease httpClientLease) {
        S3HttpClientSettings httpClientSettings = httpClientLease.settings();
        int uploadPartsInFlight = httpClientSettings.uploadPartsInFlight();
        return new ClientFactory(key, credentialsFor(key), httpClientLease.client(), uploadPartsInFlight);
    }

    /** The credentials of a key. The default chain fails with a pointer to the anonymous flag. */
    private static AwsCredentialsProvider credentialsFor(Key key) {
        return switch (key.credentialsSource()) {
            case ANONYMOUS -> AnonymousCredentialsProvider.create();
            case ACCESS_KEYS -> accessKeyCredentials(key);
            case NAMED_PROFILE -> profileCredentials(key);
            case DEFAULT_CHAIN -> new DefaultCredentialsChain();
        };
    }

    private static AwsCredentialsProvider accessKeyCredentials(Key key) {
        String accessKeyId = key.accessKeyId().orElseThrow();
        String secretAccessKey = key.secretAccessKey().orElseThrow();
        AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKeyId, secretAccessKey);
        return StaticCredentialsProvider.create(credentials);
    }

    private static AwsCredentialsProvider profileCredentials(Key key) {
        String profileName = key.profile().orElseThrow();
        return ProfileCredentialsProvider.create(profileName);
    }
}
