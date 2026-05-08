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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

/**
 * Per-provider reference-counted cache of {@link S3Client}, {@link S3AsyncClient}, {@link S3TransferManager}, and
 * {@link S3Presigner} instances, keyed by (region, endpoint, credentials profile, forcePathStyle). Multiple
 * {@link S3Storage} instances sharing the same key share one underlying client set; the SDK clients are closed when the
 * last lease is released.
 */
@NullMarked
final class S3ClientCache {

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
        final S3Client syncClient;
        final S3AsyncClient asyncClient;
        final S3TransferManager transferManager;
        final S3Presigner presigner;
        final AtomicInteger refCount = new AtomicInteger();

        Entry(S3Client sync, S3AsyncClient async, S3TransferManager tm, S3Presigner ps) {
            this.syncClient = sync;
            this.asyncClient = async;
            this.transferManager = tm;
            this.presigner = ps;
        }

        void closeAll() {
            transferManager.close();
            asyncClient.close();
            syncClient.close();
            presigner.close();
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
        S3Configuration syncConfig = S3Configuration.builder()
                .pathStyleAccessEnabled(key.forcePathStyle())
                .build();

        S3ClientBuilder syncBuilder =
                S3Client.builder().region(Region.of(key.region())).serviceConfiguration(syncConfig);
        // The async client uses the CRT-tuned builder so the underlying native runtime can
        // parallelize multipart uploads (via S3TransferManager) and split large GET requests
        // across connections (via AsyncResponseTransformer.toBlockingInputStream). The CRT
        // builder doesn't accept S3Configuration; pathStyle is set directly on the builder.
        S3CrtAsyncClientBuilder asyncBuilder = S3AsyncClient.crtBuilder()
                .region(Region.of(key.region()))
                .forcePathStyle(key.forcePathStyle())
                // S3-compatible emulators (LocalStack, older MinIO) don't always emit the
                // checksum headers CRT computes by default. WHEN_REQUIRED keeps validation
                // for callers that explicitly opt in via GetObjectRequest#checksumMode while
                // tolerating emulators that don't echo a server-computed checksum.
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
        //   4. DefaultCredentialsProvider (env vars, system props, ~/.aws/credentials,
        //      EC2 instance metadata, web identity, etc.).
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
            creds = DefaultCredentialsProvider.builder().build();
        }
        syncBuilder.credentialsProvider(creds);
        asyncBuilder.credentialsProvider(creds);
        presignerBuilder.credentialsProvider(creds);

        S3Client sync = syncBuilder.build();
        S3AsyncClient async = asyncBuilder.build();
        S3TransferManager tm = S3TransferManager.builder().s3Client(async).build();
        S3Presigner ps = presignerBuilder.build();
        return new Entry(sync, async, tm, ps);
    }
}
