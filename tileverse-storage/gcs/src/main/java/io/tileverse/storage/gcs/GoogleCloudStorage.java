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

import com.google.api.gax.paging.Page;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage.BlobTargetOption;
import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.cloud.storage.Storage.CopyRequest;
import com.google.cloud.storage.Storage.SignUrlOption;
import com.google.cloud.storage.StorageException;
import io.tileverse.storage.CopyOptions;
import io.tileverse.storage.DeleteResult;
import io.tileverse.storage.ListOptions;
import io.tileverse.storage.NotFoundException;
import io.tileverse.storage.PreconditionFailedException;
import io.tileverse.storage.PresignWriteOptions;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.ReadHandle;
import io.tileverse.storage.ReadOptions;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageCapabilities;
import io.tileverse.storage.StorageEntry;
import io.tileverse.storage.StoragePattern;
import io.tileverse.storage.UnsupportedCapabilityException;
import io.tileverse.storage.WriteOptions;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.Channels;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Google Cloud Storage implementation of {@link Storage} (flat keyspace + virtual directories, like S3 and Azure Blob).
 *
 * <p>The Storage is rooted at a {@code gs://bucket/[prefix]} (or HTTPS equivalent) URI; keys passed to read/write
 * methods are resolved relative to that prefix. Conditional writes use object generation numbers as the version tag
 * (mapped to {@link StorageEntry.File#etag()}). Presigned URLs use V4 signing and require a service-account credential.
 */
final class GoogleCloudStorage implements Storage {

    private final URI baseUri;
    private final SdkStorageLocation location;
    private final GcsClientHandle handle;
    private final StorageCapabilities capabilities;
    private final boolean isHns;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    GoogleCloudStorage(URI baseUri, SdkStorageLocation location, SdkStorageCache.Lease lease) {
        this(baseUri, location, new LeasedGcsHandle(lease));
    }

    GoogleCloudStorage(URI baseUri, SdkStorageLocation location, GcsClientHandle handle) {
        this.baseUri = baseUri;
        this.location = location;
        this.handle = handle;
        this.isHns = detectHns(handle.client(), location.bucket());
        this.capabilities = buildCapabilities(this.isHns);
    }

    private static boolean detectHns(com.google.cloud.storage.Storage client, String bucket) {
        try {
            Bucket b = client.get(bucket);
            if (b == null) {
                return false;
            }
            return b.getHierarchicalNamespace() != null
                    && Boolean.TRUE.equals(b.getHierarchicalNamespace().getEnabled());
        } catch (StorageException e) {
            return false;
        }
    }

    private static StorageCapabilities buildCapabilities(boolean isHns) {
        return StorageCapabilities.builder()
                .rangeReads(true)
                .streamingReads(true)
                .stat(true)
                .userMetadata(true)
                .list(true)
                .hierarchicalList(true)
                .realDirectories(isHns)
                .writes(true)
                .multipartUpload(true)
                .multipartThresholdBytes(8L * 1024 * 1024)
                .conditionalWrite(true)
                .bulkDelete(true)
                .bulkDeleteBatchLimit(100)
                .deleteReportsExistence(true)
                .serverSideCopy(true)
                .atomicMove(isHns)
                .presignedUrls(true)
                .maxPresignTtl(Optional.of(Duration.ofDays(7)))
                .versioning(true)
                .strongReadAfterWrite(true)
                .build();
    }

    @Override
    public URI baseUri() {
        return baseUri;
    }

    @Override
    public StorageCapabilities capabilities() {
        return capabilities;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            handle.close();
        }
    }

    private void requireOpen() {
        if (closed.get()) {
            throw new IllegalStateException("GoogleCloudStorageStorage is closed");
        }
    }

    boolean isHns() {
        return isHns;
    }

    private BlobId blobId(String key) {
        return blobId(key, null);
    }

    private BlobId blobId(String key, @Nullable Long generation) {
        String absoluteKey = location.resolve(key);
        return BlobId.of(location.bucket(), absoluteKey, generation);
    }

    @Override
    public Optional<StorageEntry.File> stat(String key) {
        requireOpen();
        try {
            Blob blob = handle.client().get(blobId(key));
            if (blob == null) {
                return Optional.empty();
            }
            String contentType = blob.getContentType();
            String etag = blob.getEtag();
            long size = blob.getSize() == null ? 0L : blob.getSize();
            Instant lastModified = blob.getUpdateTimeOffsetDateTime() == null
                    ? Instant.EPOCH
                    : blob.getUpdateTimeOffsetDateTime().toInstant();
            return Optional.of(new StorageEntry.File(
                    key,
                    size,
                    lastModified,
                    Optional.ofNullable(etag),
                    Optional.of(String.valueOf(blob.getGeneration())),
                    Optional.ofNullable(contentType),
                    blob.getMetadata() == null ? Map.of() : Map.copyOf(blob.getMetadata())));
        } catch (StorageException e) {
            if (e.getCode() == 404) {
                return Optional.empty();
            }
            throw SdkExceptionMapper.map(e, key);
        }
    }

    @Override
    public Stream<StorageEntry> list(String pattern, ListOptions options) {
        requireOpen();
        Storage.requireSafePattern(pattern);
        StoragePattern parsed = StoragePattern.parse(pattern);
        // Skip resolve() because parsed.prefix() may be empty (legitimate root-listing); requireSafePattern above has
        // already validated the pattern, so concatenating the bucket prefix directly is safe.
        String fullPrefix = location.prefix() + parsed.prefix();
        Predicate<String> matcher = parsed.matcher().orElse(k -> true);

        List<BlobListOption> opts = new ArrayList<>();
        opts.add(BlobListOption.prefix(fullPrefix));
        if (!parsed.walkDescendants()) {
            // currentDirectory() implies delimiter "/" - don't add it twice (the GCS SDK rejects duplicates).
            opts.add(BlobListOption.currentDirectory());
        }
        options.pageSize().ifPresent(p -> opts.add(BlobListOption.pageSize(p)));

        Page<Blob> page;
        Iterator<Blob> rawItems;
        try {
            page = handle.client().list(location.bucket(), opts.toArray(BlobListOption[]::new));
            rawItems = page.iterateAll().iterator();
            // Force the first-page fetch so auth/region/missing-bucket errors propagate here as typed
            // StorageException rather than escaping raw during stream consumption.
            rawItems.hasNext(); // NOSONAR java:S899 -- side-effecting call, return value intentionally ignored
        } catch (StorageException e) {
            throw SdkExceptionMapper.map(e, fullPrefix);
        }

        Iterator<Blob> wrapped = wrapGcsListIterator(rawItems, fullPrefix);
        Stream<Blob> blobStream =
                StreamSupport.stream(Spliterators.spliteratorUnknownSize(wrapped, Spliterator.ORDERED), false);
        return blobStream.map(this::toEntry).filter(entry -> matcher.test(entry.key()));
    }

    private StorageEntry toEntry(Blob blob) {
        String relKey = location.relativize(blob.getName());
        if (Boolean.TRUE.equals(blob.isDirectory())) {
            return new StorageEntry.Prefix(relKey);
        }
        long size = blob.getSize() == null ? 0L : blob.getSize();
        Instant lastModified = blob.getUpdateTimeOffsetDateTime() == null
                ? Instant.EPOCH
                : blob.getUpdateTimeOffsetDateTime().toInstant();
        String generation = blob.getGeneration() == null ? null : String.valueOf(blob.getGeneration());
        return new StorageEntry.File(
                relKey,
                size,
                lastModified,
                Optional.ofNullable(blob.getEtag()),
                Optional.ofNullable(generation),
                Optional.ofNullable(blob.getContentType()),
                Map.of());
    }

    private static <T> Iterator<T> wrapGcsListIterator(Iterator<T> raw, String contextKey) {
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                try {
                    return raw.hasNext();
                } catch (StorageException e) {
                    throw SdkExceptionMapper.map(e, contextKey);
                }
            }

            @Override
            public T next() {
                try {
                    return raw.next();
                } catch (StorageException e) {
                    throw SdkExceptionMapper.map(e, contextKey);
                }
            }
        };
    }

    @Override
    public RangeReader openRangeReader(String key) {
        requireOpen();
        if (stat(key).isEmpty()) {
            throw new NotFoundException("Blob not found: gs://" + location.bucket() + "/" + location.resolve(key));
        }
        return new GoogleCloudStorageRangeReader(handle.client(), location.bucket(), location.resolve(key));
    }

    /**
     * Strip the {@code ?alt=media} query string from the URI before the inherited validation runs. GCS REST-API URLs
     * end in {@code ?alt=media} when fetching object content, but the query string is not part of the object name.
     */
    @Override
    public String relativizeToKey(@NonNull URI uri) {
        if (uri.getRawQuery() != null) {
            try {
                URI scrubbed = new URI(
                        uri.getScheme(), uri.getAuthority(), uri.getPath(), null /* query */, uri.getRawFragment());
                return Storage.super.relativizeToKey(scrubbed);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Cannot strip query string from URI: " + uri, e);
            }
        }
        return Storage.super.relativizeToKey(uri);
    }

    @Override
    public ReadHandle read(String key, ReadOptions options) {
        requireOpen();
        Long generation = null;
        // Use options.versionId() to select a specific generation.
        if (options.versionId().isPresent()) {
            generation = Long.parseLong(options.versionId().orElseThrow());
        }
        BlobId id = blobId(key, generation);
        Blob blob = handle.client().get(id);
        if (blob == null) {
            String absoluteKey = location.resolve(key);
            throw new NotFoundException("Blob not found: gs://%s/%s".formatted(location.bucket(), absoluteKey));
        }
        try {
            ReadChannel channel = blob.reader();
            if (options.offset() > 0L) {
                channel.seek(options.offset());
            }
            if (options.length().isPresent()) {
                channel.limit(options.offset() + options.length().getAsLong());
            }
            InputStream raw = Channels.newInputStream(channel);
            long size = blob.getSize() == null ? 0L : blob.getSize();
            Instant lastModified = blob.getUpdateTimeOffsetDateTime() == null
                    ? Instant.now()
                    : blob.getUpdateTimeOffsetDateTime().toInstant();
            StorageEntry.File metadata = new StorageEntry.File(
                    key,
                    size,
                    lastModified,
                    Optional.ofNullable(blob.getEtag()),
                    Optional.of(String.valueOf(blob.getGeneration())),
                    Optional.ofNullable(blob.getContentType()),
                    blob.getMetadata() == null ? Map.of() : Map.copyOf(blob.getMetadata()));
            return new ReadHandle(new StorageExceptionTranslatingInputStream(raw, key), metadata);
        } catch (StorageException e) {
            throw SdkExceptionMapper.map(e, key);
        } catch (IOException e) {
            throw new io.tileverse.storage.StorageException("read failed for: " + key, e);
        }
    }

    /**
     * Wraps an InputStream so unchecked GCS StorageException raised during read/skip/close is mapped to a typed
     * tileverse StorageException, then translated to IOException to satisfy the InputStream JDK contract.
     */
    private static final class StorageExceptionTranslatingInputStream extends FilterInputStream {
        private final String key;

        StorageExceptionTranslatingInputStream(InputStream in, String key) {
            super(in);
            this.key = key;
        }

        @Override
        public int read() throws IOException {
            try {
                return super.read();
            } catch (StorageException e) {
                throw new IOException(SdkExceptionMapper.map(e, key));
            } catch (io.tileverse.storage.StorageException e) {
                throw new IOException(e);
            }
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            try {
                return super.read(b, off, len);
            } catch (StorageException e) {
                throw new IOException(SdkExceptionMapper.map(e, key));
            } catch (io.tileverse.storage.StorageException e) {
                throw new IOException(e);
            }
        }

        @Override
        public long skip(long n) throws IOException {
            try {
                return super.skip(n);
            } catch (StorageException e) {
                throw new IOException(SdkExceptionMapper.map(e, key));
            } catch (io.tileverse.storage.StorageException e) {
                throw new IOException(e);
            }
        }

        @Override
        public void close() throws IOException {
            try {
                super.close();
            } catch (StorageException e) {
                throw new IOException(SdkExceptionMapper.map(e, key));
            } catch (io.tileverse.storage.StorageException e) {
                throw new IOException(e);
            }
        }
    }

    @Override
    public StorageEntry.File put(String key, byte[] data, WriteOptions options) {
        requireOpen();
        BlobInfo.Builder bib = BlobInfo.newBuilder(blobId(key));
        options.contentType().ifPresent(bib::setContentType);
        if (!options.userMetadata().isEmpty()) {
            bib.setMetadata(options.userMetadata());
        }
        List<BlobTargetOption> opts = new ArrayList<>();
        if (options.ifNotExists()) {
            opts.add(BlobTargetOption.doesNotExist());
        }
        try {
            handle.client().create(bib.build(), data, opts.toArray(BlobTargetOption[]::new));
        } catch (StorageException e) {
            if (e.getCode() == 412) {
                throw new PreconditionFailedException("Key already exists: " + key, e);
            }
            throw SdkExceptionMapper.map(e, key);
        }
        return stat(key)
                .orElseThrow(() -> new io.tileverse.storage.StorageException("Wrote key but stat failed: " + key));
    }

    @Override
    public StorageEntry.File put(String key, Path source, WriteOptions options) {
        requireOpen();
        BlobInfo.Builder bib = BlobInfo.newBuilder(blobId(key));
        options.contentType().ifPresent(bib::setContentType);
        if (!options.userMetadata().isEmpty()) {
            bib.setMetadata(options.userMetadata());
        }
        List<BlobWriteOption> opts = new ArrayList<>();
        if (options.ifNotExists()) {
            opts.add(BlobWriteOption.doesNotExist());
        }
        try {
            handle.client().createFrom(bib.build(), source, opts.toArray(BlobWriteOption[]::new));
        } catch (StorageException e) {
            if (e.getCode() == 412) {
                throw new PreconditionFailedException("Key already exists: " + key, e);
            }
            throw SdkExceptionMapper.map(e, key);
        } catch (IOException e) {
            throw new io.tileverse.storage.StorageException("Failed to read source file: " + source, e);
        }
        return stat(key)
                .orElseThrow(() -> new io.tileverse.storage.StorageException("Wrote key but stat failed: " + key));
    }

    @Override
    public OutputStream openOutputStream(String key, WriteOptions options) {
        requireOpen();
        BlobInfo.Builder bib = BlobInfo.newBuilder(blobId(key));
        options.contentType().ifPresent(bib::setContentType);
        if (!options.userMetadata().isEmpty()) {
            bib.setMetadata(options.userMetadata());
        }
        List<BlobWriteOption> opts = new ArrayList<>();
        if (options.ifNotExists()) {
            opts.add(BlobWriteOption.doesNotExist());
        }
        WriteChannel writer = handle.client().writer(bib.build(), opts.toArray(BlobWriteOption[]::new));
        OutputStream sink = Channels.newOutputStream(writer);
        return new FilterOutputStream(sink) {
            private boolean alreadyClosed;

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                out.write(b, off, len);
            }

            @Override
            public void close() throws IOException {
                if (alreadyClosed) {
                    return;
                }
                alreadyClosed = true;
                try {
                    super.close();
                } catch (StorageException e) {
                    if (e.getCode() == 412) {
                        throw new PreconditionFailedException("Key already exists: " + key, e);
                    }
                    throw SdkExceptionMapper.map(e, key);
                }
            }
        };
    }

    @Override
    public void delete(String key) {
        requireOpen();
        try {
            handle.client().delete(blobId(key));
        } catch (StorageException e) {
            throw SdkExceptionMapper.map(e, key);
        }
    }

    @Override
    public DeleteResult deleteAll(Collection<String> keys) {
        requireOpen();
        List<String> keyList = new ArrayList<>(keys);
        List<BlobId> ids = keyList.stream().map(this::blobId).toList();
        Set<String> deleted = new HashSet<>();
        Set<String> didNotExist = new HashSet<>();
        Map<String, io.tileverse.storage.StorageException> failed = new HashMap<>();
        try {
            List<Boolean> results = handle.client().delete(ids);
            for (int i = 0; i < results.size(); i++) {
                String key = keyList.get(i);
                if (Boolean.TRUE.equals(results.get(i))) {
                    deleted.add(key);
                } else {
                    didNotExist.add(key);
                }
            }
        } catch (StorageException e) {
            for (String key : keyList) {
                failed.put(key, SdkExceptionMapper.map(e, key));
            }
        }
        return new DeleteResult(deleted, didNotExist, failed);
    }

    @Override
    public StorageEntry.File copy(String srcKey, String dstKey, CopyOptions options) {
        requireOpen();
        return copyInternal(srcKey, this, dstKey, options);
    }

    @Override
    public StorageEntry.File copy(String srcKey, Storage dst, String dstKey, CopyOptions options) {
        requireOpen();
        if (!(dst instanceof GoogleCloudStorage other)) {
            throw new UnsupportedCapabilityException("cross-backend copy from GoogleCloudStorageStorage to "
                    + dst.getClass().getSimpleName());
        }
        return copyInternal(srcKey, other, dstKey, options);
    }

    private StorageEntry.File copyInternal(String srcKey, GoogleCloudStorage dst, String dstKey, CopyOptions options) {
        io.tileverse.storage.Storage.requireSafeKey(srcKey);
        io.tileverse.storage.Storage.requireSafeKey(dstKey);
        if (options.ifNotExistsAtDestination() && dst.stat(dstKey).isPresent()) {
            throw new PreconditionFailedException("Destination already exists: " + dstKey);
        }
        try {
            CopyRequest req = CopyRequest.newBuilder()
                    .setSource(blobId(srcKey))
                    .setTarget(BlobId.of(dst.location.bucket(), dst.location.resolve(dstKey)))
                    .build();
            handle.client().copy(req).getResult();
        } catch (StorageException e) {
            throw SdkExceptionMapper.map(e, srcKey);
        }
        return dst.stat(dstKey)
                .orElseThrow(() -> new io.tileverse.storage.StorageException("Copy failed for: " + dstKey));
    }

    @Override
    public StorageEntry.File move(String srcKey, String dstKey, CopyOptions options) {
        requireOpen();
        StorageEntry.File copied = copy(srcKey, dstKey, options);
        delete(srcKey);
        return copied;
    }

    @Override
    public URI presignGet(String key, Duration ttl) {
        requireOpen();
        Duration max = capabilities.maxPresignTtl().orElse(Duration.ofDays(7));
        if (ttl.compareTo(max) > 0) {
            throw new IllegalArgumentException("ttl exceeds max for this backend: " + max);
        }
        try {
            BlobInfo info = BlobInfo.newBuilder(blobId(key)).build();
            return URI.create(handle.client()
                    .signUrl(
                            info,
                            ttl.toSeconds(),
                            TimeUnit.SECONDS,
                            SignUrlOption.withV4Signature(),
                            SignUrlOption.httpMethod(com.google.cloud.storage.HttpMethod.GET))
                    .toString());
        } catch (StorageException e) {
            throw SdkExceptionMapper.map(e, key);
        }
    }

    @Override
    public URI presignPut(String key, Duration ttl, PresignWriteOptions options) {
        requireOpen();
        Duration max = capabilities.maxPresignTtl().orElse(Duration.ofDays(7));
        if (ttl.compareTo(max) > 0) {
            throw new IllegalArgumentException("ttl exceeds max for this backend: " + max);
        }
        try {
            BlobInfo.Builder bib = BlobInfo.newBuilder(blobId(key));
            options.contentType().ifPresent(bib::setContentType);
            return URI.create(handle.client()
                    .signUrl(
                            bib.build(),
                            ttl.toSeconds(),
                            TimeUnit.SECONDS,
                            SignUrlOption.withV4Signature(),
                            SignUrlOption.httpMethod(com.google.cloud.storage.HttpMethod.PUT))
                    .toString());
        } catch (StorageException e) {
            throw SdkExceptionMapper.map(e, key);
        }
    }
}
