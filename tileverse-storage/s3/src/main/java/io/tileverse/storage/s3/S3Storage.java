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
import io.tileverse.storage.StorageException;
import io.tileverse.storage.StorageOutputStream;
import io.tileverse.storage.StoragePattern;
import io.tileverse.storage.UnsupportedCapabilityException;
import io.tileverse.storage.WriteOptions;
import io.tileverse.storage.batch.BatchSettings;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.jspecify.annotations.Nullable;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.DeletedObject;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.MetadataDirective;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.RequestPayer;
import software.amazon.awssdk.services.s3.model.S3Error;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

/**
 * AWS S3 implementation of {@link Storage}, supporting both general-purpose buckets and S3 Express One Zone (Directory
 * Buckets). Variant detection is based on the bucket name pattern at construction time; the declared capabilities
 * differ accordingly.
 */
final class S3Storage implements Storage {

    /** Bucket-name pattern that identifies an S3 Express directory bucket. */
    private static final Pattern EXPRESS_BUCKET_PATTERN = Pattern.compile(".+--[a-z0-9-]+--x-s3$");

    private final URI baseUri;
    private final S3StorageBucketKey ref;
    private final S3ClientHandle handle;
    private final @Nullable URI endpoint;
    private final StorageCapabilities capabilities;
    private final boolean isDirectoryBucket;
    private final boolean requesterPays;
    private final BatchSettings batchSettings;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    S3Storage(URI baseUri, S3StorageBucketKey ref, S3ClientCache.Lease lease, boolean requesterPays) {
        this(baseUri, ref, new LeasedS3Handle(lease), requesterPays, BatchSettings.objectStoreDefaults());
    }

    S3Storage(
            URI baseUri,
            S3StorageBucketKey ref,
            S3ClientCache.Lease lease,
            boolean requesterPays,
            BatchSettings batchSettings) {
        this(baseUri, ref, new LeasedS3Handle(lease), requesterPays, batchSettings);
    }

    S3Storage(URI baseUri, S3StorageBucketKey ref, S3ClientHandle handle, boolean requesterPays) {
        this(baseUri, ref, handle, requesterPays, BatchSettings.objectStoreDefaults());
    }

    /** @param batchSettings the merge policy and in-flight bound handed to every reader this Storage opens */
    S3Storage(
            URI baseUri,
            S3StorageBucketKey ref,
            S3ClientHandle handle,
            boolean requesterPays,
            BatchSettings batchSettings) {
        this.baseUri = baseUri;
        this.ref = ref;
        this.handle = handle;
        this.endpoint = resolveEndpoint(handle.client());
        this.requesterPays = requesterPays;
        this.batchSettings = Objects.requireNonNull(batchSettings, "batchSettings cannot be null");
        this.isDirectoryBucket = EXPRESS_BUCKET_PATTERN.matcher(ref.bucket()).matches();
        this.capabilities = buildCapabilities(this.isDirectoryBucket, handle.presigns());
    }

    /**
     * The endpoint override configured on the SDK client, or {@code null} for the default AWS endpoints. Every reader
     * opened by this Storage includes it in its source identifier: the identifier partitions the shared range cache,
     * and the same bucket and key on two endpoints are two different objects. The client is the only place that knows
     * the effective endpoint on every construction path, borrowed clients included.
     */
    private static @Nullable URI resolveEndpoint(S3AsyncClient client) {
        try {
            return client.serviceClientConfiguration().endpointOverride().orElse(null);
        } catch (UnsupportedOperationException e) {
            // A hand-written S3AsyncClient implementation keeping the interface's default method. Without an endpoint
            // to tell sources apart, the readers identify as canonical s3:// URIs.
            return null;
        }
    }

    /**
     * Applies {@link RequestPayer#REQUESTER} to the given AWS request builder when this Storage is configured for a
     * Requester Pays bucket. Centralised so the conditional does not have to be repeated at every {@code *.builder()}
     * call site.
     */
    private void applyRequesterPays(Consumer<RequestPayer> setter) {
        if (requesterPays) {
            setter.accept(RequestPayer.REQUESTER);
        }
    }

    private S3Presigner presigner() {
        return handle.presigner()
                .orElseThrow(() -> new UnsupportedCapabilityException("presigned URLs need a Storage opened from a URI "
                        + "or a StorageConfig; a Storage over a caller-supplied S3AsyncClient has no presigner"));
    }

    private static StorageCapabilities buildCapabilities(boolean isDirectoryBucket, boolean presigns) {
        return StorageCapabilities.builder()
                .rangeReads(true)
                .streamingReads(true)
                .stat(true)
                .userMetadata(true)
                .list(true)
                .hierarchicalList(true)
                .realDirectories(false)
                .writes(true)
                .multipartUpload(true)
                .multipartThresholdBytes(8L * 1024 * 1024)
                .conditionalWrite(true)
                .bulkDelete(true)
                .bulkDeleteBatchLimit(1000)
                .deleteReportsExistence(false)
                .serverSideCopy(true)
                .atomicMove(false)
                .presignedUrls(presigns)
                .maxPresignTtl(maxPresignTtl(isDirectoryBucket, presigns))
                .strongReadAfterWrite(true)
                .versioning(!isDirectoryBucket)
                .build();
    }

    /** The longest time-to-live of a presigned URL; empty when the Storage cannot presign. */
    private static Optional<Duration> maxPresignTtl(boolean isDirectoryBucket, boolean presigns) {
        if (!presigns) {
            return Optional.empty();
        }
        Duration max = isDirectoryBucket ? Duration.ofMinutes(5) : Duration.ofDays(7);
        return Optional.of(max);
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
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            handle.close();
        }
    }

    private void requireOpen() {
        if (closed.get()) {
            throw new IllegalStateException("S3Storage is closed");
        }
    }

    String bucket() {
        return ref.bucket();
    }

    String resolve(String key) {
        return ref.resolve(key);
    }

    boolean isDirectoryBucket() {
        return isDirectoryBucket;
    }

    @Override
    public Optional<StorageEntry.File> stat(String key) {
        requireOpen();
        HeadObjectRequest.Builder requestBuilder =
                HeadObjectRequest.builder().bucket(ref.bucket()).key(resolve(key));
        applyRequesterPays(requestBuilder::requestPayer);
        HeadObjectRequest request = requestBuilder.build();
        try {
            HeadObjectResponse resp = S3Calls.await(key, () -> handle.client().headObject(request));
            return Optional.of(fileEntryOf(key, resp));
        } catch (NotFoundException missing) {
            return Optional.empty();
        }
    }

    private static StorageEntry.File fileEntryOf(String key, HeadObjectResponse resp) {
        return new StorageEntry.File(
                key,
                resp.contentLength(),
                resp.lastModified(),
                Optional.ofNullable(resp.eTag()),
                normalizeVersionId(resp.versionId()),
                Optional.ofNullable(resp.contentType()),
                resp.metadata() == null ? Map.of() : Map.copyOf(resp.metadata()));
    }

    @Override
    public Stream<StorageEntry> list(String pattern, ListOptions options) {
        requireOpen();
        Storage.requireSafePattern(pattern);
        StoragePattern parsed = StoragePattern.parse(pattern);
        // Skip resolve() because parsed.prefix() may be empty (legitimate root-listing); requireSafePattern above has
        // already validated the pattern, so concatenating the bucket prefix directly is safe.
        String fullPrefix = ref.prefix() + parsed.prefix();
        Predicate<String> matcher = parsed.matcher().orElse(k -> true);

        ListObjectsV2Request.Builder requestBuilder =
                ListObjectsV2Request.builder().bucket(ref.bucket()).prefix(fullPrefix);
        if (!parsed.walkDescendants()) {
            requestBuilder.delimiter("/");
        }
        if (options.pageSize().isPresent()) {
            requestBuilder.maxKeys(options.pageSize().getAsInt());
        }
        applyRequesterPays(requestBuilder::requestPayer);

        Iterator<ListObjectsV2Response> pages = new ListPages(requestBuilder.build(), fullPrefix);
        Stream<ListObjectsV2Response> pageStream =
                StreamSupport.stream(Spliterators.spliteratorUnknownSize(pages, Spliterator.ORDERED), false);

        boolean recursive = parsed.walkDescendants();
        return pageStream.flatMap(page -> entriesFromPage(page, recursive)).filter(entry -> matcher.test(entry.key()));
    }

    private Stream<StorageEntry> entriesFromPage(ListObjectsV2Response page, boolean recursive) {
        Stream<StorageEntry> files = page.contents().stream().map(this::toFileEntry);
        // Recursive listings (** glob) emit only File entries. Non-recursive listings additionally
        // expose S3's synthetic CommonPrefixes (i.e. "directories") as Prefix entries.
        if (recursive) {
            return files;
        }
        Stream<StorageEntry> prefixes =
                page.commonPrefixes().stream().map(common -> new StorageEntry.Prefix(ref.relativize(common.prefix())));
        return Stream.concat(files, prefixes);
    }

    private StorageEntry.File toFileEntry(S3Object obj) {
        String relKey = ref.relativize(obj.key());
        return new StorageEntry.File(
                relKey,
                obj.size() == null ? 0L : obj.size(),
                obj.lastModified() == null ? Instant.EPOCH : obj.lastModified(),
                Optional.ofNullable(obj.eTag()),
                normalizeVersionId(null),
                Optional.empty(),
                Map.of());
    }

    /**
     * The pages of one listing, fetched one at a time as the stream advances. The first page is fetched on
     * construction: a missing bucket or a denied listing fails the {@code list} call itself, before the stream's
     * consumer starts.
     */
    private final class ListPages implements Iterator<ListObjectsV2Response> {

        private final String contextKey;

        @Nullable
        private ListObjectsV2Request nextRequest;

        @Nullable
        private ListObjectsV2Response fetched;

        ListPages(ListObjectsV2Request firstRequest, String contextKey) {
            this.contextKey = contextKey;
            this.fetched = fetch(firstRequest);
        }

        @Override
        public boolean hasNext() {
            if (fetched == null && nextRequest != null) {
                fetched = fetch(nextRequest);
            }
            return fetched != null;
        }

        @Override
        public ListObjectsV2Response next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            ListObjectsV2Response page = fetched;
            fetched = null;
            return page;
        }

        /** Fetches a page and keeps the request of the next one, if the listing goes on. */
        private ListObjectsV2Response fetch(ListObjectsV2Request request) {
            ListObjectsV2Response page =
                    S3Calls.await(contextKey, () -> handle.client().listObjectsV2(request));
            nextRequest = continuationOf(request, page);
            return page;
        }
    }

    /**
     * The request of the page after {@code page}; {@code null} when the page names no continuation token. The token
     * decides whatever {@code IsTruncated} says, as in the paginator of the SDK: the listing of a server omitting the
     * flag misses no key.
     */
    private static @Nullable ListObjectsV2Request continuationOf(
            ListObjectsV2Request request, ListObjectsV2Response page) {
        String token = page.nextContinuationToken();
        if (token == null || token.isEmpty()) {
            return null;
        }
        return request.toBuilder().continuationToken(token).build();
    }

    @Override
    public RangeReader openRangeReader(String key) {
        requireOpen();
        String fullKey = resolve(key);
        // Reuse our cached SDK clients (which already have the right endpoint, region, credentials).
        // Opening issues no request; a missing key is reported by the first read or size() call.
        return new S3RangeReader(
                handle.client(), new S3Reference(endpoint, ref.bucket(), fullKey, null), requesterPays, batchSettings);
    }

    /** Streams the object through the async client, over one connection. */
    @Override
    public ReadHandle read(String key, ReadOptions options) {
        requireOpen();
        GetObjectRequest request = getRequestFor(key, options);
        ResponseInputStream<GetObjectResponse> raw = S3Calls.await(
                key,
                () -> handle.client().getObject(request, AsyncResponseTransformer.toBlockingInputStream()),
                ResponseInputStream::abort);
        return readHandleFor(key, raw);
    }

    /** Builds the GET behind a streaming read, honoring the version, range and conditional options. */
    private GetObjectRequest getRequestFor(String key, ReadOptions options) {
        String fullKey = resolve(key);
        GetObjectRequest.Builder requestBuilder =
                GetObjectRequest.builder().bucket(ref.bucket()).key(fullKey);
        options.versionId().ifPresent(requestBuilder::versionId);
        if (options.offset() > 0L || options.length().isPresent()) {
            String range;
            if (options.length().isPresent()) {
                long end = options.offset() + options.length().getAsLong() - 1L;
                range = "bytes=" + options.offset() + "-" + end;
            } else {
                range = "bytes=" + options.offset() + "-";
            }
            requestBuilder.range(range);
        }
        options.ifMatchEtag().ifPresent(requestBuilder::ifMatch);
        options.ifModifiedSince().ifPresent(requestBuilder::ifModifiedSince);
        applyRequesterPays(requestBuilder::requestPayer);
        return requestBuilder.build();
    }

    private ReadHandle readHandleFor(String key, ResponseInputStream<GetObjectResponse> raw) {
        GetObjectResponse resp = raw.response();
        StorageEntry.File metadata = new StorageEntry.File(
                key,
                resp.contentLength() == null ? 0L : resp.contentLength(),
                resp.lastModified() == null ? Instant.now() : resp.lastModified(),
                Optional.ofNullable(resp.eTag()),
                normalizeVersionId(resp.versionId()),
                Optional.ofNullable(resp.contentType()),
                resp.metadata() == null ? Map.of() : Map.copyOf(resp.metadata()));
        return new ReadHandle(new StorageExceptionTranslatingInputStream(raw), metadata);
    }

    /**
     * Wraps an InputStream so any unchecked StorageException raised during read/skip/close is translated to IOException
     * to satisfy the InputStream JDK contract.
     */
    private static final class StorageExceptionTranslatingInputStream extends FilterInputStream {
        StorageExceptionTranslatingInputStream(InputStream in) {
            super(in);
        }

        @Override
        public int read() throws IOException {
            try {
                return super.read();
            } catch (StorageException e) {
                throw new IOException(e);
            }
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            try {
                return super.read(b, off, len);
            } catch (StorageException e) {
                throw new IOException(e);
            }
        }

        @Override
        public long skip(long n) throws IOException {
            try {
                return super.skip(n);
            } catch (StorageException e) {
                throw new IOException(e);
            }
        }

        @Override
        public void close() throws IOException {
            try {
                super.close();
            } catch (StorageException e) {
                throw new IOException(e);
            }
        }
    }

    @Override
    public StorageEntry.File put(String key, byte[] data, WriteOptions options) {
        requireOpen();
        String fullKey = resolve(key);
        PutObjectRequest.Builder requestBuilder = putRequestBuilder(fullKey, options);
        if (options.contentLength().isPresent()) {
            requestBuilder.contentLength(options.contentLength().getAsLong());
        }
        putInOneRequest(key, requestBuilder.build(), AsyncRequestBody.fromBytes(data));
        return stat(key).orElseThrow(() -> new StorageException("Wrote key but stat failed: " + key));
    }

    @Override
    public StorageEntry.File put(String key, Path source, WriteOptions options) {
        requireOpen();
        String fullKey = resolve(key);
        PutObjectRequest.Builder putBuilder = putRequestBuilder(fullKey, options);

        long sourceSize;
        try {
            sourceSize = Files.size(source);
        } catch (IOException e) {
            throw new StorageException("Could not stat source file: " + source, e);
        }
        // For zero-byte files, single-shot PutObject is required: TransferManager / multipart
        // upload of empty content is rejected by some S3-compatible backends (notably LocalStack).
        // An empty body replaces the file: empty Path uploads also confuse some backends.
        if (sourceSize == 0L) {
            putInOneRequest(key, putBuilder.build(), AsyncRequestBody.empty());
        } else if (options.disableMultipart()) {
            putInOneRequest(key, putBuilder.build(), AsyncRequestBody.fromFile(source));
        } else {
            uploadInParts(key, putBuilder.build(), source);
        }
        return stat(key).orElseThrow(() -> new StorageException("Wrote key but stat failed: " + key));
    }

    private void putInOneRequest(String key, PutObjectRequest request, AsyncRequestBody body) {
        S3Calls.await(key, () -> handle.client().putObject(request, body));
    }

    private void uploadInParts(String key, PutObjectRequest request, Path source) {
        UploadFileRequest upload = UploadFileRequest.builder()
                .source(source)
                .putObjectRequest(request)
                .build();
        S3TransferManager transferManager = handle.transferManager();
        S3Calls.await(key, () -> {
            FileUpload started = transferManager.uploadFile(upload);
            return started.completionFuture();
        });
    }

    @Override
    public StorageOutputStream openOutputStream(String key, WriteOptions options) {
        requireOpen();
        Storage.requireSafeKey(key);
        return StorageOutputStream.spoolingInTempDir("s3storage-", spooled -> put(key, spooled, options));
    }

    @Override
    public void delete(String key) {
        requireOpen();
        String fullKey = resolve(key);
        DeleteObjectRequest.Builder requestBuilder =
                DeleteObjectRequest.builder().bucket(ref.bucket()).key(fullKey);
        applyRequesterPays(requestBuilder::requestPayer);
        DeleteObjectRequest request = requestBuilder.build();
        S3Calls.await(key, () -> handle.client().deleteObject(request));
    }

    @Override
    public DeleteResult deleteAll(Collection<String> keys) {
        requireOpen();
        Set<String> deleted = new HashSet<>();
        Map<String, StorageException> failed = new HashMap<>();
        final List<String> all = List.copyOf(keys);
        final int batchSize = capabilities.bulkDeleteBatchLimit();
        for (int i = 0; i < all.size(); i += batchSize) {
            List<String> batch = all.subList(i, Math.min(i + batchSize, all.size()));
            deleteBatch(batch, deleted, failed);
        }
        // S3's DeleteObjects API does not flag missing keys; deleteReportsExistence is reported false in capabilities.
        return new DeleteResult(deleted, Set.of(), failed);
    }

    /**
     * Deletes one batch and records the outcome per key. A service error fails the keys of the batch and leaves the
     * next batches to run; a client-side failure or an interrupt ends the call.
     */
    private void deleteBatch(List<String> batch, Set<String> deleted, Map<String, StorageException> failed) {
        List<ObjectIdentifier> ids = batch.stream()
                .map(k -> ObjectIdentifier.builder().key(resolve(k)).build())
                .toList();
        DeleteObjectsRequest.Builder requestBuilder = DeleteObjectsRequest.builder()
                .bucket(ref.bucket())
                .delete(d -> d.objects(ids).quiet(false));
        applyRequesterPays(requestBuilder::requestPayer);
        DeleteObjectsRequest request = requestBuilder.build();
        DeleteObjectsResponse resp;
        try {
            resp = S3Calls.await(batch.get(0), () -> handle.client().deleteObjects(request));
        } catch (StorageException batchFailure) {
            if (!(batchFailure.getCause() instanceof S3Exception serviceFailure)) {
                throw batchFailure;
            }
            batch.forEach(k -> failed.put(k, S3ExceptionMapper.map(serviceFailure, k)));
            return;
        }
        for (DeletedObject d : resp.deleted()) {
            deleted.add(ref.relativize(d.key()));
        }
        for (S3Error err : resp.errors()) {
            failed.put(ref.relativize(err.key()), new StorageException(err.code() + ": " + err.message()));
        }
    }

    @Override
    public StorageEntry.File copy(String srcKey, String dstKey, CopyOptions options) {
        requireOpen();
        return copyInternal(srcKey, this, dstKey, options);
    }

    @Override
    public StorageEntry.File copy(String srcKey, Storage dst, String dstKey, CopyOptions options) {
        requireOpen();
        if (!(dst instanceof S3Storage other)) {
            throw new UnsupportedCapabilityException(
                    "cross-backend copy from S3Storage to " + dst.getClass().getSimpleName());
        }
        return copyInternal(srcKey, other, dstKey, options);
    }

    private StorageEntry.File copyInternal(String srcKey, S3Storage dst, String dstKey, CopyOptions options) {
        String fullSrc = resolve(srcKey);
        String fullDst = dst.resolve(dstKey);
        if (options.ifNotExistsAtDestination() && dst.stat(dstKey).isPresent()) {
            throw new PreconditionFailedException("Destination already exists: " + dstKey);
        }
        CopyObjectRequest.Builder requestBuilder = CopyObjectRequest.builder()
                .sourceBucket(ref.bucket())
                .sourceKey(fullSrc)
                .destinationBucket(dst.bucket())
                .destinationKey(fullDst);
        options.ifMatchSourceEtag().ifPresent(requestBuilder::copySourceIfMatch);
        options.overrideUserMetadata().ifPresent(md -> {
            requestBuilder.metadataDirective(MetadataDirective.REPLACE);
            requestBuilder.metadata(md);
        });
        applyRequesterPays(requestBuilder::requestPayer);
        CopyObjectRequest request = requestBuilder.build();
        S3Calls.await(srcKey, () -> handle.client().copyObject(request));
        return dst.stat(dstKey).orElseThrow(() -> new StorageException("Copy failed for: " + dstKey));
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
        GetObjectPresignRequest presignReq = GetObjectPresignRequest.builder()
                .signatureDuration(ttl)
                .getObjectRequest(r -> {
                    r.bucket(ref.bucket()).key(resolve(key));
                    applyRequesterPays(r::requestPayer);
                })
                .build();
        URL url = presign(key, signer -> signer.presignGetObject(presignReq).url());
        return URI.create(url.toString());
    }

    @Override
    public URI presignPut(String key, Duration ttl, PresignWriteOptions options) {
        requireOpen();
        Duration max = capabilities.maxPresignTtl().orElse(Duration.ofDays(7));
        if (ttl.compareTo(max) > 0) {
            throw new IllegalArgumentException("ttl exceeds max for this backend: " + max);
        }
        PutObjectPresignRequest presignReq = PutObjectPresignRequest.builder()
                .signatureDuration(ttl)
                .putObjectRequest(b -> {
                    b.bucket(ref.bucket()).key(resolve(key));
                    options.contentType().ifPresent(b::contentType);
                    applyRequesterPays(b::requestPayer);
                })
                .build();
        URL url = presign(key, signer -> signer.presignPutObject(presignReq).url());
        return URI.create(url.toString());
    }

    /**
     * Signs with the presigner of the handle. A failure of the SDK, such as missing credentials, arrives as a
     * {@link StorageException}, as for requests.
     */
    private URL presign(String key, Function<S3Presigner, URL> signing) {
        S3Presigner presigner = presigner();
        try {
            return signing.apply(presigner);
        } catch (SdkException failure) {
            throw S3Calls.map(failure, key);
        }
    }

    /**
     * Maps {@code null}, the empty string, and the literal {@code "null"} (S3's sentinel for objects in unversioned
     * buckets) to {@link Optional#empty()}; all other values are wrapped in {@link Optional#of(Object)}.
     */
    private static Optional<String> normalizeVersionId(@Nullable String raw) {
        if (raw == null || raw.isEmpty() || "null".equals(raw)) {
            return Optional.empty();
        }
        return Optional.of(raw);
    }

    private PutObjectRequest.Builder putRequestBuilder(String fullKey, WriteOptions options) {
        PutObjectRequest.Builder builder =
                PutObjectRequest.builder().bucket(ref.bucket()).key(fullKey);
        options.contentType().ifPresent(builder::contentType);
        if (!options.userMetadata().isEmpty()) {
            builder.metadata(options.userMetadata());
        }
        if (options.ifNotExists()) {
            builder.ifNoneMatch("*");
        }
        options.ifMatchEtag().ifPresent(builder::ifMatch);
        applyRequesterPays(builder::requestPayer);
        return builder;
    }
}
