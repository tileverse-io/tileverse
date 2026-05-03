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
import io.tileverse.storage.StoragePattern;
import io.tileverse.storage.UnsupportedCapabilityException;
import io.tileverse.storage.WriteOptions;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
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
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.jspecify.annotations.Nullable;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
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
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Error;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
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
    private final StorageCapabilities capabilities;
    private final boolean isDirectoryBucket;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    S3Storage(URI baseUri, S3StorageBucketKey ref, S3ClientCache.Lease lease) {
        this(baseUri, ref, new LeasedS3Handle(lease));
    }

    S3Storage(URI baseUri, S3StorageBucketKey ref, S3ClientHandle handle) {
        this.baseUri = baseUri;
        this.ref = ref;
        this.handle = handle;
        this.isDirectoryBucket = EXPRESS_BUCKET_PATTERN.matcher(ref.bucket()).matches();
        this.capabilities = buildCapabilities(this.isDirectoryBucket);
    }

    private S3AsyncClient asyncClient() {
        return handle.asyncClient()
                .orElseThrow(
                        () -> new UnsupportedCapabilityException(
                                "streaming reads via read() require an async S3 client; "
                                        + "construct this Storage via the SPI path or supply an S3ClientBundle with an async client"));
    }

    private S3TransferManager transferManager() {
        return handle.transferManager()
                .orElseThrow(() -> new UnsupportedCapabilityException("multipart upload requires an S3TransferManager; "
                        + "construct this Storage via the SPI path or supply an S3ClientBundle with a transfer manager, "
                        + "or set WriteOptions.disableMultipart()"));
    }

    private S3Presigner presigner() {
        return handle.presigner()
                .orElseThrow(() -> new UnsupportedCapabilityException("presigned URLs require an S3Presigner; "
                        + "construct this Storage via the SPI path or supply an S3ClientBundle with a presigner"));
    }

    private static StorageCapabilities buildCapabilities(boolean isDirectoryBucket) {
        StorageCapabilities.Builder b = StorageCapabilities.builder()
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
                .presignedUrls(true)
                .strongReadAfterWrite(true);
        if (isDirectoryBucket) {
            b.maxPresignTtl(Optional.of(Duration.ofMinutes(5))).versioning(false);
        } else {
            b.maxPresignTtl(Optional.of(Duration.ofDays(7))).versioning(true);
        }
        return b.build();
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
        String fullKey = resolve(key);
        try {
            HeadObjectResponse resp = handle.client()
                    .headObject(HeadObjectRequest.builder()
                            .bucket(ref.bucket())
                            .key(fullKey)
                            .build());
            return Optional.of(new StorageEntry.File(
                    key,
                    resp.contentLength(),
                    resp.lastModified(),
                    Optional.ofNullable(resp.eTag()),
                    normalizeVersionId(resp.versionId()),
                    Optional.ofNullable(resp.contentType()),
                    resp.metadata() == null ? Map.of() : Map.copyOf(resp.metadata())));
        } catch (NoSuchKeyException e) {
            return Optional.empty();
        } catch (S3Exception e) {
            if (e.statusCode() == 404) return Optional.empty();
            throw S3ExceptionMapper.map(e, key);
        }
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

        // Force the first page read up-front so wrong-region / missing-bucket / permission
        // errors surface synchronously as a typed StorageException from the list(...) call
        // itself rather than escaping raw during stream consumption.
        ListObjectsV2Iterable paginator;
        Iterator<ListObjectsV2Response> rawPages;
        try {
            paginator = handle.client().listObjectsV2Paginator(requestBuilder.build());
            rawPages = paginator.iterator();
            rawPages.hasNext(); // force first-page fetch to surface auth/region errors here
        } catch (S3Exception e) {
            throw S3ExceptionMapper.map(e, fullPrefix);
        }

        Iterator<ListObjectsV2Response> wrapped = wrapListIterator(rawPages, fullPrefix);
        Stream<ListObjectsV2Response> pageStream =
                StreamSupport.stream(Spliterators.spliteratorUnknownSize(wrapped, Spliterator.ORDERED), false);

        boolean recursive = parsed.walkDescendants();
        return pageStream.flatMap(page -> entriesFromPage(page, recursive)).filter(entry -> matcher.test(entry.key()));
    }

    private Stream<StorageEntry> entriesFromPage(ListObjectsV2Response page, boolean recursive) {
        Stream<StorageEntry> files = page.contents().stream().map(this::toFileEntry);
        // Recursive listings (** glob) emit only File entries. Non-recursive listings additionally
        // surface S3's synthetic CommonPrefixes (i.e. "directories") as Prefix entries.
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

    private static <T> Iterator<T> wrapListIterator(Iterator<T> raw, String contextKey) {
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                try {
                    return raw.hasNext();
                } catch (S3Exception e) {
                    throw S3ExceptionMapper.map(e, contextKey);
                }
            }

            @Override
            public T next() {
                try {
                    return raw.next();
                } catch (S3Exception e) {
                    throw S3ExceptionMapper.map(e, contextKey);
                }
            }
        };
    }

    @Override
    public RangeReader openRangeReader(String key) {
        requireOpen();
        String fullKey = resolve(key);
        if (stat(key).isEmpty()) {
            throw new NotFoundException("Object not found: s3://" + ref.bucket() + "/" + fullKey);
        }
        // Reuse our cached S3Client (which already has the right endpoint, region, credentials).
        return new S3RangeReader(handle.client(), new S3Reference(null, ref.bucket(), fullKey, null));
    }

    @Override
    public ReadHandle read(String key, ReadOptions options) {
        requireOpen();
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
        try {
            // Route through the CRT async client so a large GET (whole-object or large range)
            // can be split across connections internally; the toBlockingInputStream() transformer
            // hands the consumer a sync InputStream backed by CRT's parallel download.
            ResponseInputStream<GetObjectResponse> raw = asyncClient()
                    .getObject(requestBuilder.build(), AsyncResponseTransformer.toBlockingInputStream())
                    .join();
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
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof S3Exception s3e) {
                throw S3ExceptionMapper.map(s3e, key);
            }
            if (cause instanceof IOException io) {
                throw new StorageException("read failed for: " + key, io);
            }
            throw new StorageException("read failed for: " + key, cause);
        } catch (S3Exception e) {
            throw S3ExceptionMapper.map(e, key);
        }
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
        try {
            handle.client().putObject(requestBuilder.build(), RequestBody.fromBytes(data));
        } catch (S3Exception e) {
            throw S3ExceptionMapper.map(e, key);
        }
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
        // Empty Path uploads also confuse some backends, so we route empties through fromBytes.
        if (sourceSize == 0L) {
            try {
                handle.client().putObject(putBuilder.build(), RequestBody.empty());
            } catch (S3Exception e) {
                throw S3ExceptionMapper.map(e, key);
            }
        } else if (options.disableMultipart()) {
            try {
                handle.client().putObject(putBuilder.build(), source);
            } catch (S3Exception e) {
                throw S3ExceptionMapper.map(e, key);
            }
        } else {
            PutObjectRequest putObjectRequest = putBuilder.build();
            UploadFileRequest uploadRequest = UploadFileRequest.builder()
                    .source(source)
                    .putObjectRequest(putObjectRequest)
                    .build();
            FileUpload fileUpload = transferManager().uploadFile(uploadRequest);
            try {
                fileUpload.completionFuture().join();
            } catch (CompletionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof S3Exception s3e) {
                    throw S3ExceptionMapper.map(s3e, key);
                }
                throw new StorageException("Upload failed for: " + key, cause);
            }
        }
        return stat(key).orElseThrow(() -> new StorageException("Wrote key but stat failed: " + key));
    }

    @Override
    public OutputStream openOutputStream(String key, WriteOptions options) {
        requireOpen();
        Storage.requireSafeKey(key);
        Path tmp;
        OutputStream sink;
        try {
            tmp = Files.createTempFile("s3storage-", ".part");
            sink = Files.newOutputStream(tmp, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new StorageException("Could not create temp file for: " + key, e);
        }
        final Path tmpFinal = tmp;
        return new FilterOutputStream(sink) {
            private boolean alreadyClosed;

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                out.write(b, off, len);
            }

            @Override
            public void close() throws IOException {
                if (alreadyClosed) return;
                alreadyClosed = true;
                try {
                    super.close();
                    try {
                        S3Storage.this.put(key, tmpFinal, options);
                    } catch (StorageException e) {
                        throw new IOException(e);
                    }
                } finally {
                    Files.deleteIfExists(tmpFinal);
                }
            }
        };
    }

    @Override
    public void delete(String key) {
        requireOpen();
        String fullKey = resolve(key);
        try {
            handle.client()
                    .deleteObject(DeleteObjectRequest.builder()
                            .bucket(ref.bucket())
                            .key(fullKey)
                            .build());
        } catch (S3Exception e) {
            throw S3ExceptionMapper.map(e, key);
        }
    }

    @Override
    public DeleteResult deleteAll(Collection<String> keys) {
        requireOpen();
        Set<String> deleted = new HashSet<>();
        Map<String, StorageException> failed = new HashMap<>();

        final List<String> all = List.copyOf(keys);
        final int batchSize = capabilities.bulkDeleteBatchLimit();
        S3Client client = handle.client();

        for (int i = 0; i < all.size(); i += batchSize) {
            List<String> batch = all.subList(i, Math.min(i + batchSize, all.size()));
            List<ObjectIdentifier> ids = batch.stream()
                    .map(k -> ObjectIdentifier.builder().key(resolve(k)).build())
                    .toList();
            try {
                DeleteObjectsRequest deleteObjectsRequest = DeleteObjectsRequest.builder()
                        .bucket(ref.bucket())
                        .delete(d -> d.objects(ids).quiet(false))
                        .build();
                DeleteObjectsResponse resp = client.deleteObjects(deleteObjectsRequest);
                for (DeletedObject d : resp.deleted()) {
                    deleted.add(ref.relativize(d.key()));
                }
                for (S3Error err : resp.errors()) {
                    String relKey = ref.relativize(err.key());
                    failed.put(relKey, new StorageException(err.code() + ": " + err.message()));
                }
            } catch (S3Exception e) {
                batch.forEach(k -> failed.put(k, S3ExceptionMapper.map(e, k)));
            }
        }
        // S3's DeleteObjects API does not flag missing keys; deleteReportsExistence is reported false in capabilities.
        return new DeleteResult(deleted, Set.of(), failed);
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
        try {
            handle.client().copyObject(requestBuilder.build());
        } catch (S3Exception e) {
            throw S3ExceptionMapper.map(e, srcKey);
        }
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
                .getObjectRequest(r -> r.bucket(ref.bucket()).key(resolve(key)))
                .build();
        URL url = presigner().presignGetObject(presignReq).url();
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
                })
                .build();
        URL url = presigner().presignPutObject(presignReq).url();
        return URI.create(url.toString());
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
        return builder;
    }
}
