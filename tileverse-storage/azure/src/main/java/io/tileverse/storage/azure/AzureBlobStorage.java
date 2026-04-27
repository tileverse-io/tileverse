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

import com.azure.core.exception.HttpResponseException;
import com.azure.core.http.rest.PagedIterable;
import com.azure.core.http.rest.Response;
import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.batch.BlobBatchClient;
import com.azure.storage.blob.batch.BlobBatchClientBuilder;
import com.azure.storage.blob.batch.BlobBatchStorageException;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.DeleteSnapshotsOptionType;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.blob.options.BlobInputStreamOptions;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.blob.specialized.BlobInputStream;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/** Azure Blob Storage implementation of {@link Storage} (flat keyspace + virtual directories). */
final class AzureBlobStorage implements Storage {

    private final URI baseUri;
    private final AzureBlobLocation location;
    private final AzureClientHandle handle;
    private final BlobContainerClient containerClient;
    private final StorageCapabilities capabilities;

    private BlobBatchClient batchClient;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    AzureBlobStorage(URI baseUri, AzureBlobLocation location, AzureClientCache.Lease lease) {
        this(baseUri, location, new LeasedAzureHandle(lease));
    }

    AzureBlobStorage(URI baseUri, AzureBlobLocation location, AzureClientHandle handle) {
        this.baseUri = baseUri;
        this.location = location;
        this.handle = handle;
        this.containerClient = handle.blobServiceClient().getBlobContainerClient(location.container());
        this.capabilities = StorageCapabilities.builder()
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
                .bulkDeleteBatchLimit(256)
                .deleteReportsExistence(true)
                .serverSideCopy(true)
                .atomicMove(false)
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
            throw new IllegalStateException("AzureBlobStorage is closed");
        }
    }

    AzureBlobLocation location() {
        return location;
    }

    BlobContainerClient containerClient() {
        return containerClient;
    }

    private BlobClient blobClient(String key) {
        return containerClient.getBlobClient(location.resolve(key));
    }

    @Override
    public Optional<StorageEntry.File> stat(String key) {
        requireOpen();
        BlobClient client = blobClient(key);
        try {
            BlobProperties props = client.getProperties();
            Instant lastModified = props.getLastModified() == null
                    ? Instant.EPOCH
                    : props.getLastModified().toInstant();
            StorageEntry.File fileEntry = new StorageEntry.File(
                    key,
                    props.getBlobSize(),
                    lastModified,
                    Optional.ofNullable(props.getETag()),
                    Optional.ofNullable(props.getVersionId()),
                    Optional.ofNullable(props.getContentType()),
                    props.getMetadata() == null ? Map.of() : Map.copyOf(props.getMetadata()));
            return Optional.of(fileEntry);
        } catch (BlobStorageException e) {
            if (e.getStatusCode() == 404 || BlobErrorCode.BLOB_NOT_FOUND.equals(e.getErrorCode())) {
                return Optional.empty();
            }
            throw AzureExceptionMapper.map(e, key);
        }
    }

    @Override
    public Stream<StorageEntry> list(String pattern, ListOptions options) {
        requireOpen();
        StoragePattern parsed = StoragePattern.parse(pattern);
        String fullPrefix = location.resolve(parsed.prefix());
        Predicate<String> matcher = parsed.matcher().orElse(k -> true);

        ListBlobsOptions opts = new ListBlobsOptions().setPrefix(fullPrefix);
        options.pageSize().ifPresent(opts::setMaxResultsPerPage);

        Iterable<BlobItem> iter;
        Iterator<BlobItem> rawItems;
        try {
            iter = parsed.walkDescendants()
                    ? containerClient.listBlobs(opts, null)
                    : containerClient.listBlobsByHierarchy("/", opts, null);
            rawItems = iter.iterator();
            rawItems.hasNext(); // force first-page fetch to surface auth/region errors here
        } catch (HttpResponseException e) {
            throw AzureExceptionMapper.map(e, fullPrefix);
        }

        Iterator<BlobItem> wrapped = wrapListIterator(rawItems, fullPrefix);
        Stream<BlobItem> itemStream =
                StreamSupport.stream(Spliterators.spliteratorUnknownSize(wrapped, Spliterator.ORDERED), false);
        return itemStream.map(this::toEntry).filter(entry -> matcher.test(entry.key()));
    }

    private StorageEntry toEntry(BlobItem item) {
        String relKey = location.relativize(item.getName());
        if (Boolean.TRUE.equals(item.isPrefix())) {
            return new StorageEntry.Prefix(relKey);
        }
        BlobItemProperties props = item.getProperties();
        long size = props == null || props.getContentLength() == null ? 0L : props.getContentLength();
        Instant lastModified = (props == null || props.getLastModified() == null)
                ? Instant.EPOCH
                : props.getLastModified().toInstant();
        String etag = props == null ? null : props.getETag();
        String contentType = props == null ? null : props.getContentType();
        return new StorageEntry.File(
                relKey,
                size,
                lastModified,
                Optional.ofNullable(etag),
                Optional.ofNullable(item.getVersionId()),
                Optional.ofNullable(contentType),
                Map.of());
    }

    private static <T> Iterator<T> wrapListIterator(Iterator<T> raw, String contextKey) {
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                try {
                    return raw.hasNext();
                } catch (HttpResponseException e) {
                    throw AzureExceptionMapper.map(e, contextKey);
                }
            }

            @Override
            public T next() {
                try {
                    return raw.next();
                } catch (HttpResponseException e) {
                    throw AzureExceptionMapper.map(e, contextKey);
                }
            }
        };
    }

    @Override
    public RangeReader openRangeReader(String key) {
        requireOpen();
        if (stat(key).isEmpty()) {
            throw new NotFoundException("Blob not found: " + location.resolve(key));
        }
        return new AzureBlobRangeReader(blobClient(key));
    }

    @Override
    public ReadHandle read(String key, ReadOptions options) {
        requireOpen();
        BlobClient client = blobClient(key);
        BlobClient effectiveClient = options.versionId().isPresent()
                ? client.getVersionClient(options.versionId().get())
                : client;
        BlobInputStreamOptions inOpts = new BlobInputStreamOptions();
        if (options.offset() > 0L || options.length().isPresent()) {
            BlobRange range = options.length().isPresent()
                    ? new BlobRange(options.offset(), options.length().getAsLong())
                    : new BlobRange(options.offset());
            inOpts.setRange(range);
        }
        if (options.ifMatchEtag().isPresent() || options.ifModifiedSince().isPresent()) {
            BlobRequestConditions cond = new BlobRequestConditions();
            options.ifMatchEtag().ifPresent(cond::setIfMatch);
            options.ifModifiedSince()
                    .ifPresent(i -> cond.setIfModifiedSince(OffsetDateTime.ofInstant(i, ZoneOffset.UTC)));
            inOpts.setRequestConditions(cond);
        }
        try {
            BlobInputStream stream = effectiveClient.openInputStream(inOpts);
            BlobProperties props = stream.getProperties();
            Instant lastModified = props.getLastModified() == null
                    ? Instant.now()
                    : props.getLastModified().toInstant();
            StorageEntry.File metadata = new StorageEntry.File(
                    key,
                    props.getBlobSize(),
                    lastModified,
                    Optional.ofNullable(props.getETag()),
                    Optional.ofNullable(props.getVersionId()),
                    Optional.ofNullable(props.getContentType()),
                    props.getMetadata() == null ? Map.of() : Map.copyOf(props.getMetadata()));
            return new ReadHandle(new StorageExceptionTranslatingInputStream(stream), metadata);
        } catch (BlobStorageException e) {
            throw AzureExceptionMapper.map(e, key);
        }
    }

    /**
     * Wraps an InputStream so unchecked StorageExceptions raised during read/skip/close are translated to IOException
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
        BlobClient client = blobClient(key);
        BlobParallelUploadOptions opts = new BlobParallelUploadOptions(BinaryData.fromBytes(data));
        applyWriteOptions(opts, options);
        try {
            client.uploadWithResponse(opts, null, null);
        } catch (BlobStorageException e) {
            throw mapPreconditionOrFail(e, key);
        }
        return stat(key).orElseThrow(() -> new StorageException("Wrote key but stat failed: " + key));
    }

    @Override
    public StorageEntry.File put(String key, Path source, WriteOptions options) {
        requireOpen();
        BlobClient client = blobClient(key);
        try (InputStream in = Files.newInputStream(source, StandardOpenOption.READ)) {
            BlobParallelUploadOptions opts = new BlobParallelUploadOptions(in);
            opts.setParallelTransferOptions(new ParallelTransferOptions().setBlockSizeLong((long) (4 * 1024 * 1024)));
            applyWriteOptions(opts, options);
            client.uploadWithResponse(opts, null, null);
        } catch (BlobStorageException e) {
            throw mapPreconditionOrFail(e, key);
        } catch (IOException e) {
            throw new StorageException("Failed to read source file: " + source, e);
        }
        return stat(key).orElseThrow(() -> new StorageException("Wrote key but stat failed: " + key));
    }

    @Override
    public OutputStream openOutputStream(String key, WriteOptions options) {
        requireOpen();
        Path tmp;
        OutputStream sink;
        try {
            tmp = Files.createTempFile("azure-storage-", ".part");
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
                        AzureBlobStorage.this.put(key, tmpFinal, options);
                    } catch (StorageException e) {
                        throw new IOException(e);
                    }
                } finally {
                    Files.deleteIfExists(tmpFinal);
                }
            }
        };
    }

    private static void applyWriteOptions(BlobParallelUploadOptions opts, WriteOptions options) {
        if (options.contentType().isPresent()) {
            BlobHttpHeaders contentType =
                    new BlobHttpHeaders().setContentType(options.contentType().orElseThrow());
            opts.setHeaders(contentType);
        }
        if (!options.userMetadata().isEmpty()) {
            opts.setMetadata(options.userMetadata());
        }
        if (options.ifNotExists() || options.ifMatchEtag().isPresent()) {
            BlobRequestConditions cond = new BlobRequestConditions();
            if (options.ifNotExists()) {
                cond.setIfNoneMatch("*");
            }
            options.ifMatchEtag().ifPresent(cond::setIfMatch);
            opts.setRequestConditions(cond);
        }
    }

    private StorageException mapPreconditionOrFail(BlobStorageException e, String key) {
        if (e.getStatusCode() == 409 || BlobErrorCode.BLOB_ALREADY_EXISTS.equals(e.getErrorCode())) {
            return new PreconditionFailedException("Key already exists: " + key, e);
        }
        return AzureExceptionMapper.map(e, key);
    }

    @Override
    public void delete(String key) {
        requireOpen();
        try {
            blobClient(key).deleteIfExists();
        } catch (BlobStorageException e) {
            throw AzureExceptionMapper.map(e, key);
        }
    }

    @Override
    public DeleteResult deleteAll(Collection<String> keys) {
        requireOpen();
        Set<String> deleted = new HashSet<>(keys);
        Set<String> didNotExist = new HashSet<>();
        Map<String, StorageException> failed = new HashMap<>();
        BlobBatchClient batch = batchClient();
        int limit = capabilities.bulkDeleteBatchLimit();
        List<String> blobUrls = new ArrayList<>(limit);
        for (String key : keys) {
            blobUrls.add(blobClient(key).getBlobUrl());
            if (blobUrls.size() == limit) {
                submitDeleteBatch(batch, blobUrls, deleted, didNotExist, failed);
                blobUrls.clear();
            }
        }
        if (!blobUrls.isEmpty()) {
            submitDeleteBatch(batch, blobUrls, deleted, didNotExist, failed);
        }
        return new DeleteResult(deleted, didNotExist, failed);
    }

    private BlobBatchClient batchClient() {
        BlobBatchClient bc = batchClient;
        if (bc == null) {
            BlobServiceClient blobServiceClient = handle.blobServiceClient();
            bc = new BlobBatchClientBuilder(blobServiceClient).buildClient();
            batchClient = bc;
        }
        return bc;
    }

    /**
     * Submit one delete batch and reconcile the result. The batch endpoint reports per-blob status codes; the SDK
     * aggregates non-success statuses into {@link BlobBatchStorageException}. We unwrap that exception to populate the
     * three partitions: keys that succeed land in {@code deleted}, 404 Not Found lands in {@code didNotExist}, and
     * every other per-blob error is recorded in {@code failed}. Other thrown exceptions (e.g. transport failures) fail
     * the entire chunk.
     */
    private void submitDeleteBatch(
            BlobBatchClient batch,
            List<String> blobUrls,
            Set<String> deleted,
            Set<String> didNotExist,
            Map<String, StorageException> failed) {
        try {
            // Force eager iteration so the batch is actually submitted and any error surfaces here.
            PagedIterable<Response<Void>> deleteBlobs = batch.deleteBlobs(blobUrls, DeleteSnapshotsOptionType.INCLUDE);
            deleteBlobs.iterator().forEachRemaining(r -> {});
        } catch (BlobBatchStorageException ex) {
            for (BlobStorageException one : ex.getBatchExceptions()) {
                String url = one.getResponse().getRequest().getUrl().toString();
                String key = keyForBlobUrl(url);
                if (one.getErrorCode() == BlobErrorCode.BLOB_NOT_FOUND || one.getStatusCode() == 404) {
                    deleted.remove(key);
                    didNotExist.add(key);
                    continue;
                }
                deleted.remove(key);
                failed.put(key, AzureExceptionMapper.map(one, key));
            }
        } catch (BlobStorageException ex) {
            // Whole-batch failure (e.g. 401/403/throttling): mark every key in this chunk as failed.
            for (String url : blobUrls) {
                String key = keyForBlobUrl(url);
                deleted.remove(key);
                failed.put(key, AzureExceptionMapper.map(ex, key));
            }
        }
    }

    /**
     * Recover a relative blob key from a request URL of the form {@code <endpoint>/<container>/<key...>?<query>}. The
     * extra parsing is needed because {@link BlobStorageException#getResponse()} only carries the failed request, not
     * the caller-supplied key.
     */
    private String keyForBlobUrl(String blobUrl) {
        int q = blobUrl.indexOf('?');
        String path = q < 0 ? blobUrl : blobUrl.substring(0, q);
        String marker = "/" + location.container() + "/";
        int idx = path.indexOf(marker);
        return idx < 0 ? path : path.substring(idx + marker.length());
    }

    @Override
    public StorageEntry.File copy(String srcKey, String dstKey, CopyOptions options) {
        requireOpen();
        return copyInternal(srcKey, this, dstKey, options);
    }

    @Override
    public StorageEntry.File copy(String srcKey, Storage dst, String dstKey, CopyOptions options) {
        requireOpen();
        if (!(dst instanceof AzureBlobStorage other)) {
            throw new UnsupportedCapabilityException("cross-backend copy from AzureBlobStorage to "
                    + dst.getClass().getSimpleName());
        }
        return copyInternal(srcKey, other, dstKey, options);
    }

    private StorageEntry.File copyInternal(String srcKey, AzureBlobStorage dst, String dstKey, CopyOptions options) {
        if (options.ifNotExistsAtDestination() && dst.stat(dstKey).isPresent()) {
            throw new PreconditionFailedException("Destination already exists: " + dstKey);
        }
        BlobClient src = blobClient(srcKey);
        BlobClient target = dst.blobClient(dstKey);
        try {
            target.copyFromUrl(src.getBlobUrl());
        } catch (BlobStorageException e) {
            throw AzureExceptionMapper.map(e, srcKey);
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

    // --- presign (SAS)

    @Override
    public URI presignGet(String key, Duration ttl) {
        requireOpen();
        BlobClient client = blobClient(key);
        OffsetDateTime expiry = OffsetDateTime.now(ZoneOffset.UTC).plus(ttl);
        BlobSasPermission perm = new BlobSasPermission().setReadPermission(true);
        BlobServiceSasSignatureValues sasValues = new BlobServiceSasSignatureValues(expiry, perm);
        try {
            String sas = client.generateSas(sasValues);
            return URI.create(client.getBlobUrl() + "?" + sas);
        } catch (HttpResponseException e) {
            throw AzureExceptionMapper.map(e, key);
        }
    }

    @Override
    public URI presignPut(String key, Duration ttl, PresignWriteOptions options) {
        requireOpen();
        BlobClient client = blobClient(key);
        OffsetDateTime expiry = OffsetDateTime.now(ZoneOffset.UTC).plus(ttl);
        BlobSasPermission perm =
                new BlobSasPermission().setWritePermission(true).setCreatePermission(true);
        BlobServiceSasSignatureValues sasValues = new BlobServiceSasSignatureValues(expiry, perm);
        try {
            String sas = client.generateSas(sasValues);
            return URI.create(client.getBlobUrl() + "?" + sas);
        } catch (HttpResponseException e) {
            throw AzureExceptionMapper.map(e, key);
        }
    }
}
