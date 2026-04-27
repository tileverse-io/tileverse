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

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.models.DataLakeFileOpenInputStreamResult;
import com.azure.storage.file.datalake.models.DataLakeRequestConditions;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.FileRange;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import com.azure.storage.file.datalake.models.PathHttpHeaders;
import com.azure.storage.file.datalake.models.PathItem;
import com.azure.storage.file.datalake.models.PathProperties;
import com.azure.storage.file.datalake.options.DataLakeFileInputStreamOptions;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.jspecify.annotations.Nullable;

/**
 * Azure Data Lake Storage Gen2 implementation of {@link Storage} (HNS, real directories).
 *
 * <p>Azure DataLake Gen2 (HNS) does not expose per-object version IDs; {@link StorageEntry.File#versionId()} is always
 * empty.
 */
final class AzureDataLakeStorage implements Storage {

    private static final StorageCapabilities capabilities = StorageCapabilities.builder()
            .rangeReads(true)
            .streamingReads(true)
            .stat(true)
            .userMetadata(true)
            .list(true)
            .hierarchicalList(true)
            .realDirectories(true)
            .writes(true)
            .multipartUpload(true)
            .multipartThresholdBytes(8L * 1024 * 1024)
            .conditionalWrite(true)
            .bulkDelete(true)
            .bulkDeleteBatchLimit(Integer.MAX_VALUE)
            .deleteReportsExistence(true)
            .serverSideCopy(true)
            .atomicMove(true)
            .presignedUrls(false)
            .versioning(false)
            .strongReadAfterWrite(true)
            .build();

    private final URI baseUri;
    private final AzureBlobLocation location;
    private final AzureClientHandle handle;
    private final DataLakeFileSystemClient fileSystem;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    AzureDataLakeStorage(URI baseUri, AzureBlobLocation location, AzureClientCache.Lease lease) {
        this(baseUri, location, new LeasedAzureHandle(lease));
    }

    AzureDataLakeStorage(URI baseUri, AzureBlobLocation location, AzureClientHandle handle) {
        this.baseUri = baseUri;
        this.location = location;
        this.handle = handle;
        this.fileSystem = handle.dataLakeServiceClient()
                .orElseThrow(() -> new IllegalArgumentException(
                        "AzureDataLakeStorage requires a DataLakeServiceClient; supplied AzureClientHandle has none"))
                .getFileSystemClient(location.container());
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
            throw new IllegalStateException("AzureDataLakeStorage is closed");
        }
    }

    private DataLakeFileClient fileClient(String key) {
        return fileSystem.getFileClient(location.resolve(key));
    }

    @Override
    public Optional<StorageEntry.File> stat(String key) {
        requireOpen();
        DataLakeFileClient fc = fileClient(key);
        try {
            PathProperties props = fc.getProperties();
            return Optional.of(new StorageEntry.File(
                    key,
                    props.getFileSize(),
                    props.getLastModified() == null
                            ? Instant.EPOCH
                            : props.getLastModified().toInstant(),
                    Optional.ofNullable(props.getETag()),
                    Optional.empty(),
                    Optional.ofNullable(props.getContentType()),
                    props.getMetadata() == null ? Map.of() : Map.copyOf(props.getMetadata())));
        } catch (DataLakeStorageException e) {
            if (e.getStatusCode() == 404) return Optional.empty();
            throw AzureExceptionMapper.map(e, key);
        }
    }

    @Override
    public Stream<StorageEntry> list(String pattern, ListOptions options) {
        requireOpen();
        StoragePattern parsed = StoragePattern.parse(pattern);
        String fullPrefix = location.resolve(parsed.prefix());
        Predicate<String> matcher = parsed.matcher().orElse(k -> true);

        ListPathsOptions listOpts = new ListPathsOptions()
                .setPath(fullPrefix.isEmpty() ? null : fullPrefix.replaceAll("/$", ""))
                .setRecursive(parsed.walkDescendants());
        options.pageSize().ifPresent(listOpts::setMaxResults);

        Iterable<PathItem> iter;
        Iterator<PathItem> rawItems;
        try {
            iter = fileSystem.listPaths(listOpts, null);
            rawItems = iter.iterator();
            rawItems.hasNext(); // force first-page fetch to surface auth/region errors here
        } catch (DataLakeStorageException e) {
            throw AzureExceptionMapper.map(e, fullPrefix);
        }

        Iterator<PathItem> wrapped = wrapDataLakeListIterator(rawItems, fullPrefix);
        Stream<PathItem> itemStream =
                StreamSupport.stream(Spliterators.spliteratorUnknownSize(wrapped, Spliterator.ORDERED), false);
        return itemStream.map(this::toEntry).filter(entry -> matcher.test(entry.key()));
    }

    private StorageEntry toEntry(PathItem item) {
        String relKey = location.relativize(item.getName());
        if (Boolean.TRUE.equals(item.isDirectory())) {
            return new StorageEntry.Directory(
                    relKey + "/", Optional.ofNullable(item.getLastModified()).map(OffsetDateTime::toInstant));
        }
        Instant lastModified = item.getLastModified() == null
                ? Instant.EPOCH
                : item.getLastModified().toInstant();
        return new StorageEntry.File(
                relKey,
                item.getContentLength(),
                lastModified,
                Optional.ofNullable(item.getETag()),
                Optional.empty(),
                Optional.empty(),
                Map.of());
    }

    private static <T> Iterator<T> wrapDataLakeListIterator(Iterator<T> raw, String contextKey) {
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                try {
                    return raw.hasNext();
                } catch (DataLakeStorageException e) {
                    throw AzureExceptionMapper.map(e, contextKey);
                }
            }

            @Override
            public T next() {
                try {
                    return raw.next();
                } catch (DataLakeStorageException e) {
                    throw AzureExceptionMapper.map(e, contextKey);
                }
            }
        };
    }

    @Override
    public RangeReader openRangeReader(String key) {
        requireOpen();

        // Reuse AzureBlobRangeReader by going through the parallel blob endpoint.
        // The underlying data plane is shared on HNS-enabled accounts.
        BlobServiceClient blobServiceClient = handle.blobServiceClient();
        BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient(location.container());
        String blobName = location.resolve(key);
        BlobClient blobClient = blobContainerClient.getBlobClient(blobName);
        return new AzureBlobRangeReader(blobClient);
    }

    @Override
    public ReadHandle read(String key, ReadOptions options) {
        requireOpen();
        DataLakeFileClient fc = fileClient(key);
        DataLakeFileInputStreamOptions inOpts = new DataLakeFileInputStreamOptions();
        final long offset = options.offset();
        if (offset > 0L || options.length().isPresent()) {
            @Nullable
            Long count = !options.length().isPresent() ? null : options.length().getAsLong();
            FileRange fr = new FileRange(offset, count);
            inOpts.setRange(fr);
        }
        if (options.ifMatchEtag().isPresent() || options.ifModifiedSince().isPresent()) {
            DataLakeRequestConditions cond = new DataLakeRequestConditions();
            options.ifMatchEtag().ifPresent(cond::setIfMatch);
            options.ifModifiedSince()
                    .ifPresent(i -> cond.setIfModifiedSince(OffsetDateTime.ofInstant(i, ZoneOffset.UTC)));
            inOpts.setRequestConditions(cond);
        }
        try {
            DataLakeFileOpenInputStreamResult result = fc.openInputStream(inOpts);
            PathProperties props = result.getProperties();
            Instant lastModified = props.getLastModified() == null
                    ? Instant.now()
                    : props.getLastModified().toInstant();
            StorageEntry.File metadata = new StorageEntry.File(
                    key,
                    props.getFileSize(),
                    lastModified,
                    Optional.ofNullable(props.getETag()),
                    Optional.empty(),
                    Optional.ofNullable(props.getContentType()),
                    props.getMetadata() == null ? Map.of() : Map.copyOf(props.getMetadata()));
            return new ReadHandle(new StorageExceptionTranslatingInputStream(result.getInputStream()), metadata);
        } catch (DataLakeStorageException e) {
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
        DataLakeFileClient fc = fileClient(key);
        if (options.ifNotExists() && fc.exists().booleanValue()) {
            throw new PreconditionFailedException("Key already exists: " + key);
        }
        try {
            fc.upload(com.azure.core.util.BinaryData.fromBytes(data), true);
            applyPostWrite(fc, options);
        } catch (DataLakeStorageException e) {
            throw AzureExceptionMapper.map(e, key);
        }
        return stat(key).orElseThrow(() -> new StorageException("Wrote key but stat failed: " + key));
    }

    @Override
    public StorageEntry.File put(String key, Path source, WriteOptions options) {
        requireOpen();
        DataLakeFileClient fc = fileClient(key);
        if (options.ifNotExists() && fc.exists().booleanValue()) {
            throw new PreconditionFailedException("Key already exists: " + key);
        }
        try {
            fc.uploadFromFile(source.toString(), true);
            applyPostWrite(fc, options);
        } catch (DataLakeStorageException e) {
            throw AzureExceptionMapper.map(e, key);
        }
        return stat(key).orElseThrow(() -> new StorageException("Wrote key but stat failed: " + key));
    }

    private static void applyPostWrite(DataLakeFileClient fc, WriteOptions options) {
        if (options.contentType().isPresent()) {
            String contentType = options.contentType().orElseThrow();
            fc.setHttpHeaders(new PathHttpHeaders().setContentType(contentType));
        }
        if (!options.userMetadata().isEmpty()) {
            fc.setMetadata(options.userMetadata());
        }
    }

    @Override
    public OutputStream openOutputStream(String key, WriteOptions options) {
        requireOpen();
        Path tmp;
        OutputStream sink;
        try {
            tmp = Files.createTempFile("azure-datalake-", ".part");
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
                        AzureDataLakeStorage.this.put(key, tmpFinal, options);
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
        try {
            fileClient(key).deleteIfExists();
        } catch (DataLakeStorageException e) {
            throw AzureExceptionMapper.map(e, key);
        }
    }

    @Override
    public DeleteResult deleteAll(Collection<String> keys) {
        requireOpen();
        Set<String> deleted = new HashSet<>();
        Set<String> didNotExist = new HashSet<>();
        Map<String, StorageException> failed = new HashMap<>();
        for (String key : keys) {
            try {
                boolean existed = Boolean.TRUE.equals(fileClient(key).deleteIfExists());
                if (existed) {
                    deleted.add(key);
                } else {
                    didNotExist.add(key);
                }
            } catch (DataLakeStorageException e) {
                failed.put(key, AzureExceptionMapper.map(e, key));
            }
        }
        return new DeleteResult(deleted, didNotExist, failed);
    }

    @Override
    public StorageEntry.File copy(String srcKey, String dstKey, CopyOptions options) {
        requireOpen();
        // DataLake SDK does not expose a server-side copy primitive; fall through to the
        // parallel blob endpoint which supports CopyFromUrl on the same data plane.
        if (options.ifNotExistsAtDestination() && stat(dstKey).isPresent()) {
            throw new PreconditionFailedException("Destination already exists: " + dstKey);
        }
        BlobContainerClient blobContainer = handle.blobServiceClient().getBlobContainerClient(location.container());
        BlobClient srcBlob = blobContainer.getBlobClient(location.resolve(srcKey));
        BlobClient dstBlob = blobContainer.getBlobClient(location.resolve(dstKey));
        try {
            dstBlob.copyFromUrl(srcBlob.getBlobUrl());
        } catch (BlobStorageException e) {
            throw AzureExceptionMapper.map(e, srcKey);
        }
        return stat(dstKey).orElseThrow(() -> new StorageException("Copy failed for: " + dstKey));
    }

    @Override
    public StorageEntry.File copy(String srcKey, Storage dst, String dstKey, CopyOptions options) {
        requireOpen();
        if (!(dst instanceof AzureDataLakeStorage)) {
            throw new UnsupportedCapabilityException("cross-backend copy from AzureDataLakeStorage to "
                    + dst.getClass().getSimpleName());
        }
        return copy(srcKey, dstKey, options);
    }

    @Override
    public StorageEntry.File move(String srcKey, String dstKey, CopyOptions options) {
        requireOpen();
        if (options.ifNotExistsAtDestination() && stat(dstKey).isPresent()) {
            throw new PreconditionFailedException("Destination already exists: " + dstKey);
        }
        DataLakeFileClient fc = fileClient(srcKey);
        try {
            fc.rename(null, location.resolve(dstKey));
        } catch (DataLakeStorageException e) {
            if (e.getStatusCode() == 404) throw new NotFoundException("Source not found: " + srcKey, e);
            throw AzureExceptionMapper.map(e, srcKey);
        }
        return stat(dstKey).orElseThrow(() -> new StorageException("Move failed for: " + dstKey));
    }

    @Override
    public URI presignGet(String key, Duration ttl) {
        // Use AzureBlobStorage on the parallel blob endpoint for SAS-based presign.
        throw new UnsupportedCapabilityException("presignGet on DataLake (use AzureBlobStorage on the same account)");
    }

    @Override
    public URI presignPut(String key, Duration ttl, PresignWriteOptions options) {
        throw new UnsupportedCapabilityException("presignPut on DataLake (use AzureBlobStorage on the same account)");
    }
}
