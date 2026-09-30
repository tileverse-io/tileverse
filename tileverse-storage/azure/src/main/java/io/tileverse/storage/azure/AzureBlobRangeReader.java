/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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

import static java.util.Objects.requireNonNull;

import com.azure.core.http.HttpHeaderName;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.models.BlobDownloadAsyncResponse;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.DownloadRetryOptions;
import com.azure.storage.blob.specialized.BlobAsyncClientBase;
import com.azure.storage.blob.specialized.SpecializedBlobClientBuilder;
import io.tileverse.storage.AbstractRangeReader;
import io.tileverse.storage.ContentRange;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.StorageException;
import io.tileverse.storage.adapters.ByteBufferOutputStream;
import io.tileverse.storage.adapters.ByteBufferSinkException;
import io.tileverse.storage.batch.BatchSettings;
import io.tileverse.storage.batch.CoalescingPolicy;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * A RangeReader implementation that reads from an Azure Blob Storage container.
 *
 * <p>This class enables reading data stored in Azure Blob Storage using the Azure Storage Blob client library for Java.
 *
 * <p>Batched reads merge nearby ranges and run fetches concurrently on the shared batch executor under the
 * {@link BatchSettings} of the Storage the reader was opened from; worst-case amplification is the requested bytes plus
 * the merged gaps.
 *
 * <p>Range bodies stream straight into the caller's buffer, heap or direct, through the SDK's download stream; no read
 * holds a full-size heap copy of the response.
 */
@Slf4j
class AzureBlobRangeReader extends AbstractRangeReader implements RangeReader {

    private final BlobClient blobClient;
    private final BlobAsyncClientBase asyncClient;
    private final BatchSettings batchSettings;
    private final AtomicReference<OptionalLong> contentLength = new AtomicReference<>();

    /**
     * Creates a new AzureBlobRangeReader for the specified blob, batching under the
     * {@link BatchSettings#objectStoreDefaults() object-store defaults}.
     *
     * <p>Construction performs no I/O. A missing blob is reported by the first {@link #readRange(long, int)} or
     * {@link #size()} call instead of at construction time.
     *
     * @param blobClient The Azure Blob client to read from
     */
    AzureBlobRangeReader(BlobClient blobClient) {
        this(blobClient, BatchSettings.objectStoreDefaults());
    }

    /**
     * Creates a new AzureBlobRangeReader with the batch settings of the Storage it belongs to.
     *
     * @param blobClient The Azure Blob client to read from
     * @param batchSettings the merge policy and in-flight bound for batched reads
     */
    AzureBlobRangeReader(BlobClient blobClient, BatchSettings batchSettings) {
        this(blobClient, asyncClientFor(blobClient), batchSettings);
    }

    /**
     * Creates a new AzureBlobRangeReader sending its requests through {@code asyncClient}.
     *
     * @param blobClient The Azure Blob client for the blob URL
     * @param asyncClient the asynchronous client for the same blob
     * @param batchSettings the merge policy and in-flight bound for batched reads
     */
    AzureBlobRangeReader(BlobClient blobClient, BlobAsyncClientBase asyncClient, BatchSettings batchSettings) {
        this.blobClient = requireNonNull(blobClient, "BlobClient cannot be null");
        this.asyncClient = requireNonNull(asyncClient, "asynchronous client cannot be null");
        this.batchSettings = requireNonNull(batchSettings, "BatchSettings cannot be null");
    }

    /**
     * Blocking on a call of the asynchronous client cancels it on interrupt and keeps the interrupt status; the
     * synchronous client leaves a download running and clears the status of an interrupted properties request. The
     * builder reuses the pipeline of {@code blobClient} and sends no request.
     */
    private static BlobAsyncClientBase asyncClientFor(BlobClient blobClient) {
        requireNonNull(blobClient, "BlobClient cannot be null");
        return new SpecializedBlobClientBuilder().blobClient(blobClient).buildBlockBlobAsyncClient();
    }

    BatchSettings batchSettings() {
        return batchSettings;
    }

    /**
     * Streams the range body straight into {@code target} through a {@link ByteBufferOutputStream}, keeping the
     * per-download retry options. A sink failure, a body longer than requested or a target refusing the write, is
     * reported as a {@link StorageException} with the sink's message.
     *
     * <p>An interrupt cancels the download, and nothing resumes it. The sink is closed before this method returns or
     * throws: it rejects a chunk already in flight instead of changing a target already handed back to the caller.
     *
     * <p>The download runs without a deadline on the whole range, because a large range on a slow link outlasts any
     * fixed one. Each stalled download attempt ends on the HTTP client's read timeout between chunks, 60 seconds by
     * default, unless a caller-supplied client disables it.
     */
    @Override
    protected int readRangeNoFlip(long offset, int actualLength, ByteBuffer target) {
        try (ByteBufferOutputStream sink = new ByteBufferOutputStream(target, actualLength)) {
            final long start = System.nanoTime();

            BlobDownloadAsyncResponse response =
                    download(offset, actualLength, sink).block();

            if (log.isDebugEnabled()) {
                long end = System.nanoTime();
                long millis = Duration.ofNanos(end - start).toMillis();
                log.debug("range:[{} +{}], time: {}ms]", offset, actualLength, millis);
            }

            if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
                throw new StorageException("Failed to download blob range, status code: " + response.getStatusCode());
            }

            if (contentLength.get() == null) {
                String contentRange = response.getHeaders().getValue(HttpHeaderName.CONTENT_RANGE);
                ContentRange.totalOf(contentRange)
                        .ifPresent(total -> contentLength.compareAndSet(null, OptionalLong.of(total)));
            }
            return sink.bytesWritten();
        } catch (BlobStorageException e) {
            throw AzureExceptionMapper.map(e, blobClient.getBlobUrl());
        } catch (StorageException e) {
            throw e;
        } catch (Exception e) {
            Optional<ByteBufferSinkException> sinkFailure = sinkFailure(e);
            if (sinkFailure.isPresent()) {
                throw new StorageException(sinkFailure.get().getMessage(), e);
            }
            throw new StorageException("Failed to read range from blob: " + e.getMessage(), e);
        }
    }

    private Mono<BlobDownloadAsyncResponse> download(long offset, int length, ByteBufferOutputStream sink) {
        BlobRange range = new BlobRange(offset, (long) length);
        DownloadRetryOptions options = new DownloadRetryOptions().setMaxRetryRequests(3);
        Mono<BlobDownloadAsyncResponse> pending =
                asyncClient.downloadStreamWithResponse(range, options, new BlobRequestConditions(), false);
        return pending.flatMap(response -> writeBody(response, sink));
    }

    private static Mono<BlobDownloadAsyncResponse> writeBody(
            BlobDownloadAsyncResponse response, ByteBufferOutputStream sink) {
        Flux<ByteBuffer> body = response.getValue();
        return body.doOnNext(chunk -> writeChunk(chunk, sink)).then(Mono.just(response));
    }

    private static void writeChunk(ByteBuffer chunk, ByteBufferOutputStream sink) {
        try {
            if (chunk.hasArray()) {
                sink.write(chunk.array(), chunk.arrayOffset() + chunk.position(), chunk.remaining());
            } else {
                byte[] copy = new byte[chunk.remaining()];
                chunk.duplicate().get(copy);
                sink.write(copy, 0, copy.length);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** The download pipeline delivers the sink's failure wrapped in an unchecked or reactive exception. */
    private static Optional<ByteBufferSinkException> sinkFailure(Throwable failure) {
        Throwable cause = failure;
        while (cause != null) {
            if (cause instanceof ByteBufferSinkException sinkFailure) {
                return Optional.of(sinkFailure);
            }
            cause = cause.getCause();
        }
        return Optional.empty();
    }

    @Override
    protected CoalescingPolicy coalescingPolicy() {
        return batchSettings.coalescingPolicy();
    }

    @Override
    protected int maxConcurrentFetches() {
        return batchSettings.concurrencyCap();
    }

    @Override
    public OptionalLong size() {
        OptionalLong known = contentLength.get();
        if (known == null) {
            known = contentLength.updateAndGet(current -> current != null ? current : fetchSize());
        }
        return known;
    }

    private OptionalLong fetchSize() {
        BlobProperties properties = fetchProperties().orElseThrow(this::noPropertiesReturned);
        return OptionalLong.of(properties.getBlobSize());
    }

    private Optional<BlobProperties> fetchProperties() {
        try {
            return asyncClient.getProperties().blockOptional();
        } catch (BlobStorageException e) {
            throw AzureExceptionMapper.map(e, blobClient.getBlobUrl());
        } catch (RuntimeException e) {
            throw new StorageException("Failed to get blob size: " + e.getMessage(), e);
        }
    }

    private StorageException noPropertiesReturned() {
        return new StorageException("Failed to get blob size: no properties returned for " + blobClient.getBlobUrl());
    }

    @Override
    public String getSourceIdentifier() {
        return blobClient.getBlobUrl();
    }

    @Override
    public void close() {
        // Azure BlobClient doesn't require explicit closing
    }
}
