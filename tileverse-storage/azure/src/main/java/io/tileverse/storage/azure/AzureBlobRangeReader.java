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
import com.azure.core.http.rest.Response;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.DownloadRetryOptions;
import io.tileverse.storage.AbstractRangeReader;
import io.tileverse.storage.ContentRange;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.StorageException;
import io.tileverse.storage.batch.CoalescingPolicy;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

/**
 * A RangeReader implementation that reads from an Azure Blob Storage container.
 *
 * <p>This class enables reading data stored in Azure Blob Storage using the Azure Storage Blob client library for Java.
 *
 * <p>Batched reads merge nearby ranges under the object-store coalescing policy and run up to 8 fetches concurrently on
 * the shared batch executor; worst-case amplification is the requested bytes plus the merged gaps.
 */
@Slf4j
class AzureBlobRangeReader extends AbstractRangeReader implements RangeReader {

    /** Fetch parallelism for batched reads on the shared batch executor. */
    private static final int MAX_CONCURRENT_FETCHES = 8;

    private final BlobClient blobClient;
    private final AtomicReference<OptionalLong> contentLength = new AtomicReference<>();

    /**
     * Creates a new AzureBlobRangeReader for the specified blob.
     *
     * <p>Construction performs no I/O. A missing blob is reported by the first {@link #readRange(long, int)} or
     * {@link #size()} call instead of at construction time.
     *
     * @param blobClient The Azure Blob client to read from
     */
    AzureBlobRangeReader(BlobClient blobClient) {
        this.blobClient = requireNonNull(blobClient, "BlobClient cannot be null");
    }

    @Override
    protected int readRangeNoFlip(long offset, int actualLength, ByteBuffer target) {

        try {
            final long start = System.nanoTime();
            // Download the specified range
            BlobRange range = new BlobRange(offset, (long) actualLength);
            DownloadRetryOptions options = new DownloadRetryOptions().setMaxRetryRequests(3);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(actualLength);

            // API requires Duration and Context parameters
            Response<Void> response = blobClient.downloadStreamWithResponse(
                    outputStream,
                    range,
                    options,
                    new BlobRequestConditions(),
                    false,
                    Duration.ofSeconds(60), // Timeout
                    com.azure.core.util.Context.NONE); // Context

            if (log.isDebugEnabled()) {
                long end = System.nanoTime();
                long millis = Duration.ofNanos(end - start).toMillis();
                log.debug("range:[{} +{}], time: {}ms]", offset, actualLength, millis);
            }

            // Verify the response is successful
            if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
                throw new StorageException("Failed to download blob range, status code: " + response.getStatusCode());
            }

            if (contentLength.get() == null) {
                String contentRange = response.getHeaders().getValue(HttpHeaderName.CONTENT_RANGE);
                ContentRange.totalOf(contentRange)
                        .ifPresent(total -> contentLength.compareAndSet(null, OptionalLong.of(total)));
            }

            // Copy the bytes directly into the target buffer
            byte[] data = outputStream.toByteArray();
            target.put(data);
            // Return the number of bytes read
            return data.length;
        } catch (BlobStorageException e) {
            throw AzureExceptionMapper.map(e, blobClient.getBlobUrl());
        } catch (StorageException e) {
            throw e;
        } catch (Exception e) {
            throw new StorageException("Failed to read range from blob: " + e.getMessage(), e);
        }
    }

    @Override
    protected CoalescingPolicy coalescingPolicy() {
        return CoalescingPolicy.objectStoreDefaults();
    }

    @Override
    protected int maxConcurrentFetches() {
        return MAX_CONCURRENT_FETCHES;
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
        try {
            return OptionalLong.of(blobClient.getProperties().getBlobSize());
        } catch (BlobStorageException e) {
            throw AzureExceptionMapper.map(e, blobClient.getBlobUrl());
        } catch (RuntimeException e) {
            throw new StorageException("Failed to get blob size: " + e.getMessage(), e);
        }
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
