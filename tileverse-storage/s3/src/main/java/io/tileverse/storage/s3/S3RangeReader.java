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
package io.tileverse.storage.s3;

import io.tileverse.io.ByteBufferPool;
import io.tileverse.io.ByteBufferPool.PooledByteBuffer;
import io.tileverse.storage.AbstractRangeReader;
import io.tileverse.storage.ContentRange;
import io.tileverse.storage.NotFoundException;
import io.tileverse.storage.RangeNotSatisfiableException;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.RangeRequest;
import io.tileverse.storage.StorageException;
import io.tileverse.storage.batch.BatchPlanner;
import io.tileverse.storage.batch.CoalescingPolicy;
import io.tileverse.storage.batch.PlannedFetch;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.Nullable;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.RequestPayer;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * A {@link RangeReader} implementation that reads data from an AWS S3-compatible object storage service.
 *
 * <p>This class reads S3 objects through the {@link S3Client} of the AWS SDK for Java v2 and serves both standard AWS
 * S3 and self-hosted S3-compatible services like MinIO.
 *
 * <h2>Construction and Configuration</h2>
 *
 * Readers are opened by {@code S3Storage#openRangeReader(String)} and borrow the SDK clients of that storage; opening a
 * reader issues no request. Credentials, region, endpoint and path-style addressing are therefore settled before a
 * reader exists: {@link S3StorageProvider} resolves them from the {@code storage.s3.*} parameters and the base URI, and
 * {@link S3ClientCache} builds one client set per distinct combination, shared by every storage that requests it. A
 * caller that already holds a configured {@link S3Client} hands it to {@link S3StorageProvider#open(java.net.URI,
 * S3Client)} instead.
 *
 * <h2>S3-Compatible Endpoints</h2>
 *
 * Readers over a custom endpoint (MinIO, Ceph, LocalStack, an on-premise gateway) come from an {@code S3Storage} whose
 * SDK client was built with that endpoint; most self-hosted services also need <b>path-style access</b>. The endpoint
 * is part of {@link #getSourceIdentifier()}: the same bucket and key on two endpoints are two different objects, and
 * the identifier partitions the shared range cache. Readers over the default AWS endpoints identify as canonical
 * {@code s3://bucket/key} URIs, which are unambiguous because bucket names are global.
 *
 * <h2>Batched and Streaming Reads</h2>
 *
 * {@code readRanges} merges nearby ranges under the object-store coalescing policy and, when the CRT
 * {@link S3AsyncClient} is present, fetches the planned ranges in parallel with one async {@code getObject} each.
 * Without the async client the {@link AbstractRangeReader} template runs the same plan on the shared batch executor.
 *
 * <p>Range bodies stream straight into their destination, heap or direct, on every path: single reads through a
 * {@link software.amazon.awssdk.core.sync.ResponseTransformer} that keeps the SDK's body-read retries, batched fetches
 * through an {@link AsyncResponseTransformer} writing chunks as they arrive. No read holds a full-size heap copy of the
 * response; transient heap per read is bounded by the SDK's chunk size, plus one pooled scratch buffer for a merged
 * fetch that scatters to several requests.
 */
final class S3RangeReader extends AbstractRangeReader implements RangeReader {

    /** Fetch parallelism when batched reads fall back to the AbstractRangeReader template (no async client). */
    private static final int MAX_CONCURRENT_FETCHES = 8;

    private final S3Client s3Client;

    @Nullable
    private final S3AsyncClient asyncClient;

    private final S3Reference s3Location;
    private final boolean requesterPays;

    private final AtomicReference<OptionalLong> contentLength = new AtomicReference<>();

    /**
     * Creates a reader without an async client; batched reads run through the {@link AbstractRangeReader} template.
     *
     * @param s3Client The S3 client to use
     * @param s3Location The S3 reference (bucket + key)
     * @param requesterPays when {@code true}, every request adds {@code x-amz-request-payer: requester}
     */
    S3RangeReader(S3Client s3Client, S3Reference s3Location, boolean requesterPays) {
        this(s3Client, null, s3Location, requesterPays);
    }

    /**
     * Creates a new S3RangeReader for the specified S3 object.
     *
     * <p>Construction performs no I/O. A missing object is reported by the first {@link #readRange(long, int)} or
     * {@link #size()} call instead of at construction time.
     *
     * @param s3Client The S3 client to use for single reads and metadata
     * @param asyncClient the CRT async client for parallel batched reads, or null to batch through the shared executor
     * @param s3Location The S3 reference (bucket + key)
     * @param requesterPays when {@code true}, every request adds {@code x-amz-request-payer: requester}
     */
    S3RangeReader(
            S3Client s3Client, @Nullable S3AsyncClient asyncClient, S3Reference s3Location, boolean requesterPays) {
        this.s3Client = Objects.requireNonNull(s3Client, "S3Client cannot be null");
        this.asyncClient = asyncClient;
        this.s3Location = Objects.requireNonNull(s3Location, "S3Location cannot be null");
        this.requesterPays = requesterPays;
    }

    private GetObjectRequest buildGetRequest(long offset, int length) {
        long rangeEnd = offset + length - 1;
        GetObjectRequest.Builder request = GetObjectRequest.builder()
                .bucket(s3Location.bucket())
                .key(s3Location.key())
                .range("bytes=" + offset + "-" + rangeEnd);
        if (requesterPays) {
            request.requestPayer(RequestPayer.REQUESTER);
        }
        return request.build();
    }

    /**
     * Streams the range body straight into {@code target} through {@link ByteBufferResponseTransformer}, inside the
     * SDK's retry loop. A body longer than requested is a {@link StorageException} raised by the transformer and
     * rethrown here whether or not the SDK wrapped it.
     */
    @Override
    protected int readRangeNoFlip(final long offset, final int actualLength, ByteBuffer target) {
        try {
            ByteBufferResponseTransformer body = new ByteBufferResponseTransformer(target, actualLength);
            GetObjectResponse response = s3Client.getObject(buildGetRequest(offset, actualLength), body);
            captureSizeFrom(response);
            return body.bytesWritten();
        } catch (NoSuchKeyException e) {
            throw new NotFoundException("S3 object does not exist: s3://" + s3Location, e);
        } catch (S3Exception e) {
            throw S3ExceptionMapper.map(e, s3Location.key());
        } catch (SdkException e) {
            if (e.getCause() instanceof StorageException raisedWhileStreaming) {
                throw raisedWhileStreaming;
            }
            throw new StorageException("Failed to read range from S3: " + e.getMessage(), e);
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

    /**
     * Reads a batch with one parallel CRT {@code getObject} per planned fetch when the async client is present; without
     * it, the batched-read template runs the same plan on the shared executor through the sync client.
     *
     * <p>A fetch answered 416 (entirely past EOF) reports 0 bytes for its entries, exactly like {@code readRange}; any
     * other failure aborts the whole call after the in-flight fetches complete. Worst-case amplification: the requested
     * bytes plus the gaps the object-store policy merges, at most {@link CoalescingPolicy#maxFetchBytes()} per fetch.
     *
     * @param requests the ranges to read and the buffers they land in
     * @return the number of bytes read per request, in request order
     */
    @Override
    public int[] readRanges(List<RangeRequest> requests) {
        if (asyncClient == null) {
            return super.readRanges(requests);
        }
        RangeRequest.validate(requests);
        if (requests.isEmpty()) {
            return new int[0];
        }
        List<PlannedFetch> fetches = BatchPlanner.plan(requests, coalescingPolicy());
        int[] counts = new int[requests.size()];
        if (fetches.isEmpty()) {
            return counts;
        }
        CompletableFuture<?>[] outcomes = new CompletableFuture<?>[fetches.size()];
        for (int i = 0; i < outcomes.length; i++) {
            outcomes[i] = fetchAsync(fetches.get(i), requests, counts);
        }
        try {
            CompletableFuture.allOf(outcomes).join();
        } catch (CompletionException failure) {
            throw unwrapBatchFailure(failure);
        }
        return counts;
    }

    /**
     * Issues one async GET for a fetch, streaming the body into its destination as chunks arrive: the caller's target
     * for a direct fetch, pooled heap scratch scattered to its requests for a merged one. A 416 leaves the fetch's
     * entries at 0 and completes normally; every other failure completes the future exceptionally with the mapped
     * storage exception.
     */
    private CompletableFuture<Void> fetchAsync(PlannedFetch fetch, List<RangeRequest> requests, int[] counts) {
        if (fetch.isDirect()) {
            return fetchDirect(fetch, requests, counts);
        }
        return fetchAndScatter(fetch, requests, counts);
    }

    private CompletableFuture<Void> fetchDirect(PlannedFetch fetch, List<RangeRequest> requests, int[] counts) {
        int requestIndex = fetch.slices().get(0).requestIndex();
        ByteBuffer target = requests.get(requestIndex).target();
        int start = target.position();
        return streamFetch(fetch, target).handle((streamed, failure) -> {
            if (failure != null) {
                return failedFetch(failure);
            }
            captureSizeFrom(streamed.response());
            target.position(start + streamed.bytesWritten());
            counts[requestIndex] = streamed.bytesWritten();
            return null;
        });
    }

    private CompletableFuture<Void> fetchAndScatter(PlannedFetch fetch, List<RangeRequest> requests, int[] counts) {
        PooledByteBuffer pooled = ByteBufferPool.heapBuffer(fetch.range().length());
        ByteBuffer scratch = pooled.buffer();
        CompletableFuture<ByteBufferAsyncResponseTransformer.Result> attempt;
        try {
            attempt = streamFetch(fetch, scratch);
        } catch (RuntimeException synchronousFailure) {
            pooled.close();
            throw synchronousFailure;
        }
        return attempt.handle((streamed, failure) -> {
            try (pooled) {
                if (failure != null) {
                    return failedFetch(failure);
                }
                captureSizeFrom(streamed.response());
                fetch.scatter(scratch, streamed.bytesWritten(), requests, counts);
                return null;
            }
        });
    }

    private CompletableFuture<ByteBufferAsyncResponseTransformer.Result> streamFetch(
            PlannedFetch fetch, ByteBuffer destination) {
        GetObjectRequest request =
                buildGetRequest(fetch.range().offset(), fetch.range().length());
        ByteBufferAsyncResponseTransformer body = new ByteBufferAsyncResponseTransformer(
                destination, fetch.range().length());
        return asyncClient.getObject(request, body);
    }

    /** Completes a 416 fetch normally with its entries left at 0; rethrows every other failure mapped. */
    private @Nullable Void failedFetch(Throwable failure) {
        StorageException translated = unwrapBatchFailure(failure);
        if (translated instanceof RangeNotSatisfiableException) {
            return null;
        }
        throw translated;
    }

    /** Unwraps async completion wrappers and maps SDK failures onto the storage exception hierarchy. */
    private StorageException unwrapBatchFailure(Throwable failure) {
        Throwable cause = failure;
        while ((cause instanceof CompletionException || cause instanceof ExecutionException)
                && cause.getCause() != null) {
            cause = cause.getCause();
        }
        if (cause instanceof StorageException storageFailure) {
            return storageFailure;
        }
        if (cause instanceof NoSuchKeyException noSuchKey) {
            return new NotFoundException("S3 object does not exist: s3://" + s3Location, noSuchKey);
        }
        if (cause instanceof S3Exception s3Failure) {
            return S3ExceptionMapper.map(s3Failure, s3Location.key());
        }
        return new StorageException("Failed to read ranges from S3: " + cause.getMessage(), cause);
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
            HeadObjectRequest.Builder headBuilder =
                    HeadObjectRequest.builder().bucket(s3Location.bucket()).key(s3Location.key());
            if (requesterPays) {
                headBuilder.requestPayer(RequestPayer.REQUESTER);
            }
            HeadObjectResponse headResponse = s3Client.headObject(headBuilder.build());
            Long size = headResponse.contentLength();
            return size == null ? OptionalLong.empty() : OptionalLong.of(size);
        } catch (NoSuchKeyException e) {
            throw new NotFoundException("S3 object does not exist: s3://" + s3Location, e);
        } catch (S3Exception e) {
            throw S3ExceptionMapper.map(e, s3Location.key());
        } catch (SdkException e) {
            throw new StorageException("Failed to access S3 object " + s3Location + ": " + e.getMessage(), e);
        }
    }

    /**
     * Captures the total object size from a range response's {@code Content-Range} header, when not already known. Lets
     * a read that happens before any {@link #size()} call populate the memoized value for free, without an extra HEAD
     * request.
     */
    private void captureSizeFrom(GetObjectResponse response) {
        if (contentLength.get() != null) {
            return;
        }
        ContentRange.totalOf(response.contentRange())
                .ifPresent(total -> contentLength.compareAndSet(null, OptionalLong.of(total)));
    }

    @Override
    public String getSourceIdentifier() {
        return s3Location.toString();
    }

    @Override
    public void close() {
        // S3Client is typically managed externally and should be closed by the caller
    }
}
