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
import io.tileverse.storage.BatchReadResult;
import io.tileverse.storage.ContentRange;
import io.tileverse.storage.NotFoundException;
import io.tileverse.storage.RangeNotSatisfiableException;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.RangeRequest;
import io.tileverse.storage.StorageException;
import io.tileverse.storage.batch.BatchPlanner;
import io.tileverse.storage.batch.BatchSettings;
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
 * {@code readRanges} merges nearby ranges under the {@link BatchSettings} of the Storage the reader was opened from
 * and, when an {@link S3AsyncClient} is present, fetches the planned ranges in parallel with one async
 * {@code getObject} each. Without the async client the {@link AbstractRangeReader} template runs the same plan on the
 * shared batch executor.
 *
 * <p>Range bodies stream straight into their destination, heap or direct, on every path: single reads through a
 * {@link software.amazon.awssdk.core.sync.ResponseTransformer} that keeps the SDK's body-read retries, batched fetches
 * through an {@link AsyncResponseTransformer} writing chunks as they arrive. No read holds a full-size heap copy of the
 * response; transient heap per read is bounded by the SDK's chunk size, plus one pooled scratch buffer for a merged
 * fetch that scatters to several requests.
 */
final class S3RangeReader extends AbstractRangeReader implements RangeReader {

    private final S3Client s3Client;

    @Nullable
    private final S3AsyncClient asyncClient;

    private final S3Reference s3Location;
    private final boolean requesterPays;
    private final EndpointEtags endpointEtags;
    private final BatchSettings batchSettings;

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
     * @param asyncClient the async client for parallel batched reads, or null to batch through the shared executor
     * @param s3Location The S3 reference (bucket + key)
     * @param requesterPays when {@code true}, every request adds {@code x-amz-request-payer: requester}
     */
    S3RangeReader(
            S3Client s3Client, @Nullable S3AsyncClient asyncClient, S3Reference s3Location, boolean requesterPays) {
        this(s3Client, asyncClient, s3Location, requesterPays, new EndpointEtags());
    }

    /**
     * Creates a reader that shares the endpoint's ETag record with the other readers of the same clients, batching
     * under the {@link BatchSettings#objectStoreDefaults() object-store defaults}.
     *
     * @param s3Client The S3 client to use for single reads and metadata
     * @param asyncClient the async client for parallel batched reads, or null to batch through the shared executor
     * @param s3Location The S3 reference (bucket + key)
     * @param requesterPays when {@code true}, every request adds {@code x-amz-request-payer: requester}
     * @param endpointEtags the shared record of the async client rejecting a response for want of an ETag header
     */
    S3RangeReader(
            S3Client s3Client,
            @Nullable S3AsyncClient asyncClient,
            S3Reference s3Location,
            boolean requesterPays,
            EndpointEtags endpointEtags) {
        this(s3Client, asyncClient, s3Location, requesterPays, endpointEtags, BatchSettings.objectStoreDefaults());
    }

    /**
     * Creates a reader with the batch settings of the Storage it belongs to.
     *
     * @param s3Client The S3 client to use for single reads and metadata
     * @param asyncClient the async client for parallel batched reads, or null to batch through the shared executor
     * @param s3Location The S3 reference (bucket + key)
     * @param requesterPays when {@code true}, every request adds {@code x-amz-request-payer: requester}
     * @param endpointEtags the shared record of the async client rejecting a response for want of an ETag header
     * @param batchSettings the merge policy and in-flight bound for batched reads
     */
    S3RangeReader(
            S3Client s3Client,
            @Nullable S3AsyncClient asyncClient,
            S3Reference s3Location,
            boolean requesterPays,
            EndpointEtags endpointEtags,
            BatchSettings batchSettings) {
        this.s3Client = Objects.requireNonNull(s3Client, "S3Client cannot be null");
        this.asyncClient = asyncClient;
        this.s3Location = Objects.requireNonNull(s3Location, "S3Location cannot be null");
        this.requesterPays = requesterPays;
        this.endpointEtags = Objects.requireNonNull(endpointEtags, "EndpointEtags cannot be null");
        this.batchSettings = Objects.requireNonNull(batchSettings, "BatchSettings cannot be null");
    }

    BatchSettings batchSettings() {
        return batchSettings;
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
        return batchSettings.coalescingPolicy();
    }

    @Override
    protected int maxConcurrentFetches() {
        return batchSettings.concurrencyCap();
    }

    /**
     * Reads a batch with one async {@code getObject} per planned fetch when the async client is present, at most
     * {@link #maxConcurrentFetches()} of them in flight at once with each completion admitting the next; without the
     * async client, the batched-read template runs the same plan on the shared executor through the sync client.
     *
     * <p>A fetch answered 416 (entirely past EOF) reports 0 bytes for its entries, exactly like {@code readRange}; any
     * other failure, an {@link Error} included, stops the admission of new fetches and aborts the whole call once the
     * fetches in flight have completed, rethrowing an Error unchanged. Worst-case amplification: the requested bytes
     * plus the gaps merged by the object-store policy, at most {@link CoalescingPolicy#maxFetchBytes()} per fetch. Peak
     * heap scratch of one call is the in-flight bound times that cap, since a merged fetch borrows scratch for its
     * whole extent.
     *
     * <p>A CRT-based async client rejects the responses of an endpoint omitting the {@code ETag} header. From its first
     * rejection on, batches read through the template path. See {@link EndpointEtags}. The result counts one fetch per
     * async GET issued and, as bytes transferred, the bytes streamed by those GETs; the GETs rejected before the switch
     * to the template path stay in the count.
     *
     * @param requests the ranges to read and the buffers they land in
     * @return the bytes read per request, in request order, and what the call cost
     */
    @Override
    public BatchReadResult readRanges(List<RangeRequest> requests) {
        if (asyncClient == null || endpointEtags.omitted()) {
            return super.readRanges(requests);
        }
        RangeRequest.validate(requests);
        if (requests.isEmpty()) {
            return BatchReadResult.EMPTY;
        }
        List<PlannedFetch> fetches = BatchPlanner.plan(requests, coalescingPolicy());
        int[] counts = new int[requests.size()];
        if (fetches.isEmpty()) {
            return BatchReadResult.of(requests, counts, 0, 0, 0);
        }
        int[] targetPositions = targetPositions(requests);
        BoundedFetches run = new BoundedFetches(fetches, requests, counts);
        try {
            run.run(maxConcurrentFetches());
        } catch (CompletionException failure) {
            if (EndpointEtags.rejectedForMissingEtag(failure)) {
                return rereadThroughSyncClient(requests, targetPositions).merge(run.cost());
            }
            throw unwrapBatchFailure(failure);
        }
        return BatchReadResult.of(requests, counts, run.fetchesLaunched(), run.bytesTransferred(), 0);
    }

    /**
     * Launches the fetches of one batch with a bound on how many are outstanding. Each completion admits the next
     * fetch, the first failure stops admission, and {@link #run} returns only once every launched fetch has completed
     * whatever its outcome: no thread writes a target after the batch call returns or throws.
     *
     * <p>A fetch may complete on the thread that launched it, inside the launch loop. Such a completion updates the
     * counters and leaves the admission to the loop already running, which keeps a plan of many instantly completing
     * fetches from nesting one launch inside another.
     */
    private final class BoundedFetches {

        private final List<PlannedFetch> fetches;
        private final List<RangeRequest> requests;
        private final int[] counts;
        private final long[] transferredPerFetch;
        private final CompletableFuture<Void> outcome = new CompletableFuture<>();

        private int maxInFlight;
        private int next; // guarded by this
        private int inFlight; // guarded by this
        private boolean admitting; // guarded by this

        @Nullable
        private Throwable failure; // guarded by this

        BoundedFetches(List<PlannedFetch> fetches, List<RangeRequest> requests, int[] counts) {
            this.fetches = fetches;
            this.requests = requests;
            this.counts = counts;
            this.transferredPerFetch = new long[fetches.size()];
        }

        /** How many fetches were launched, whether or not they completed normally. */
        synchronized int fetchesLaunched() {
            return next;
        }

        /** The bytes streamed by the fetches that completed normally. */
        synchronized long bytesTransferred() {
            long total = 0;
            for (long perFetch : transferredPerFetch) {
                total += perFetch;
            }
            return total;
        }

        /** The transport numbers alone, for a result whose per-request view comes from elsewhere. */
        BatchReadResult cost() {
            return BatchReadResult.of(List.of(), new int[0], fetchesLaunched(), bytesTransferred(), 0);
        }

        /**
         * Runs every fetch and returns once all launched fetches completed.
         *
         * @throws CompletionException wrapping the first fetch failure
         */
        void run(int maxInFlight) {
            this.maxInFlight = maxInFlight;
            synchronized (this) {
                admit();
            }
            outcome.join();
        }

        /**
         * Launches fetches while the bound allows and none has failed, then settles the outcome once nothing is left. A
         * call arriving while the loop runs on this thread returns at once: the loop re-reads the counters on its next
         * turn.
         */
        private void admit() {
            // must be called under the monitor
            if (admitting) {
                return;
            }
            admitting = true;
            try {
                while (failure == null && inFlight < maxInFlight && next < fetches.size()) {
                    launch(next++);
                }
            } finally {
                admitting = false;
            }
            if (inFlight == 0 && (failure != null || next == fetches.size())) {
                settle();
            }
        }

        @SuppressWarnings("java:S1181") // an Error must still settle the batch, or run waits forever
        private void launch(int index) {
            // must be called under the monitor
            inFlight++;
            CompletableFuture<Integer> launched;
            try {
                launched = fetchAsync(fetches.get(index));
            } catch (Throwable failedToLaunch) {
                completed(index, 0, failedToLaunch);
                return;
            }
            launched.whenComplete((streamed, thrown) -> completed(index, streamed == null ? 0 : streamed, thrown));
        }

        private synchronized void completed(int index, int streamed, @Nullable Throwable thrown) {
            inFlight--;
            transferredPerFetch[index] = streamed;
            if (thrown != null && failure == null) {
                failure = thrown;
            }
            admit();
        }

        private void settle() {
            // must be called under the monitor
            if (failure == null) {
                outcome.complete(null);
            } else {
                outcome.completeExceptionally(failure);
            }
        }

        /**
         * Issues one async GET for a fetch, streaming the body into its destination as chunks arrive: the caller's
         * target for a direct fetch, pooled heap scratch scattered to its requests for a merged one. A 416 leaves the
         * fetch's entries at 0 and completes normally; every other failure completes the future exceptionally with the
         * mapped storage exception, or with the {@link Error} unmapped. A failure before the GET goes out, an Error
         * included, is thrown.
         */
        private CompletableFuture<Integer> fetchAsync(PlannedFetch fetch) {
            if (fetch.isDirect()) {
                return fetchDirect(fetch, requests, counts);
            }
            return fetchAndScatter(fetch, requests, counts);
        }
    }

    /**
     * Reruns a batch on the template path and records the endpoint. Targets go back to their positions at the start of
     * the batch: a fetch that already wrote would otherwise land its bytes twice.
     */
    private BatchReadResult rereadThroughSyncClient(List<RangeRequest> requests, int[] targetPositions) {
        endpointEtags.recordOmission();
        restoreTargetPositions(requests, targetPositions);
        return super.readRanges(requests);
    }

    private static int[] targetPositions(List<RangeRequest> requests) {
        int[] positions = new int[requests.size()];
        for (int i = 0; i < positions.length; i++) {
            positions[i] = requests.get(i).target().position();
        }
        return positions;
    }

    private static void restoreTargetPositions(List<RangeRequest> requests, int[] positions) {
        for (int i = 0; i < positions.length; i++) {
            requests.get(i).target().position(positions[i]);
        }
    }

    /**
     * Streams a direct fetch into its single requester's target. The completion closes the body before anything else:
     * no writer may touch the target after the batch returns.
     */
    @SuppressWarnings("java:S3398") // per-fetch I/O of the reader; the launcher only sequences it
    private CompletableFuture<Integer> fetchDirect(PlannedFetch fetch, List<RangeRequest> requests, int[] counts) {
        int requestIndex = fetch.slices().get(0).requestIndex();
        ByteBuffer target = requests.get(requestIndex).target();
        int start = target.position();
        ByteBufferAsyncResponseTransformer body =
                new ByteBufferAsyncResponseTransformer(target, fetch.range().length());
        return streamFetch(fetch, body).handle((streamed, failure) -> {
            body.close();
            if (failure != null) {
                return failedFetch(failure);
            }
            captureSizeFrom(streamed.response());
            target.position(start + streamed.bytesWritten());
            counts[requestIndex] = streamed.bytesWritten();
            return streamed.bytesWritten();
        });
    }

    /**
     * Streams a merged fetch into pooled scratch and scatters it to its requesters. The completion closes the body
     * before anything else: no writer may touch the scratch once it goes back to the pool.
     */
    @SuppressWarnings({
        "java:S3398", // per-fetch I/O of the reader; the launcher only sequences it
        "java:S1181" // the scratch returns to the pool on any failure before the GET goes out, an Error included
    })
    private CompletableFuture<Integer> fetchAndScatter(PlannedFetch fetch, List<RangeRequest> requests, int[] counts) {
        PooledByteBuffer pooled = ByteBufferPool.heapBuffer(fetch.range().length());
        ByteBuffer scratch = pooled.buffer();
        ByteBufferAsyncResponseTransformer body;
        CompletableFuture<ByteBufferAsyncResponseTransformer.Result> attempt;
        try {
            body = new ByteBufferAsyncResponseTransformer(scratch, fetch.range().length());
            attempt = streamFetch(fetch, body);
        } catch (Throwable synchronousFailure) {
            pooled.close();
            throw synchronousFailure;
        }
        return attempt.handle((streamed, failure) -> {
            body.close();
            try (pooled) {
                if (failure != null) {
                    return failedFetch(failure);
                }
                captureSizeFrom(streamed.response());
                fetch.scatter(scratch, streamed.bytesWritten(), requests, counts);
                return streamed.bytesWritten();
            }
        });
    }

    private CompletableFuture<ByteBufferAsyncResponseTransformer.Result> streamFetch(
            PlannedFetch fetch, ByteBufferAsyncResponseTransformer body) {
        GetObjectRequest request =
                buildGetRequest(fetch.range().offset(), fetch.range().length());
        return asyncClient.getObject(request, body);
    }

    /**
     * Completes a 416 fetch normally with its entries left at 0 and no bytes; rethrows every other failure mapped, and
     * an {@link Error} as itself.
     */
    private Integer failedFetch(Throwable failure) {
        StorageException translated = unwrapBatchFailure(failure);
        if (translated instanceof RangeNotSatisfiableException) {
            return 0;
        }
        throw translated;
    }

    /**
     * Unwraps async completion wrappers and maps SDK failures onto the storage exception hierarchy. An {@link Error} is
     * rethrown as itself.
     */
    private StorageException unwrapBatchFailure(Throwable failure) {
        Throwable cause = failure;
        while ((cause instanceof CompletionException || cause instanceof ExecutionException)
                && cause.getCause() != null) {
            cause = cause.getCause();
        }
        if (cause instanceof Error error) {
            throw error;
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
