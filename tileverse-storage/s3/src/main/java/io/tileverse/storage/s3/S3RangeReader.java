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
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.Nullable;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.RequestPayer;

/**
 * A {@link RangeReader} implementation that reads data from an AWS S3-compatible object storage service.
 *
 * <p>This class reads S3 objects through the {@link S3AsyncClient} of the AWS SDK for Java v2 and serves both standard
 * AWS S3 and self-hosted S3-compatible services like MinIO.
 *
 * <h2>Construction and Configuration</h2>
 *
 * Readers are opened by {@code S3Storage#openRangeReader(String)} and borrow the SDK client of that storage; opening a
 * reader issues no request. Credentials, region, endpoint and path-style addressing are therefore settled before a
 * reader exists: {@link S3StorageProvider} resolves them from the {@code storage.s3.*} parameters and the base URI, and
 * {@link S3ClientCache} builds one client set per distinct combination, shared by the storages requesting it. A caller
 * holding a configured {@link S3AsyncClient} hands it to {@link S3StorageProvider#open(java.net.URI, S3AsyncClient)}
 * instead.
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
 * {@code readRanges} merges nearby ranges under the {@link BatchSettings} of the Storage opening the reader and fetches
 * the planned ranges in parallel, with one async {@code getObject} each.
 *
 * <p>Range bodies stream straight into their destination, heap or direct, through an {@link AsyncResponseTransformer}
 * writing chunks as they arrive, inside the SDK's retry loop. No read holds a full-size heap copy of the response;
 * transient heap per read is bounded by the SDK's chunk size, plus one pooled scratch buffer for a merged fetch
 * scattering to several requests. Once a read returns or throws, no thread writes its destination.
 */
final class S3RangeReader extends AbstractRangeReader implements RangeReader {

    private final S3AsyncClient client;
    private final S3Reference s3Location;
    private final boolean requesterPays;
    private final BatchSettings batchSettings;

    private final AtomicReference<OptionalLong> contentLength = new AtomicReference<>();

    /**
     * Creates a reader batching under the {@link BatchSettings#objectStoreDefaults() object-store defaults}.
     *
     * <p>Construction performs no I/O. A missing object is reported by the first {@link #readRange(long, int)} or
     * {@link #size()} call instead of at construction time.
     *
     * @param client the client serving the reads
     * @param s3Location the S3 reference (bucket + key)
     * @param requesterPays when {@code true}, requests add {@code x-amz-request-payer: requester}
     */
    S3RangeReader(S3AsyncClient client, S3Reference s3Location, boolean requesterPays) {
        this(client, s3Location, requesterPays, BatchSettings.objectStoreDefaults());
    }

    /**
     * Creates a reader with the batch settings of the Storage it belongs to.
     *
     * @param client the client serving the reads
     * @param s3Location the S3 reference (bucket + key)
     * @param requesterPays when {@code true}, requests add {@code x-amz-request-payer: requester}
     * @param batchSettings the merge policy and in-flight bound for batched reads
     */
    S3RangeReader(S3AsyncClient client, S3Reference s3Location, boolean requesterPays, BatchSettings batchSettings) {
        this.client = Objects.requireNonNull(client, "S3AsyncClient cannot be null");
        this.s3Location = Objects.requireNonNull(s3Location, "S3Location cannot be null");
        this.requesterPays = requesterPays;
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
     * Streams the range body straight into {@code target} through {@link ByteBufferAsyncResponseTransformer}, inside
     * the SDK's retry loop, and waits for it. The transformer closes before this method returns or throws: an attempt
     * abandoned by the SDK, a scheduled retry included, never writes the target afterwards.
     */
    @Override
    protected int readRangeNoFlip(final long offset, final int actualLength, ByteBuffer target) {
        int start = target.position();
        GetObjectRequest request = buildGetRequest(offset, actualLength);
        ByteBufferAsyncResponseTransformer body = new ByteBufferAsyncResponseTransformer(target, actualLength);
        try {
            ByteBufferAsyncResponseTransformer.Result streamed =
                    S3Calls.await(s3Location.key(), () -> client.getObject(request, body));
            captureSizeFrom(streamed.response());
            target.position(start + streamed.bytesWritten());
            return streamed.bytesWritten();
        } finally {
            body.close();
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
     * Reads a batch with one async {@code getObject} per planned fetch, at most {@link #maxConcurrentFetches()} of them
     * in flight at once, with each completion admitting the next.
     *
     * <p>A fetch answered 416 (entirely past EOF) reports 0 bytes for its entries, exactly like {@code readRange}; any
     * other failure, an {@link Error} included, stops the admission of new fetches and aborts the whole call once the
     * fetches in flight have completed, rethrowing an Error unchanged. Worst-case amplification: the requested bytes
     * plus the gaps merged by the object-store policy, at most {@link CoalescingPolicy#maxFetchBytes()} per fetch. Peak
     * heap scratch of one call is the in-flight bound times that cap, since a merged fetch borrows scratch for its
     * whole extent. The result counts one fetch per async GET issued and, as bytes transferred, the bytes streamed by
     * those GETs.
     *
     * @param requests the ranges to read and their target buffers
     * @return the bytes read per request, in request order, and what the call cost
     */
    @Override
    public BatchReadResult readRanges(List<RangeRequest> requests) {
        RangeRequest.validate(requests);
        if (requests.isEmpty()) {
            return BatchReadResult.EMPTY;
        }
        List<PlannedFetch> fetches = BatchPlanner.plan(requests, coalescingPolicy());
        int[] counts = new int[requests.size()];
        if (fetches.isEmpty()) {
            return BatchReadResult.of(requests, counts, 0, 0, 0);
        }
        BoundedFetches run = new BoundedFetches(fetches, requests, counts);
        try {
            run.run(maxConcurrentFetches());
        } catch (CompletionException failure) {
            throw S3Calls.map(failure, s3Location.key());
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
        return client.getObject(request, body);
    }

    /**
     * Completes a 416 fetch normally with its entries left at 0 and no bytes; rethrows every other failure mapped, and
     * an {@link Error} as itself.
     */
    private Integer failedFetch(Throwable failure) {
        StorageException translated = S3Calls.map(failure, s3Location.key());
        if (translated instanceof RangeNotSatisfiableException) {
            return 0;
        }
        throw translated;
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
        HeadObjectRequest.Builder headBuilder =
                HeadObjectRequest.builder().bucket(s3Location.bucket()).key(s3Location.key());
        if (requesterPays) {
            headBuilder.requestPayer(RequestPayer.REQUESTER);
        }
        HeadObjectRequest request = headBuilder.build();
        HeadObjectResponse headResponse = S3Calls.await(s3Location.key(), () -> client.headObject(request));
        Long size = headResponse.contentLength();
        return size == null ? OptionalLong.empty() : OptionalLong.of(size);
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
        // the S3AsyncClient belongs to the Storage opening this reader
    }
}
