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

import static io.tileverse.storage.RangeReaderTestSupport.counts;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.tileverse.storage.AccessDeniedException;
import io.tileverse.storage.BatchReadResult;
import io.tileverse.storage.RangeRequest;
import io.tileverse.storage.StorageException;
import io.tileverse.storage.batch.BatchSettings;
import io.tileverse.storage.batch.CoalescingPolicy;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.RequestPayer;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Unit tests for the batched read path of {@link S3RangeReader}: one parallel async GET per planned fetch, streamed
 * into its destination.
 */
@ExtendWith(MockitoExtension.class)
class S3RangeReaderBatchTest {

    private static final String BUCKET = "test-bucket";
    private static final String KEY = "test-key";
    private static final int OBJECT_SIZE = 4 * 1024 * 1024;
    /** Small against the ranges read, large enough to keep the tests quick. */
    private static final int CHUNK_SIZE = 4096;

    private static final byte[] OBJECT = bytesFor(0, OBJECT_SIZE);

    @Mock
    private S3AsyncClient asyncClient;

    private S3ObjectStub object;
    private S3RangeReader reader;

    private static byte byteAt(long offset) {
        return (byte) (offset * 31);
    }

    private static byte[] bytesFor(long offset, int length) {
        byte[] data = new byte[length];
        for (int i = 0; i < length; i++) {
            data[i] = byteAt(offset + i);
        }
        return data;
    }

    private static byte[] contents(ByteBuffer buffer, int from, int length) {
        byte[] out = new byte[length];
        buffer.get(from, out);
        return out;
    }

    @BeforeEach
    void createReader() {
        object = new S3ObjectStub(OBJECT, CHUNK_SIZE);
        reader = new S3RangeReader(asyncClient, new S3Reference(null, BUCKET, KEY, null), false);
    }

    private static List<RangeRequest> batchOf(long[][] ranges) {
        List<RangeRequest> requests = new ArrayList<>();
        for (long[] range : ranges) {
            requests.add(RangeRequest.of(range[0], (int) range[1], ByteBuffer.allocate((int) range[1])));
        }
        return requests;
    }

    private static void assertContents(List<RangeRequest> requests, int[] counts) {
        for (int i = 0; i < requests.size(); i++) {
            RangeRequest request = requests.get(i);
            long offset = request.range().offset();
            int expected = (int) Math.max(0, Math.min(request.range().length(), OBJECT_SIZE - offset));
            assertThat(counts[i]).as("bytes for entry " + i).isEqualTo(expected);
            ByteBuffer target = request.target().duplicate().flip();
            assertThat(target.remaining()).isEqualTo(expected);
            assertThat(contents(target, 0, expected)).as("entry " + i).containsExactly(bytesFor(offset, expected));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void farApartRangesFetchInParallelOneAsyncGetEach() {
        object.installAsync(asyncClient);
        List<RangeRequest> requests = batchOf(new long[][] {{0, 100}, {1_000_000, 200}, {2_000_000, 300}});

        int[] counts = counts(reader.readRanges(requests));

        assertContents(requests, counts);
        verify(asyncClient, times(3)).getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void nearbyRangesMergeIntoOneAsyncGet() {
        object.installAsync(asyncClient);
        List<RangeRequest> requests = batchOf(new long[][] {{0, 100}, {1_000, 100}});

        BatchReadResult result = reader.readRanges(requests);

        assertContents(requests, counts(result));
        verify(asyncClient, times(1)).getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class));
        assertThat(result.fetches()).isEqualTo(1);
        assertThat(result.bytesRequested()).isEqualTo(200);
        assertThat(result.bytesTransferred())
                .as("the merged GET bridges the 900-byte gap")
                .isEqualTo(1_100);
        assertThat(result.bytesFromCache()).isZero();
    }

    @Test
    void directFetchesStreamIntoTheCallerTargetAtItsPosition() {
        object.installAsync(asyncClient);
        ByteBuffer direct = ByteBuffer.allocateDirect(1000);
        direct.position(100);
        ByteBuffer heap = ByteBuffer.allocate(500);
        heap.position(20);
        List<RangeRequest> requests = List.of(RangeRequest.of(0, 300, direct), RangeRequest.of(2_000_000, 400, heap));

        int[] counts = counts(reader.readRanges(requests));

        assertThat(counts).containsExactly(300, 400);
        assertThat(direct.position()).isEqualTo(400);
        assertThat(direct.limit()).isEqualTo(1000);
        assertThat(contents(direct, 100, 300)).containsExactly(bytesFor(0, 300));
        assertThat(heap.position()).isEqualTo(420);
        assertThat(contents(heap, 20, 400)).containsExactly(bytesFor(2_000_000, 400));
    }

    @Test
    @SuppressWarnings("unchecked")
    void mergedFetchesScatterFromStreamedScratch() {
        object.installAsync(asyncClient);
        ByteBuffer first = ByteBuffer.allocate(200);
        first.position(50);
        ByteBuffer second = ByteBuffer.allocateDirect(100);
        List<RangeRequest> requests = List.of(RangeRequest.of(0, 100, first), RangeRequest.of(1_000, 100, second));

        int[] counts = counts(reader.readRanges(requests));

        assertThat(counts).containsExactly(100, 100);
        verify(asyncClient, times(1)).getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class));
        assertThat(first.position()).isEqualTo(150);
        assertThat(contents(first, 50, 100)).containsExactly(bytesFor(0, 100));
        assertThat(second.position()).isEqualTo(100);
        assertThat(contents(second, 0, 100)).containsExactly(bytesFor(1_000, 100));
    }

    @Test
    void pastEofFetchesReportZeroWithoutFailingTheBatch() {
        object.installAsync(asyncClient);
        List<RangeRequest> requests =
                batchOf(new long[][] {{OBJECT_SIZE + 1_000_000, 10}, {0, 100}, {OBJECT_SIZE - 50, 100}});

        BatchReadResult result = reader.readRanges(requests);

        assertThat(counts(result)).containsExactly(0, 100, 50);
        assertContents(requests, counts(result));
        assertThat(result.fetches()).as("the past-EOF GET was issued too").isEqualTo(3);
        assertThat(result.bytesTransferred()).isEqualTo(150);
    }

    @Test
    void aBodyLongerThanTheFetchFailsTheBatch() {
        object.respondingWithExtraBytes(10);
        object.installAsync(asyncClient);
        List<RangeRequest> requests = batchOf(new long[][] {{0, 100}, {1_000_000, 100}});

        assertThatThrownBy(() -> reader.readRanges(requests))
                .isInstanceOf(StorageException.class)
                .hasMessageContaining("more data than requested");
    }

    @Test
    @SuppressWarnings("unchecked")
    void nonRangeFailuresAbortTheBatchMapped() {
        lenient()
                .when(asyncClient.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
                .thenReturn(CompletableFuture.failedFuture(S3Exception.builder()
                        .statusCode(403)
                        .message("Forbidden")
                        .build()));
        List<RangeRequest> requests = batchOf(new long[][] {{0, 100}, {1_000_000, 100}});

        assertThatThrownBy(() -> reader.readRanges(requests)).isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void sizeIsCapturedFromAsyncResponses() {
        object.installAsync(asyncClient);
        reader.readRanges(batchOf(new long[][] {{0, 100}, {1_000_000, 100}}));

        assertThat(reader.size()).hasValue(OBJECT_SIZE);
        verify(asyncClient, never()).headObject(any(HeadObjectRequest.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void requesterPaysAppliesToAsyncBatchRequests() {
        object.installAsync(asyncClient);
        S3RangeReader paying = new S3RangeReader(asyncClient, new S3Reference(null, BUCKET, KEY, null), true);

        paying.readRanges(batchOf(new long[][] {{0, 100}, {1_000_000, 100}}));

        ArgumentCaptor<GetObjectRequest> sent = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(asyncClient, times(2)).getObject(sent.capture(), any(AsyncResponseTransformer.class));
        assertThat(sent.getAllValues()).allMatch(request -> request.requestPayer() == RequestPayer.REQUESTER);
    }

    /**
     * The reader keeps at most the configured number of fetches in flight: two of five fetches start at once, each
     * completion admits the next, and the batch lands every byte.
     */
    @Test
    void theInFlightBoundAdmitsTheNextFetchOnCompletion() throws Exception {
        object.holdingAsyncResponses().installAsync(asyncClient);
        S3RangeReader bounded = readerBoundedTo(2);
        List<RangeRequest> requests = farApartRanges(5);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            Future<BatchReadResult> batch = caller.submit(() -> bounded.readRanges(requests));

            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> assertThat(object.heldResponses()).isEqualTo(2));
            assertThat(object.attempts())
                    .as("fetches started before any completion")
                    .isEqualTo(2);

            object.releaseOne();
            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> assertThat(object.attempts()).isEqualTo(3));
            assertThat(object.heldResponses()).isEqualTo(2);
            assertThat(batch.isDone()).isFalse();

            object.releaseAll();
            await().atMost(Duration.ofSeconds(5)).until(() -> object.attempts() == 5 && object.heldResponses() == 0);
            object.releaseAll();
            int[] counts = counts(batch.get(5, TimeUnit.SECONDS));

            assertContents(requests, counts);
            assertThat(object.attempts()).isEqualTo(5);
            assertThat(object.asyncPeakInFlight()).isEqualTo(2);
        } finally {
            caller.shutdownNow();
        }
    }

    /**
     * A failed fetch stops the admission of new ones, and the call throws only after the fetches already in flight have
     * completed: no thread writes a target after the call returns or throws.
     */
    @Test
    void aFailureStopsAdmissionAndTheCallDrainsTheFetchesInFlight() {
        object.holdingAsyncResponses().installAsync(asyncClient);
        S3RangeReader bounded = readerBoundedTo(2);
        List<RangeRequest> requests = farApartRanges(5);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            Future<BatchReadResult> batch = caller.submit(() -> bounded.readRanges(requests));
            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> assertThat(object.heldResponses()).isEqualTo(2));

            object.failOne(
                    S3Exception.builder().statusCode(403).message("Forbidden").build());

            // admission runs on the thread completing a response, and that thread is this one
            assertThat(object.attempts())
                    .as("no fetch admitted after the failure")
                    .isEqualTo(2);
            assertThat(batch.isDone())
                    .as("the call waits for the fetch still in flight")
                    .isFalse();

            object.releaseOne();

            assertThatThrownBy(() -> batch.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(AccessDeniedException.class);
            assertThat(object.attempts()).isEqualTo(2);
        } finally {
            caller.shutdownNow();
        }
    }

    /**
     * A completion admits the next fetch on the completing thread. An {@link Error} launching that fetch, such as an
     * exhausted heap, must still end the call, with that same Error.
     */
    @Test
    void anErrorLaunchingAFetchAdmittedByACompletionEndsTheCallWithThatError() {
        OutOfMemoryError heapExhausted = new OutOfMemoryError("Java heap space");
        object.holdingAsyncResponses().throwingOnAsyncCall(2, heapExhausted).installAsync(asyncClient);
        S3RangeReader bounded = readerBoundedTo(1);
        List<RangeRequest> requests = farApartRanges(2);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            Future<BatchReadResult> batch = caller.submit(() -> bounded.readRanges(requests));
            await().atMost(Duration.ofSeconds(5)).until(() -> object.heldResponses() == 1);

            object.releaseOne();

            assertThatThrownBy(() -> batch.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .cause()
                    .isSameAs(heapExhausted);
        } finally {
            caller.shutdownNow();
        }
    }

    /**
     * An {@link Error} launching a fetch on the calling thread follows the target contract too: the call throws only
     * once the fetches in flight have completed, and no fetch writes a target after the call threw.
     */
    @Test
    void anErrorLaunchingAFetchIsThrownOnlyAfterTheFetchesInFlightCompleted() {
        OutOfMemoryError heapExhausted = new OutOfMemoryError("Java heap space");
        object.holdingAsyncResponses().throwingOnAsyncCall(2, heapExhausted).installAsync(asyncClient);
        S3RangeReader bounded = readerBoundedTo(2);
        List<RangeRequest> requests = farApartRanges(2);
        ByteBuffer heldTarget = requests.get(0).target();
        AtomicInteger heldTargetPositionWhenTheCallEnded = new AtomicInteger(-1);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            Future<BatchReadResult> batch = caller.submit(() -> {
                try {
                    return bounded.readRanges(requests);
                } finally {
                    heldTargetPositionWhenTheCallEnded.set(heldTarget.position());
                }
            });
            await().atMost(Duration.ofSeconds(5)).until(() -> object.heldResponses() == 1);
            assertThatThrownBy(() -> batch.get(200, TimeUnit.MILLISECONDS))
                    .as("the call waits for the fetch in flight")
                    .isInstanceOf(TimeoutException.class);

            object.releaseOne();

            assertThatThrownBy(() -> batch.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .cause()
                    .isSameAs(heapExhausted);
            assertThat(heldTarget.position())
                    .as("no fetch writes a target after the call threw")
                    .isEqualTo(heldTargetPositionWhenTheCallEnded.get());
        } finally {
            caller.shutdownNow();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void anErrorFailingAFetchIsRethrownUnchanged() {
        OutOfMemoryError heapExhausted = new OutOfMemoryError("Java heap space");
        when(asyncClient.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
                .thenReturn(CompletableFuture.failedFuture(heapExhausted));
        List<RangeRequest> requests = batchOf(new long[][] {{0, 100}, {1_000_000, 100}});

        assertThatThrownBy(() -> reader.readRanges(requests)).isSameAs(heapExhausted);
    }

    @Test
    void anUnboundedReaderLaunchesEveryFetchAtOnce() throws Exception {
        object.holdingAsyncResponses().installAsync(asyncClient);
        S3RangeReader unbounded = readerBoundedTo(0);
        List<RangeRequest> requests = farApartRanges(5);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            Future<BatchReadResult> batch = caller.submit(() -> unbounded.readRanges(requests));
            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> assertThat(object.heldResponses()).isEqualTo(5));

            object.releaseAll();

            assertContents(requests, counts(batch.get(5, TimeUnit.SECONDS)));
            assertThat(object.asyncPeakInFlight()).isEqualTo(5);
        } finally {
            caller.shutdownNow();
        }
    }

    /**
     * A fetch may complete on the thread that launched it, inside the launch loop. Thousands of such completions must
     * run the loop on, not nest it: with a bound of two, a plan of twenty thousand fetches would otherwise recurse
     * twenty thousand levels deep.
     */
    @Test
    @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void completionsArrivingInsideTheLaunchLoopDoNotNestIt() {
        object.installAsync(asyncClient);
        S3RangeReader bounded = readerBoundedTo(2);
        int fetchCount = 20_000;
        long[][] ranges = new long[fetchCount][];
        for (int i = 0; i < fetchCount; i++) {
            ranges[i] = new long[] {i, 1};
        }
        List<RangeRequest> requests = batchOf(ranges);

        BatchReadResult result = bounded.readRanges(requests);

        assertContents(requests, counts(result));
        assertThat(result.fetches()).isEqualTo(fetchCount);
        assertThat(object.asyncPeakInFlight()).isEqualTo(1);
    }

    /**
     * The SDK's retry stage starts a scheduled attempt without checking whether the call already failed. A retry
     * scheduled before the API call timeout can stream its body after the batch throws, and that body must not reach
     * the caller's target.
     */
    @Test
    void aDirectFetchTimingOutWritesNothingToTheTargetAfterTheCallThrows() {
        TimedOutCall call = new TimedOutCall(asyncClient);
        ByteBuffer target = ByteBuffer.allocate(100);
        List<RangeRequest> requests = List.of(RangeRequest.of(0, 100, target));

        assertThatThrownBy(() -> reader.readRanges(requests)).isInstanceOf(StorageException.class);
        call.startTheScheduledRetry(100);

        assertThat(contents(target, 0, 100)).containsOnly((byte) 0);
    }

    /** Same as the direct fetch: the body of the retry must not reach the scratch returned to the pool. */
    @Test
    void aMergedFetchTimingOutWritesNothingToItsScratchAfterTheCallThrows() {
        TimedOutCall call = new TimedOutCall(asyncClient);
        List<RangeRequest> requests = batchOf(new long[][] {{0, 100}, {1_000, 100}});

        assertThatThrownBy(() -> reader.readRanges(requests)).isInstanceOf(StorageException.class);
        call.startTheScheduledRetry(1_100);

        assertThat(call.bytesWrittenByTheRetry()).isZero();
    }

    private S3RangeReader readerBoundedTo(int maxInFlightFetches) {
        BatchSettings noMerging = new BatchSettings(-1, CoalescingPolicy.DEFAULT_MAX_FETCH_BYTES, maxInFlightFetches);
        return new S3RangeReader(asyncClient, new S3Reference(null, BUCKET, KEY, null), false, noMerging);
    }

    /** One fetch per range: the ranges sit farther apart than any gap a merging policy would bridge. */
    private static List<RangeRequest> farApartRanges(int count) {
        long[][] ranges = new long[count][];
        for (int i = 0; i < count; i++) {
            ranges[i] = new long[] {i * 700_000L, 100};
        }
        return batchOf(ranges);
    }
}
