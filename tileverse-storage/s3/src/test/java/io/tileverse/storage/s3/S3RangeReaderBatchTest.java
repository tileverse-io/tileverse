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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.tileverse.storage.AccessDeniedException;
import io.tileverse.storage.RangeRequest;
import io.tileverse.storage.StorageException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.RequestPayer;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Unit tests for the batched read path of {@link S3RangeReader}: one parallel async GET per planned fetch, streamed
 * into its destination, when the CRT client is present; the AbstractRangeReader template otherwise.
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
    private S3Client s3Client;

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
        reader = new S3RangeReader(s3Client, asyncClient, new S3Reference(null, BUCKET, KEY, null), false);
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

        int[] counts = reader.readRanges(requests);

        assertContents(requests, counts);
        verify(asyncClient, times(3)).getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class));
        verify(s3Client, never()).getObject(any(GetObjectRequest.class), any(ResponseTransformer.class));
        verify(s3Client, never()).getObjectAsBytes(any(GetObjectRequest.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void nearbyRangesMergeIntoOneAsyncGet() {
        object.installAsync(asyncClient);
        List<RangeRequest> requests = batchOf(new long[][] {{0, 100}, {1_000, 100}});

        int[] counts = reader.readRanges(requests);

        assertContents(requests, counts);
        verify(asyncClient, times(1)).getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class));
    }

    @Test
    void directFetchesStreamIntoTheCallerTargetAtItsPosition() {
        object.installAsync(asyncClient);
        ByteBuffer direct = ByteBuffer.allocateDirect(1000);
        direct.position(100);
        ByteBuffer heap = ByteBuffer.allocate(500);
        heap.position(20);
        List<RangeRequest> requests = List.of(RangeRequest.of(0, 300, direct), RangeRequest.of(2_000_000, 400, heap));

        int[] counts = reader.readRanges(requests);

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

        int[] counts = reader.readRanges(requests);

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

        int[] counts = reader.readRanges(requests);

        assertThat(counts).containsExactly(0, 100, 50);
        assertContents(requests, counts);
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
        verify(s3Client, never()).headObject(any(HeadObjectRequest.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void requesterPaysAppliesToAsyncBatchRequests() {
        object.installAsync(asyncClient);
        S3RangeReader paying = new S3RangeReader(s3Client, asyncClient, new S3Reference(null, BUCKET, KEY, null), true);

        paying.readRanges(batchOf(new long[][] {{0, 100}, {1_000_000, 100}}));

        ArgumentCaptor<GetObjectRequest> sent = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(asyncClient, times(2)).getObject(sent.capture(), any(AsyncResponseTransformer.class));
        assertThat(sent.getAllValues()).allMatch(request -> request.requestPayer() == RequestPayer.REQUESTER);
    }

    @Test
    @SuppressWarnings("unchecked")
    void withoutAsyncClientTheTemplateRunsOnTheSyncClient() {
        object.installSync(s3Client);
        S3RangeReader syncOnly = new S3RangeReader(s3Client, new S3Reference(null, BUCKET, KEY, null), false);

        List<RangeRequest> farApart = batchOf(new long[][] {{0, 100}, {1_000_000, 200}});
        int[] counts = syncOnly.readRanges(farApart);

        assertContents(farApart, counts);
        verify(s3Client, times(2)).getObject(any(GetObjectRequest.class), any(ResponseTransformer.class));
        verify(asyncClient, never()).getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class));

        List<RangeRequest> nearby = batchOf(new long[][] {{0, 100}, {1_000, 100}});
        int[] mergedCounts = syncOnly.readRanges(nearby);

        assertContents(nearby, mergedCounts);
        verify(s3Client, times(3)).getObject(any(GetObjectRequest.class), any(ResponseTransformer.class));
    }
}
