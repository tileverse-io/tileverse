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
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.RequestPayer;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Unit tests for the batched read path of {@link S3RangeReader}: one parallel async GET per planned fetch when the CRT
 * client is present, the AbstractRangeReader template otherwise.
 */
@ExtendWith(MockitoExtension.class)
class S3RangeReaderBatchTest {

    private static final String BUCKET = "test-bucket";
    private static final String KEY = "test-key";
    private static final int OBJECT_SIZE = 4 * 1024 * 1024;

    @Mock
    private S3Client s3Client;

    @Mock
    private S3AsyncClient asyncClient;

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

    private static long[] parseRange(GetObjectRequest request) {
        String[] bounds = request.range().replace("bytes=", "").split("-");
        return new long[] {Long.parseLong(bounds[0]), Long.parseLong(bounds[1])};
    }

    @SuppressWarnings("unchecked")
    private void stubAsyncObjectStore() {
        lenient()
                .when(asyncClient.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
                .thenAnswer(invocation -> {
                    GetObjectRequest request = invocation.getArgument(0);
                    long[] bounds = parseRange(request);
                    if (bounds[0] >= OBJECT_SIZE) {
                        return CompletableFuture.failedFuture(S3Exception.builder()
                                .statusCode(416)
                                .message("Requested Range Not Satisfiable")
                                .build());
                    }
                    int available = (int) (Math.min(bounds[1] + 1, OBJECT_SIZE) - bounds[0]);
                    GetObjectResponse response = GetObjectResponse.builder()
                            .contentRange("bytes " + bounds[0] + "-" + (bounds[0] + available - 1) + "/" + OBJECT_SIZE)
                            .build();
                    return CompletableFuture.completedFuture(
                            ResponseBytes.fromByteArray(response, bytesFor(bounds[0], available)));
                });
    }

    private void stubSyncObjectStore() {
        lenient().when(s3Client.getObjectAsBytes(any(GetObjectRequest.class))).thenAnswer(invocation -> {
            GetObjectRequest request = invocation.getArgument(0);
            long[] bounds = parseRange(request);
            if (bounds[0] >= OBJECT_SIZE) {
                throw S3Exception.builder()
                        .statusCode(416)
                        .message("Requested Range Not Satisfiable")
                        .build();
            }
            int available = (int) (Math.min(bounds[1] + 1, OBJECT_SIZE) - bounds[0]);
            GetObjectResponse response = GetObjectResponse.builder()
                    .contentRange("bytes " + bounds[0] + "-" + (bounds[0] + available - 1) + "/" + OBJECT_SIZE)
                    .build();
            return ResponseBytes.fromByteArray(response, bytesFor(bounds[0], available));
        });
    }

    @BeforeEach
    void createReader() {
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
            for (int b = 0; b < expected; b++) {
                assertThat(target.get(b)).as("byte " + b + " of entry " + i).isEqualTo(byteAt(offset + b));
            }
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void farApartRangesFetchInParallelOneAsyncGetEach() {
        stubAsyncObjectStore();
        List<RangeRequest> requests = batchOf(new long[][] {{0, 100}, {1_000_000, 200}, {2_000_000, 300}});

        int[] counts = reader.readRanges(requests);

        assertContents(requests, counts);
        verify(asyncClient, times(3)).getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class));
        verify(s3Client, never()).getObjectAsBytes(any(GetObjectRequest.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void nearbyRangesMergeIntoOneAsyncGet() {
        stubAsyncObjectStore();
        List<RangeRequest> requests = batchOf(new long[][] {{0, 100}, {1_000, 100}});

        int[] counts = reader.readRanges(requests);

        assertContents(requests, counts);
        verify(asyncClient, times(1)).getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class));
    }

    @Test
    void pastEofFetchesReportZeroWithoutFailingTheBatch() {
        stubAsyncObjectStore();
        List<RangeRequest> requests =
                batchOf(new long[][] {{OBJECT_SIZE + 1_000_000, 10}, {0, 100}, {OBJECT_SIZE - 50, 100}});

        int[] counts = reader.readRanges(requests);

        assertThat(counts).containsExactly(0, 100, 50);
        assertContents(requests, counts);
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
        stubAsyncObjectStore();
        reader.readRanges(batchOf(new long[][] {{0, 100}, {1_000_000, 100}}));

        assertThat(reader.size()).hasValue(OBJECT_SIZE);
        verify(s3Client, never()).headObject(any(HeadObjectRequest.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void requesterPaysAppliesToAsyncBatchRequests() {
        stubAsyncObjectStore();
        S3RangeReader paying = new S3RangeReader(s3Client, asyncClient, new S3Reference(null, BUCKET, KEY, null), true);

        paying.readRanges(batchOf(new long[][] {{0, 100}, {1_000_000, 100}}));

        ArgumentCaptor<GetObjectRequest> sent = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(asyncClient, times(2)).getObject(sent.capture(), any(AsyncResponseTransformer.class));
        assertThat(sent.getAllValues()).allMatch(request -> request.requestPayer() == RequestPayer.REQUESTER);
    }

    @Test
    @SuppressWarnings("unchecked")
    void withoutAsyncClientTheTemplateRunsOnTheSyncClient() {
        stubSyncObjectStore();
        S3RangeReader syncOnly = new S3RangeReader(s3Client, new S3Reference(null, BUCKET, KEY, null), false);

        List<RangeRequest> farApart = batchOf(new long[][] {{0, 100}, {1_000_000, 200}});
        int[] counts = syncOnly.readRanges(farApart);

        assertContents(farApart, counts);
        verify(s3Client, times(2)).getObjectAsBytes(any(GetObjectRequest.class));
        verify(asyncClient, never()).getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class));

        List<RangeRequest> nearby = batchOf(new long[][] {{0, 100}, {1_000, 100}});
        int[] mergedCounts = syncOnly.readRanges(nearby);

        assertContents(nearby, mergedCounts);
        verify(s3Client, times(3)).getObjectAsBytes(any(GetObjectRequest.class));
    }
}
