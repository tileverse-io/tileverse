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

import io.tileverse.storage.RangeRequest;
import io.tileverse.storage.StorageException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

/**
 * Batched reads against an endpoint that answers without an {@code ETag} header, which the CRT client rejects.
 * {@link S3StorageS3ProxyFilesystemIT} covers the same ground against a real endpoint.
 */
@ExtendWith(MockitoExtension.class)
class S3RangeReaderMissingEtagTest {

    private static final String BUCKET = "test-bucket";
    private static final String KEY = "test-key";
    private static final int OBJECT_SIZE = 4 * 1024 * 1024;
    private static final int CHUNK_SIZE = 4096;

    private static final byte[] OBJECT = bytesFor(0, OBJECT_SIZE);

    /** The message the CRT client raises for a response with no ETag header. */
    private static final String CRT_MESSAGE =
            "Failed to send the request: Response missing required ETag header. (SDK Attempt Count: 1)";

    @Mock
    private S3Client s3Client;

    @Mock
    private S3AsyncClient asyncClient;

    private S3ObjectStub object;
    private EndpointEtags endpointEtags;
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

    @BeforeEach
    void createReader() {
        object = new S3ObjectStub(OBJECT, CHUNK_SIZE).respondingWithoutEtag();
        endpointEtags = new EndpointEtags();
        reader = new S3RangeReader(
                s3Client, asyncClient, new S3Reference(null, BUCKET, KEY, null), false, endpointEtags);
    }

    private static List<RangeRequest> batchOf(long[][] ranges) {
        List<RangeRequest> requests = new ArrayList<>();
        for (long[] range : ranges) {
            requests.add(RangeRequest.of(range[0], (int) range[1], ByteBuffer.allocate((int) range[1])));
        }
        return requests;
    }

    private static List<RangeRequest> farApartRanges() {
        return batchOf(new long[][] {{0, 100}, {1_000_000, 200}, {2_000_000, 300}});
    }

    private static void assertContents(List<RangeRequest> requests, int[] counts) {
        for (int i = 0; i < requests.size(); i++) {
            RangeRequest request = requests.get(i);
            int length = request.range().length();
            assertThat(counts[i]).as("bytes for entry " + i).isEqualTo(length);
            byte[] read = new byte[length];
            request.target().duplicate().flip().get(read);
            assertThat(read)
                    .as("entry " + i)
                    .containsExactly(bytesFor(request.range().offset(), length));
        }
    }

    @SuppressWarnings("unchecked")
    private void asyncClientRejectsWith(RuntimeException failure) {
        lenient()
                .when(asyncClient.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
                .thenReturn(CompletableFuture.failedFuture(failure));
    }

    @Test
    @SuppressWarnings("unchecked")
    void theFirstRejectionRerunsTheBatchOnTheSyncClient() {
        object.installSync(s3Client);
        asyncClientRejectsWith(SdkClientException.create(CRT_MESSAGE));
        List<RangeRequest> requests = farApartRanges();

        int[] counts = reader.readRanges(requests);

        assertContents(requests, counts);
        assertThat(endpointEtags.omitted()).isTrue();
        verify(s3Client, times(3)).getObject(any(GetObjectRequest.class), any(ResponseTransformer.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void anEndpointAlreadyKnownToOmitTheEtagIsNeverOfferedToTheAsyncClient() {
        object.installSync(s3Client);
        endpointEtags.recordOmission();
        List<RangeRequest> requests = farApartRanges();

        int[] counts = reader.readRanges(requests);

        assertContents(requests, counts);
        verify(asyncClient, never()).getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void aSingleReadWithoutAnEtagKeepsTheNextBatchOffTheAsyncClient() {
        object.installSync(s3Client);

        reader.readRange(0L, 64, ByteBuffer.allocate(64));
        List<RangeRequest> requests = farApartRanges();
        int[] counts = reader.readRanges(requests);

        assertContents(requests, counts);
        assertThat(endpointEtags.omitted()).isTrue();
        verify(asyncClient, never()).getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class));
    }

    @Test
    void theRerunFillsTheTargetsFromThePositionsTheCallerChose() {
        object.installSync(s3Client);
        asyncClientRejectsWith(SdkClientException.create(CRT_MESSAGE));
        ByteBuffer first = ByteBuffer.allocate(500);
        first.position(100);
        ByteBuffer second = ByteBuffer.allocate(400);
        second.position(20);
        List<RangeRequest> requests = List.of(RangeRequest.of(0, 300, first), RangeRequest.of(2_000_000, 300, second));

        int[] counts = reader.readRanges(requests);

        assertThat(counts).containsExactly(300, 300);
        assertThat(first.position()).isEqualTo(400);
        assertThat(second.position()).isEqualTo(320);
        byte[] fromFirst = new byte[300];
        first.duplicate().position(100).get(fromFirst);
        assertThat(fromFirst).containsExactly(bytesFor(0, 300));
        byte[] fromSecond = new byte[300];
        second.duplicate().position(20).get(fromSecond);
        assertThat(fromSecond).containsExactly(bytesFor(2_000_000, 300));
    }

    @Test
    @SuppressWarnings("unchecked")
    void anyOtherBatchFailureIsReportedAndLeavesTheAsyncClientInUse() {
        asyncClientRejectsWith(SdkClientException.create("Unable to execute HTTP request: connection reset"));
        List<RangeRequest> requests = farApartRanges();

        assertThatThrownBy(() -> reader.readRanges(requests))
                .isInstanceOf(StorageException.class)
                .hasMessageContaining("connection reset");

        assertThat(endpointEtags.omitted()).isFalse();
        verify(s3Client, never()).getObject(any(GetObjectRequest.class), any(ResponseTransformer.class));
    }
}
