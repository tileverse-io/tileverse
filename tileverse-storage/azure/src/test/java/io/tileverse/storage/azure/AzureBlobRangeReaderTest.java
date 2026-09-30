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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.azure.core.http.HttpHeaderName;
import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpResponse;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobClientBuilder;
import com.azure.storage.blob.models.BlobDownloadAsyncResponse;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.BlobAsyncClientBase;
import io.tileverse.storage.NotFoundException;
import io.tileverse.storage.StorageException;
import io.tileverse.storage.batch.BatchSettings;
import io.tileverse.storage.batch.CoalescingPolicy;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Unit tests for {@link AzureBlobRangeReader}: bodies stream through the SDK's asynchronous download straight into the
 * caller's buffer.
 */
@ExtendWith(MockitoExtension.class)
class AzureBlobRangeReaderTest {

    private static final String BLOB_URL = "https://teststorage.blob.core.windows.net/testcontainer/test.pmtiles";
    private static final int CONTENT_LENGTH = 10000;
    /** Small enough that every read of interest arrives in many chunks. */
    private static final int CHUNK_SIZE = 64;

    private static final byte[] TEST_DATA = createTestData(CONTENT_LENGTH);

    @Mock
    private BlobClient blobClient;

    @Mock
    private BlobAsyncClientBase asyncClient;

    @Mock
    private BlobProperties blobProperties;

    /** Bytes served past every requested range, like a server ignoring the range. */
    private int extraBytes;

    private AzureBlobRangeReader reader;

    /** Creates test data with a predictable pattern. */
    private static byte[] createTestData(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) (i % 256);
        }
        return data;
    }

    @BeforeEach
    void setUp() {
        lenient().when(blobClient.getBlobUrl()).thenReturn(BLOB_URL);
        stubRangedDownloads();
        reader = newReader();
    }

    private AzureBlobRangeReader newReader() {
        return new AzureBlobRangeReader(blobClient, asyncClient, BatchSettings.objectStoreDefaults());
    }

    /**
     * Serves TEST_DATA as the SDK does: the requested range, truncated at the blob's end, is a body of chunks and the
     * response has the Content-Range header. A range starting past the end fails with a 416. A test stubbing a failure
     * uses {@code doReturn}, because {@code when} would call this answer with null arguments.
     */
    private void stubRangedDownloads() {
        lenient()
                .when(asyncClient.downloadStreamWithResponse(any(), any(), any(), anyBoolean()))
                .thenAnswer(invocation -> {
                    BlobRange range = invocation.getArgument(0);
                    long offset = range.getOffset();
                    if (offset >= CONTENT_LENGTH) {
                        return Mono.error(blobException(416, "InvalidRange"));
                    }
                    int available = (int) Math.min(range.getCount(), CONTENT_LENGTH - offset);
                    return Mono.just(downloadResponse(offset, available));
                });
    }

    private BlobDownloadAsyncResponse downloadResponse(long offset, int length) {
        HttpHeaders headers = new HttpHeaders()
                .set(
                        HttpHeaderName.CONTENT_RANGE,
                        "bytes " + offset + "-" + (offset + length - 1) + "/" + CONTENT_LENGTH);
        Flux<ByteBuffer> body = Flux.fromIterable(chunks((int) offset, length + extraBytes));
        return new BlobDownloadAsyncResponse(null, 206, headers, body, null);
    }

    private static List<ByteBuffer> chunks(int from, int length) {
        List<ByteBuffer> chunks = new ArrayList<>();
        for (int start = 0; start < length; start += CHUNK_SIZE) {
            int chunkLength = Math.min(CHUNK_SIZE, length - start);
            chunks.add(ByteBuffer.wrap(TEST_DATA, from + start, chunkLength).slice());
        }
        return chunks;
    }

    /**
     * Builds a real {@link BlobStorageException} wired to the given status. This flows through
     * {@link AzureExceptionMapper#map} as a live Azure error would: the mapper reads the status from
     * {@code getResponse().getStatusCode()}, which a bare mock of {@code BlobStorageException} itself would leave null.
     */
    private static BlobStorageException blobException(int status, String message) {
        HttpResponse response = mock(HttpResponse.class);
        lenient().when(response.getStatusCode()).thenReturn(status);
        return new BlobStorageException(message, response, null);
    }

    /** Stubs the properties response consumed by {@code size()}. */
    private void stubProperties() {
        when(asyncClient.getProperties()).thenReturn(Mono.just(blobProperties));
        when(blobProperties.getBlobSize()).thenReturn((long) CONTENT_LENGTH);
    }

    private static byte[] contents(ByteBuffer buffer, int from, int length) {
        byte[] out = new byte[length];
        buffer.get(from, out);
        return out;
    }

    @Test
    void testConstructorMakesNoRequests() {
        newReader();
        verifyNoInteractions(blobClient, asyncClient);
    }

    @Test
    void testGetSize() {
        stubProperties();

        assertThat(reader.size()).hasValue(CONTENT_LENGTH);
        verify(asyncClient, times(1)).getProperties();
    }

    @Test
    void testSizeIsLazyAndMemoized() {
        stubProperties();
        AzureBlobRangeReader lazy = newReader();
        verify(asyncClient, never()).getProperties();

        assertThat(lazy.size()).hasValue(CONTENT_LENGTH);
        assertThat(lazy.size()).hasValue(CONTENT_LENGTH);
        verify(asyncClient, times(1)).getProperties();
    }

    @Test
    void testSizeCapturedFromRangeResponseWithoutProperties() {
        reader.readRange(0, 10);

        assertThat(reader.size()).hasValue(CONTENT_LENGTH);
        verify(asyncClient, never()).getProperties();
    }

    @Test
    void aMissingBlobFailsTheSizeRequestWithNotFound() {
        doReturn(Mono.error(blobException(404, "BlobNotFound")))
                .when(asyncClient)
                .getProperties();

        assertThatThrownBy(() -> reader.size()).isInstanceOf(NotFoundException.class);
    }

    @Test
    void aSizeRequestAnsweredWithoutPropertiesFailsWithAStorageException() {
        when(asyncClient.getProperties()).thenReturn(Mono.empty());

        assertThatThrownBy(() -> reader.size())
                .isInstanceOf(StorageException.class)
                .hasMessageContaining("no properties returned")
                .hasNoCause();
    }

    @Test
    void testNotFoundThrownOnFirstReadNotAtConstruction() {
        AzureBlobRangeReader missing = newReader();
        doReturn(Mono.error(blobException(404, "BlobNotFound")))
                .when(asyncClient)
                .downloadStreamWithResponse(any(), any(), any(), anyBoolean());

        assertThatThrownBy(() -> missing.readRange(0, 10)).isInstanceOf(NotFoundException.class);
    }

    @Test
    void testReadEntireFile() {
        ByteBuffer buffer = reader.readRange(0, CONTENT_LENGTH).flip();

        assertThat(buffer.remaining()).isEqualTo(CONTENT_LENGTH);
        assertThat(contents(buffer, 0, CONTENT_LENGTH)).containsExactly(TEST_DATA);
    }

    @Test
    void testReadRange() {
        int offset = 100;
        int length = 500;

        ByteBuffer buffer = reader.readRange(offset, length).flip();

        assertThat(buffer.remaining()).isEqualTo(length);
        assertThat(contents(buffer, 0, length)).containsExactly(Arrays.copyOfRange(TEST_DATA, offset, offset + length));
    }

    @Test
    void testReadRangeBeyondEnd() {
        int offset = CONTENT_LENGTH - 200;
        int length = 500;

        ByteBuffer buffer = reader.readRange(offset, length).flip();

        // The server, not the client, truncates the response to the available data
        assertThat(buffer.remaining()).isEqualTo(200);
        assertThat(contents(buffer, 0, 200)).containsExactly(Arrays.copyOfRange(TEST_DATA, offset, CONTENT_LENGTH));
    }

    @Test
    void bodyStreamsIntoADirectTargetAtItsPosition() {
        ByteBuffer target = ByteBuffer.allocateDirect(700);
        target.position(50);

        int read = reader.readRange(100, 500, target);

        assertThat(read).isEqualTo(500);
        assertThat(target.position()).isEqualTo(550);
        assertThat(target.limit()).isEqualTo(700);
        assertThat(contents(target, 50, 500)).containsExactly(Arrays.copyOfRange(TEST_DATA, 100, 600));
    }

    /** A read waiting for the body under a deadline would park in a timed wait. */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aRangeDownloadHasNoDeadlineForTheWholeBody() throws InterruptedException {
        doReturn(Mono.never()).when(asyncClient).downloadStreamWithResponse(any(), any(), any(), anyBoolean());
        FutureTask<ByteBuffer> read = new FutureTask<>(() -> reader.readRange(0, 100));
        Thread readingThread = new Thread(read, "never-answered-read");

        readingThread.start();
        try {
            await().atMost(Duration.ofSeconds(10)).until(() -> isParked(readingThread));
            assertThat(readingThread.getState()).isEqualTo(Thread.State.WAITING);

            readingThread.interrupt();
            assertThatThrownBy(() -> read.get(10, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(StorageException.class);
        } finally {
            readingThread.interrupt();
            readingThread.join(TimeUnit.SECONDS.toMillis(10));
        }
    }

    private static boolean isParked(Thread thread) {
        Thread.State state = thread.getState();
        return state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING;
    }

    /**
     * A transport thread may deliver a chunk after the interrupt has cancelled the download. The read has thrown by
     * then, and the target belongs to the caller again.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aChunkArrivingAfterAnInterruptedReadThrewLeavesTheTargetUntouched() throws Exception {
        LateBody body = new LateBody();
        BlobDownloadAsyncResponse response =
                new BlobDownloadAsyncResponse(null, 206, new HttpHeaders(), Flux.from(body), null);
        doReturn(Mono.just(response)).when(asyncClient).downloadStreamWithResponse(any(), any(), any(), anyBoolean());
        ByteBuffer target = ByteBuffer.allocate(100);
        FutureTask<Integer> read = new FutureTask<>(() -> reader.readRange(0, 100, target));
        Thread readingThread = new Thread(read, "read-with-a-late-chunk");

        readingThread.start();
        try {
            body.awaitSubscription();
            readingThread.interrupt();
            assertThatThrownBy(() -> read.get(10, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(StorageException.class);
            assertThat(body.isCancelled())
                    .as("download cancelled by the interrupt")
                    .isTrue();

            body.deliver(ByteBuffer.wrap(TEST_DATA, 0, 100));

            assertThat(target.position())
                    .as("target position after the late chunk")
                    .isZero();
            assertThat(contents(target, 0, 100))
                    .as("target bytes after the late chunk")
                    .containsOnly(0);
        } finally {
            readingThread.interrupt();
            readingThread.join(TimeUnit.SECONDS.toMillis(10));
        }
    }

    /**
     * A response body delivering its chunk only when the test calls {@link #deliver}, even after a cancel, as a
     * transport thread already past its cancel check does.
     */
    private static final class LateBody implements Publisher<ByteBuffer>, Subscription {

        private final CountDownLatch subscribed = new CountDownLatch(1);
        private final AtomicBoolean cancelled = new AtomicBoolean();
        private volatile Subscriber<? super ByteBuffer> subscriber;

        @Override
        public void subscribe(Subscriber<? super ByteBuffer> bodySubscriber) {
            subscriber = bodySubscriber;
            bodySubscriber.onSubscribe(this);
            subscribed.countDown();
        }

        @Override
        public void request(long n) {
            // chunks go out only through deliver()
        }

        @Override
        public void cancel() {
            cancelled.set(true);
        }

        void awaitSubscription() throws InterruptedException {
            assertThat(subscribed.await(10, TimeUnit.SECONDS))
                    .as("body subscribed by the read")
                    .isTrue();
        }

        boolean isCancelled() {
            return cancelled.get();
        }

        void deliver(ByteBuffer chunk) {
            subscriber.onNext(chunk);
        }
    }

    @Test
    void testReadZeroLength() {
        ByteBuffer buffer = reader.readRange(100, 0).flip();

        assertThat(buffer.remaining()).isZero();
        verify(asyncClient, never()).downloadStreamWithResponse(any(), any(), any(), anyBoolean());
    }

    @Test
    void testReadWithNegativeOffset() {
        assertThatThrownBy(() -> reader.readRange(-1, 10)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testReadWithNegativeLength() {
        assertThatThrownBy(() -> reader.readRange(0, -1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testReadOffsetBeyondEnd() {
        ByteBuffer target = ByteBuffer.allocate(10);

        int read = reader.readRange(CONTENT_LENGTH + 100, 10, target);

        assertThat(read).isZero();
        assertThat(target.position()).isZero();
    }

    @Test
    void moreDataThanRequestedIsAStorageError() {
        extraBytes = 50;

        assertThatThrownBy(() -> reader.readRange(0, 100))
                .isInstanceOf(StorageException.class)
                .hasMessageContaining("more data than requested");
    }

    @Test
    void downloadFailuresMapToStorageException() {
        doReturn(Mono.error(new IllegalStateException("socket closed")))
                .when(asyncClient)
                .downloadStreamWithResponse(any(), any(), any(), anyBoolean());

        assertThatThrownBy(() -> reader.readRange(0, 100))
                .isInstanceOf(StorageException.class)
                .isNotInstanceOf(NotFoundException.class)
                .hasMessageContaining("socket closed");
    }

    @Test
    void batchedReadsUseTheObjectStorePolicyOnTheSharedExecutor() {
        BlobClient anonymousClient = new BlobClientBuilder()
                .endpoint("https://account.blob.core.windows.net")
                .containerName("container")
                .blobName("blob.bin")
                .buildClient();
        AzureBlobRangeReader plainReader = new AzureBlobRangeReader(anonymousClient);
        assertThat(plainReader.coalescingPolicy()).isEqualTo(CoalescingPolicy.objectStoreDefaults());
        assertThat(plainReader.maxConcurrentFetches()).isEqualTo(8);
    }
}
