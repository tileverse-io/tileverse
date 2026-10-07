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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.tileverse.storage.NotFoundException;
import io.tileverse.storage.StorageException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

/**
 * Unit tests for the single-range path of {@link S3RangeReader}: a body streams through the async client's
 * {@link AsyncResponseTransformer} straight into the caller's buffer, and nothing writes that buffer once the read
 * returned or threw.
 */
@ExtendWith(MockitoExtension.class)
class S3RangeReaderTest {

    private static final String BUCKET = "test-bucket";
    private static final String KEY = "test.pmtiles";
    private static final int CONTENT_LENGTH = 10000;
    /** Small enough that every read of interest arrives in many chunks. */
    private static final int CHUNK_SIZE = 64;

    private static final byte[] TEST_DATA = createTestData(CONTENT_LENGTH);

    @Mock
    private S3AsyncClient client;

    @Mock
    private HeadObjectResponse headObjectResponse;

    private final ExecutorService caller = Executors.newSingleThreadExecutor();

    private S3ObjectStub object;
    private S3RangeReader reader;

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
        object = new S3ObjectStub(TEST_DATA, CHUNK_SIZE);
        object.installAsync(client);
        reader = newReader(KEY);
    }

    @AfterEach
    void stopCaller() {
        caller.shutdownNow();
    }

    private S3RangeReader newReader(String key) {
        return new S3RangeReader(client, new S3Reference(null, BUCKET, key, null), false);
    }

    /** Stubs the HEAD response used by {@code size()}. Called only by tests that exercise size(). */
    private void stubHeadObject() {
        lenient()
                .when(client.headObject(any(HeadObjectRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(headObjectResponse));
        lenient().when(headObjectResponse.contentLength()).thenReturn((long) CONTENT_LENGTH);
        lenient().when(headObjectResponse.lastModified()).thenReturn(Instant.EPOCH);
    }

    /** Matches the GET for exactly the given range. */
    private static GetObjectRequest requestForRange(long offset, int length) {
        String range = "bytes=" + offset + "-" + (offset + length - 1);
        return argThat(request -> range.equals(request.range()));
    }

    /** The range went out as one streamed GET. */
    @SuppressWarnings("unchecked")
    private void verifyStreamedGet(long offset, int length) {
        verify(client).getObject(requestForRange(offset, length), any(AsyncResponseTransformer.class));
    }

    private static byte[] contents(ByteBuffer buffer, int from, int length) {
        byte[] out = new byte[length];
        buffer.get(from, out);
        return out;
    }

    @Test
    void testConstructorMakesNoRequests() {
        newReader(KEY);
        verifyNoInteractions(client);
    }

    @Test
    void testGetSize() {
        stubHeadObject();
        assertThat(reader.size()).hasValue(CONTENT_LENGTH);
        verify(client, times(1)).headObject(any(HeadObjectRequest.class));
    }

    @Test
    void testSizeIsLazyAndMemoized() {
        stubHeadObject();
        S3RangeReader lazy = newReader(KEY);
        verify(client, never()).headObject(any(HeadObjectRequest.class));

        assertThat(lazy.size()).hasValue(CONTENT_LENGTH);
        assertThat(lazy.size()).hasValue(CONTENT_LENGTH);
        verify(client, times(1)).headObject(any(HeadObjectRequest.class));
    }

    @Test
    void testSizeThrowsNotFoundForMissingKey() {
        S3RangeReader missing = newReader("missing");
        when(client.headObject(any(HeadObjectRequest.class)))
                .thenReturn(CompletableFuture.failedFuture(
                        NoSuchKeyException.builder().message("no such key").build()));

        assertThatThrownBy(missing::size).isInstanceOf(NotFoundException.class);
    }

    @Test
    void testSizeCapturedFromRangeResponseWithoutHead() {
        S3RangeReader lazy = newReader(KEY);
        lazy.readRange(0, 10);

        assertThat(lazy.size()).hasValue(CONTENT_LENGTH);
        verify(client, never()).headObject(any(HeadObjectRequest.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testNotFoundThrownOnFirstReadNotAtConstruction() {
        S3RangeReader missing = newReader("missing");
        when(client.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
                .thenReturn(CompletableFuture.failedFuture(
                        NoSuchKeyException.builder().message("no such key").build()));

        assertThatThrownBy(() -> missing.readRange(0, 10)).isInstanceOf(NotFoundException.class);
    }

    @Test
    void testReadEntireFile() {
        ByteBuffer buffer = reader.readRange(0, CONTENT_LENGTH).flip();

        assertThat(buffer.remaining()).isEqualTo(CONTENT_LENGTH);
        assertThat(contents(buffer, 0, CONTENT_LENGTH)).containsExactly(TEST_DATA);
        verifyStreamedGet(0, CONTENT_LENGTH);
    }

    @Test
    void testReadRange() {
        int offset = 100;
        int length = 500;

        ByteBuffer buffer = reader.readRange(offset, length).flip();

        assertThat(buffer.remaining()).isEqualTo(length);
        assertThat(contents(buffer, 0, length)).containsExactly(Arrays.copyOfRange(TEST_DATA, offset, offset + length));
        verifyStreamedGet(offset, length);
    }

    @Test
    void testReadRangeBeyondEnd() {
        int offset = CONTENT_LENGTH - 200;
        int length = 500;

        ByteBuffer buffer = reader.readRange(offset, length).flip();

        // The server, not the client, truncates the response to the available data
        assertThat(buffer.remaining()).isEqualTo(200);
        assertThat(contents(buffer, 0, 200)).containsExactly(Arrays.copyOfRange(TEST_DATA, offset, CONTENT_LENGTH));
        // The client requests the full range as asked; it does not pre-truncate at EOF
        verifyStreamedGet(offset, length);
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

    @Test
    void aDroppedBodyIsRetriedFromTheTargetStartPosition() {
        object.failingBodyReads(1);
        ByteBuffer target = ByteBuffer.allocate(400);
        target.position(10);

        int read = reader.readRange(0, 300, target);

        assertThat(read).isEqualTo(300);
        assertThat(object.attempts()).isEqualTo(2);
        assertThat(target.position()).isEqualTo(310);
        assertThat(contents(target, 10, 300)).containsExactly(Arrays.copyOfRange(TEST_DATA, 0, 300));
    }

    @Test
    void aBodyDroppedOnEveryAttemptFailsTheRead() {
        object.failingBodyReads(3);

        assertThatThrownBy(() -> reader.readRange(0, 300)).isInstanceOf(StorageException.class);
        assertThat(object.attempts()).isEqualTo(3);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testReadZeroLength() {
        ByteBuffer buffer = reader.readRange(100, 0).flip();

        assertThat(buffer.remaining()).isZero();
        verify(client, never()).getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class));
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
    @SuppressWarnings("unchecked")
    void testS3ExceptionDuringRead() {
        when(client.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
                .thenReturn(CompletableFuture.failedFuture(
                        SdkException.builder().message("S3 download error").build()));

        assertThatThrownBy(() -> reader.readRange(0, 100)).isInstanceOf(StorageException.class);
    }

    @Test
    void testS3ReturnsMoreThanRequested() {
        // A server ignoring the requested range sends more than asked for: a storage error, not an EOF
        // condition, and the body is cancelled instead of drained.
        object.respondingWithExtraBytes(50);

        assertThatThrownBy(() -> reader.readRange(0, 100))
                .isInstanceOf(StorageException.class)
                .hasMessageContaining("more data than requested");
        assertThat(object.cancellations()).isEqualTo(1);
    }

    @Test
    void testGetSourceIdentifier() {
        assertThat(reader.getSourceIdentifier()).isEqualTo("s3://%s/%s".formatted(BUCKET, KEY));
    }

    @Test
    void testReadStraddlingEofReturnsShortCount() {
        ByteBuffer target = ByteBuffer.allocate(100);

        int read = reader.readRange(CONTENT_LENGTH - 10, 100, target);

        assertThat(read).isEqualTo(10);
        assertThat(target.position()).isEqualTo(10);
    }

    @Test
    void testReadPastEofReturnsZero() {
        ByteBuffer target = ByteBuffer.allocate(100);

        int read = reader.readRange(CONTENT_LENGTH + 5, 100, target);

        assertThat(read).isZero();
        assertThat(target.position()).isZero();
    }

    /**
     * The SDK's retry stage starts a scheduled attempt without checking whether the call already failed. A retry
     * scheduled before the API call timeout can stream its body after the read throws, and that body must not reach the
     * caller's target.
     */
    @Test
    void aReadTimingOutWritesNothingToTheTargetAfterItThrows() {
        TimedOutCall call = new TimedOutCall(client);
        ByteBuffer target = ByteBuffer.allocate(100);

        assertThatThrownBy(() -> reader.readRange(0, 100, target)).isInstanceOf(StorageException.class);
        call.startTheScheduledRetry(100);

        assertThat(contents(target, 0, 100)).containsOnly((byte) 0);
        assertThat(target.position()).isZero();
    }

    @Test
    void anInterruptedReadFailsAndLeavesTheTargetToTheCaller() throws Exception {
        object.holdingAsyncResponses();
        ByteBuffer target = ByteBuffer.allocate(300);
        AtomicReference<Thread> reading = new AtomicReference<>();
        Future<Throwable> outcome = caller.submit(() -> {
            reading.set(Thread.currentThread());
            try {
                reader.readRange(0, 300, target);
                return null;
            } catch (StorageException failure) {
                return Thread.interrupted() ? failure : new AssertionError("interrupt flag cleared", failure);
            }
        });
        await().atMost(Duration.ofSeconds(5)).until(() -> object.heldResponses() == 1);

        reading.get().interrupt();
        Throwable failure = outcome.get(5, TimeUnit.SECONDS);
        object.releaseAll();

        assertThat(failure).isInstanceOf(StorageException.class).hasCauseInstanceOf(InterruptedIOException.class);
        assertThat(target.position()).isZero();
        assertThat(contents(target, 0, 300)).containsOnly((byte) 0);
    }

    @Test
    @SuppressWarnings("unchecked")
    void aReadByAnInterruptedThreadSendsNoRequest() throws Exception {
        Future<Throwable> outcome = caller.submit(() -> {
            Thread.currentThread().interrupt();
            try {
                reader.readRange(0, 100);
                return null;
            } catch (StorageException failure) {
                Thread.interrupted();
                return failure;
            }
        });

        assertThat(outcome.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(InterruptedIOException.class);
        verify(client, never()).getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class));
    }
}
