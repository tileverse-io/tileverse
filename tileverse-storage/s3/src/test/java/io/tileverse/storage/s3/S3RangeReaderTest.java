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
import io.tileverse.storage.adapters.ByteBufferSinkException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

/**
 * Unit tests for the single-range path of {@link S3RangeReader}: bodies stream through the SDK's
 * {@link ResponseTransformer} straight into the caller's buffer.
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
    private S3Client s3Client;

    @Mock
    private HeadObjectResponse headObjectResponse;

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
        object.installSync(s3Client);
        reader = newReader(KEY);
    }

    private S3RangeReader newReader(String key) {
        return new S3RangeReader(s3Client, new S3Reference(null, BUCKET, key, null), false);
    }

    /** Stubs the HEAD response used by {@code size()}. Called only by tests that exercise size(). */
    private void stubHeadObject() {
        lenient().when(s3Client.headObject(any(HeadObjectRequest.class))).thenReturn(headObjectResponse);
        lenient().when(headObjectResponse.contentLength()).thenReturn((long) CONTENT_LENGTH);
        lenient().when(headObjectResponse.lastModified()).thenReturn(Instant.EPOCH);
    }

    /** Matches the GET for exactly the given range. */
    private static GetObjectRequest requestForRange(long offset, int length) {
        String range = "bytes=" + offset + "-" + (offset + length - 1);
        return argThat(request -> range.equals(request.range()));
    }

    /** The range went out as one streamed GET and never through the byte-array entry point. */
    @SuppressWarnings("unchecked")
    private void verifyStreamedGet(long offset, int length) {
        verify(s3Client).getObject(requestForRange(offset, length), any(ResponseTransformer.class));
        verify(s3Client, never()).getObjectAsBytes(any(GetObjectRequest.class));
    }

    private static byte[] contents(ByteBuffer buffer, int from, int length) {
        byte[] out = new byte[length];
        buffer.get(from, out);
        return out;
    }

    @Test
    void testConstructorMakesNoRequests() {
        newReader(KEY);
        verifyNoInteractions(s3Client);
    }

    @Test
    void testGetSize() {
        stubHeadObject();
        assertThat(reader.size()).hasValue(CONTENT_LENGTH);
        verify(s3Client, times(1)).headObject(any(HeadObjectRequest.class));
    }

    @Test
    void testSizeIsLazyAndMemoized() {
        stubHeadObject();
        S3RangeReader lazy = newReader(KEY);
        verify(s3Client, never()).headObject(any(HeadObjectRequest.class));

        assertThat(lazy.size()).hasValue(CONTENT_LENGTH);
        assertThat(lazy.size()).hasValue(CONTENT_LENGTH);
        verify(s3Client, times(1)).headObject(any(HeadObjectRequest.class));
    }

    @Test
    void testSizeThrowsNotFoundForMissingKey() {
        S3RangeReader missing = newReader("missing");
        when(s3Client.headObject(any(HeadObjectRequest.class)))
                .thenThrow(NoSuchKeyException.builder().message("no such key").build());

        assertThatThrownBy(missing::size).isInstanceOf(NotFoundException.class);
    }

    @Test
    void testSizeCapturedFromRangeResponseWithoutHead() {
        S3RangeReader lazy = newReader(KEY);
        lazy.readRange(0, 10);

        assertThat(lazy.size()).hasValue(CONTENT_LENGTH);
        verify(s3Client, never()).headObject(any(HeadObjectRequest.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testNotFoundThrownOnFirstReadNotAtConstruction() {
        S3RangeReader missing = newReader("missing");
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class)))
                .thenThrow(NoSuchKeyException.builder().message("no such key").build());

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
        verify(s3Client, never()).getObject(any(GetObjectRequest.class), any(ResponseTransformer.class));
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
        when(s3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class)))
                .thenThrow(SdkException.builder().message("S3 download error").build());

        assertThatThrownBy(() -> reader.readRange(0, 100)).isInstanceOf(StorageException.class);
    }

    @Test
    void testS3ReturnsMoreThanRequested() {
        // A server that ignores the requested range and returns more data than asked for is a
        // storage error, not an EOF condition; the connection is aborted instead of drained.
        object.respondingWithExtraBytes(50);

        // The SDK wraps the transformer's failure in its own SdkException before it reaches the
        // reader; the reader unwraps that SdkException and rethrows the original StorageException
        // as-is, its cause still the sink's failure, instead of wrapping it a second time.
        assertThatThrownBy(() -> reader.readRange(0, 100))
                .isInstanceOf(StorageException.class)
                .hasMessageContaining("more data than requested")
                .hasCauseInstanceOf(ByteBufferSinkException.class);
        assertThat(object.aborts()).isEqualTo(1);
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
}
