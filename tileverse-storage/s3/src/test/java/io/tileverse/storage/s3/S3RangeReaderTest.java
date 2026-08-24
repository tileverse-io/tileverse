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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.tileverse.storage.NotFoundException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;
import java.util.OptionalLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

// Use LENIENT strictness to avoid unnecessary stubbing errors in tests
@ExtendWith(MockitoExtension.class)
class S3RangeReaderTest {

    private static final String BUCKET = "test-bucket";
    private static final String KEY = "test.pmtiles";
    private static final int CONTENT_LENGTH = 10000;
    private static final byte[] TEST_DATA = createTestData(CONTENT_LENGTH);

    @Mock
    private S3Client s3Client;

    @Mock
    private HeadObjectResponse headObjectResponse;

    @Mock
    private GetObjectResponse getObjectResponse;

    @Mock
    private ResponseBytes<GetObjectResponse> responseBytes;

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
        // Setup mock behavior for GET request - explicitly use GetObjectRequest overload
        lenient()
                .when(s3Client.getObjectAsBytes((GetObjectRequest) any(GetObjectRequest.class)))
                .thenReturn(responseBytes);
        lenient().when(responseBytes.response()).thenReturn(getObjectResponse);

        // Setup default response for full object
        lenient().when(getObjectResponse.contentLength()).thenReturn((long) CONTENT_LENGTH);

        // Setup mock data for response
        lenient().when(responseBytes.asByteArray()).thenAnswer(invocation -> {
            // Get the request to determine the range
            GetObjectRequest capturedRequest = mock(GetObjectRequest.class);
            if (mockingDetails(s3Client).getInvocations().size() > 0) {
                capturedRequest = (GetObjectRequest) mockingDetails(s3Client).getInvocations().stream()
                        .filter(i -> i.getMethod().getName().equals("getObjectAsBytes"))
                        .findFirst()
                        .get()
                        .getArgument(0);
            }

            String rangeHeader = capturedRequest.range();
            if (rangeHeader == null) {
                return TEST_DATA;
            }

            // Parse the range header
            String[] rangeParts = rangeHeader.replace("bytes=", "").split("-");
            int start = Integer.parseInt(rangeParts[0]);
            int end = Integer.parseInt(rangeParts[1]);
            int length = end - start + 1;

            // A real object store truncates a range that extends past the object's actual end
            // instead of padding it. The mock matches that behavior.
            int availableEnd = Math.min(start + length, TEST_DATA.length);
            return Arrays.copyOfRange(TEST_DATA, start, availableEnd);
        });

        // Create the reader directly via the package-private constructor.
        // The Builder is gone in 2.0; tests in this package construct readers via the
        // package-private constructor (production paths use Storage.openRangeReader).
        reader = new S3RangeReader(s3Client, new S3Reference(null, BUCKET, KEY, null), false);
    }

    /** Stubs the HEAD response used by {@code size()}. Called only by tests that exercise size(). */
    private void stubHeadObject() {
        lenient().when(s3Client.headObject(any(HeadObjectRequest.class))).thenReturn(headObjectResponse);
        lenient().when(headObjectResponse.contentLength()).thenReturn((long) CONTENT_LENGTH);
        lenient().when(headObjectResponse.lastModified()).thenReturn(Instant.EPOCH);
    }

    @Test
    void testConstructorMakesNoRequests() {
        new S3RangeReader(s3Client, new S3Reference(null, BUCKET, KEY, null), false);
        verifyNoInteractions(s3Client);
    }

    @Test
    void testGetSize() {
        stubHeadObject();
        assertEquals(CONTENT_LENGTH, reader.size().getAsLong());
        verify(s3Client, times(1)).headObject(any(HeadObjectRequest.class));
    }

    @Test
    void testSizeIsLazyAndMemoized() {
        stubHeadObject();
        S3RangeReader lazy = new S3RangeReader(s3Client, new S3Reference(null, BUCKET, KEY, null), false);
        verify(s3Client, never()).headObject((HeadObjectRequest) any(HeadObjectRequest.class));

        assertEquals(OptionalLong.of(CONTENT_LENGTH), lazy.size());
        assertEquals(OptionalLong.of(CONTENT_LENGTH), lazy.size());
        verify(s3Client, times(1)).headObject((HeadObjectRequest) any(HeadObjectRequest.class));
    }

    @Test
    void testSizeThrowsNotFoundForMissingKey() {
        S3RangeReader missing = new S3RangeReader(s3Client, new S3Reference(null, BUCKET, "missing", null), false);
        when(s3Client.headObject((HeadObjectRequest) any(HeadObjectRequest.class)))
                .thenThrow(NoSuchKeyException.builder().message("no such key").build());

        assertThatThrownBy(missing::size).isInstanceOf(NotFoundException.class);
    }

    @Test
    void testSizeCapturedFromRangeResponseWithoutHead() {
        byte[] slice = Arrays.copyOfRange(TEST_DATA, 0, 10);
        GetObjectResponse response = GetObjectResponse.builder()
                .contentLength(10L)
                .contentRange("bytes 0-9/" + CONTENT_LENGTH)
                .build();
        when(s3Client.getObjectAsBytes((GetObjectRequest) any(GetObjectRequest.class)))
                .thenReturn(ResponseBytes.fromByteArray(response, slice));

        S3RangeReader lazy = new S3RangeReader(s3Client, new S3Reference(null, BUCKET, KEY, null), false);
        lazy.readRange(0, 10);

        assertEquals(OptionalLong.of(CONTENT_LENGTH), lazy.size());
        verify(s3Client, never()).headObject((HeadObjectRequest) any(HeadObjectRequest.class));
    }

    @Test
    void testNotFoundThrownOnFirstReadNotAtConstruction() {
        S3RangeReader missing = new S3RangeReader(s3Client, new S3Reference(null, BUCKET, "missing", null), false);
        when(s3Client.getObjectAsBytes((GetObjectRequest) any(GetObjectRequest.class)))
                .thenThrow(NoSuchKeyException.builder().message("no such key").build());

        assertThatThrownBy(() -> missing.readRange(0, 10)).isInstanceOf(NotFoundException.class);
    }

    @Test
    void testReadEntireFile() {
        ByteBuffer buffer = reader.readRange(0, CONTENT_LENGTH);
        buffer.flip();

        assertEquals(CONTENT_LENGTH, buffer.remaining());

        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);

        assertArrayEquals(TEST_DATA, bytes);

        verify(s3Client).getObjectAsBytes((GetObjectRequest) argThat(request -> request instanceof GetObjectRequest
                && ((GetObjectRequest) request).range().equals("bytes=0-" + (CONTENT_LENGTH - 1))));
    }

    @Test
    void testReadRange() {
        int offset = 100;
        int length = 500;

        ByteBuffer buffer = reader.readRange(offset, length);
        buffer.flip();

        assertEquals(length, buffer.remaining());

        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);

        byte[] expectedBytes = Arrays.copyOfRange(TEST_DATA, offset, offset + length);
        assertArrayEquals(expectedBytes, bytes);

        verify(s3Client).getObjectAsBytes((GetObjectRequest) argThat(request -> request instanceof GetObjectRequest
                && ((GetObjectRequest) request).range().equals("bytes=" + offset + "-" + (offset + length - 1))));
    }

    @Test
    void testReadRangeBeyondEnd() {
        int offset = CONTENT_LENGTH - 200;
        int length = 500; // Beyond the end

        ByteBuffer buffer = reader.readRange(offset, length);
        buffer.flip();

        // The server, not the client, truncates the response to the available data
        assertEquals(200, buffer.remaining());

        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);

        byte[] expectedBytes = Arrays.copyOfRange(TEST_DATA, offset, CONTENT_LENGTH);
        assertArrayEquals(expectedBytes, bytes);

        // The client requests the full range as asked; it does not pre-truncate at EOF
        verify(s3Client).getObjectAsBytes((GetObjectRequest) argThat(request -> request instanceof GetObjectRequest
                && ((GetObjectRequest) request).range().equals("bytes=" + offset + "-" + (offset + length - 1))));
    }

    @Test
    void testReadZeroLength() {
        ByteBuffer buffer = reader.readRange(100, 0);
        buffer.flip();

        assertEquals(0, buffer.remaining());

        // Should not make any API calls for zero length
        verify(s3Client, never()).getObjectAsBytes((GetObjectRequest) any(GetObjectRequest.class));
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
    void testS3ExceptionDuringRead() {
        // Override the default behavior for this specific test
        when(s3Client.getObjectAsBytes((GetObjectRequest) any(GetObjectRequest.class)))
                .thenThrow(SdkException.builder().message("S3 download error").build());

        assertThatThrownBy(() -> reader.readRange(0, 100)).isInstanceOf(io.tileverse.storage.StorageException.class);
    }

    @Test
    void testS3ReturnsMoreThanRequested() {
        // A server that ignores the requested range and returns more data than asked for is a
        // storage error, not an EOF condition.
        byte[] tooMuch = Arrays.copyOfRange(TEST_DATA, 0, 150);
        GetObjectResponse oversizedResponse =
                GetObjectResponse.builder().contentLength((long) tooMuch.length).build();
        when(s3Client.getObjectAsBytes((GetObjectRequest) any(GetObjectRequest.class)))
                .thenReturn(ResponseBytes.fromByteArray(oversizedResponse, tooMuch));

        assertThatThrownBy(() -> reader.readRange(0, 100)).isInstanceOf(io.tileverse.storage.StorageException.class);
    }

    @Test
    void testGetSourceIdentifier() {
        assertEquals("s3://%s/%s".formatted(BUCKET, KEY), reader.getSourceIdentifier());
    }

    @Test
    void testReadStraddlingEofReturnsShortCount() {
        // The server answers a range that runs past EOF with only the available bytes.
        byte[] available = Arrays.copyOfRange(TEST_DATA, CONTENT_LENGTH - 10, CONTENT_LENGTH);
        GetObjectResponse shortResponse = GetObjectResponse.builder()
                .contentLength((long) available.length)
                .build();
        when(s3Client.getObjectAsBytes((GetObjectRequest) any(GetObjectRequest.class)))
                .thenReturn(ResponseBytes.fromByteArray(shortResponse, available));

        ByteBuffer target = ByteBuffer.allocate(100);
        int read = reader.readRange(CONTENT_LENGTH - 10, 100, target);

        assertEquals(10, read);
    }

    @Test
    void testReadPastEofReturnsZero() {
        AwsServiceException invalidRange = S3Exception.builder()
                .statusCode(416)
                .message("Requested Range Not Satisfiable")
                .build();
        when(s3Client.getObjectAsBytes((GetObjectRequest) any(GetObjectRequest.class)))
                .thenThrow(invalidRange);

        ByteBuffer target = ByteBuffer.allocate(100);
        int read = reader.readRange(CONTENT_LENGTH + 5, 100, target);

        assertEquals(0, read);
        assertEquals(0, target.position());
    }
}
