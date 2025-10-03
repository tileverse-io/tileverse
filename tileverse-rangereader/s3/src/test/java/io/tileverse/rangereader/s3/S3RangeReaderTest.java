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
package io.tileverse.rangereader.s3;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

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

    /**
     * Creates test data with a predictable pattern.
     */
    private static byte[] createTestData(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) (i % 256);
        }
        return data;
    }

    @BeforeEach
    void setUp() throws IOException {
        // Make mocks lenient for this test class to avoid unnecessary stubbing errors
        lenient().when(s3Client.headObject(any(HeadObjectRequest.class))).thenReturn(headObjectResponse);
        lenient().when(headObjectResponse.contentLength()).thenReturn((long) CONTENT_LENGTH);

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

            // Return the requested range
            return Arrays.copyOfRange(TEST_DATA, start, start + length);
        });

        // Create the reader
        reader = S3RangeReader.builder()
                .s3Client(s3Client)
                .bucket(BUCKET)
                .key(KEY)
                .build();
    }

    @Test
    void testGetSize() throws IOException {
        assertEquals(CONTENT_LENGTH, reader.size().getAsLong());
        verify(s3Client, times(1)).headObject(any(HeadObjectRequest.class));
    }

    @Test
    void testReadEntireFile() throws IOException {
        when(getObjectResponse.contentLength()).thenReturn((long) CONTENT_LENGTH);

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
    void testReadRange() throws IOException {
        int offset = 100;
        int length = 500;
        when(getObjectResponse.contentLength()).thenReturn((long) length);

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
    void testReadRangeBeyondEnd() throws IOException {
        int offset = CONTENT_LENGTH - 200;
        int length = 500; // Beyond the end
        when(getObjectResponse.contentLength()).thenReturn(200L);

        ByteBuffer buffer = reader.readRange(offset, length);
        buffer.flip();

        // Should only return up to the end of the file
        assertEquals(200, buffer.remaining());

        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);

        byte[] expectedBytes = Arrays.copyOfRange(TEST_DATA, offset, CONTENT_LENGTH);
        assertArrayEquals(expectedBytes, bytes);

        verify(s3Client).getObjectAsBytes((GetObjectRequest) argThat(request -> request instanceof GetObjectRequest
                && ((GetObjectRequest) request).range().equals("bytes=" + offset + "-" + (CONTENT_LENGTH - 1))));
    }

    @Test
    void testReadZeroLength() throws IOException {
        ByteBuffer buffer = reader.readRange(100, 0);
        buffer.flip();

        assertEquals(0, buffer.remaining());

        // Should not make any API calls for zero length
        verify(s3Client, never()).getObjectAsBytes((GetObjectRequest) any(GetObjectRequest.class));
    }

    @Test
    void testReadWithNegativeOffset() {
        assertThrows(IllegalArgumentException.class, () -> reader.readRange(-1, 10));
    }

    @Test
    void testReadWithNegativeLength() {
        assertThrows(IllegalArgumentException.class, () -> reader.readRange(0, -1));
    }

    @Test
    void testObjectNotExists() {
        // Override the default behavior for this specific test
        when(s3Client.headObject(any(HeadObjectRequest.class)))
                .thenThrow(NoSuchKeyException.builder()
                        .message("Key does not exist")
                        .build());

        assertThrows(IOException.class, () -> S3RangeReader.builder()
                .s3Client(s3Client)
                .bucket(BUCKET)
                .key(KEY)
                .build());
    }

    @Test
    void testS3ExceptionDuringConstruction() {
        // Override the default behavior for this specific test
        when(s3Client.headObject(any(HeadObjectRequest.class)))
                .thenThrow(SdkException.builder().message("S3 error").build());

        assertThrows(IOException.class, () -> S3RangeReader.builder()
                .s3Client(s3Client)
                .bucket(BUCKET)
                .key(KEY)
                .build());
    }

    @Test
    void testS3ExceptionDuringRead() {
        // Override the default behavior for this specific test
        when(s3Client.getObjectAsBytes((GetObjectRequest) any(GetObjectRequest.class)))
                .thenThrow(SdkException.builder().message("S3 download error").build());

        assertThrows(IOException.class, () -> reader.readRange(0, 100));
    }

    @Test
    void testS3UnexpectedContentLength() {
        // Override the default behavior for this specific test
        when(getObjectResponse.contentLength()).thenReturn(50L); // Wrong content length

        assertThrows(IOException.class, () -> reader.readRange(0, 100));
    }
}
