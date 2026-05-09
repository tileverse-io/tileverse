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
package io.tileverse.storage.gcs;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobGetOption;
import com.google.cloud.storage.StorageException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GoogleCloudStorageRangeReaderTest {

    private static final String BUCKET = "test-bucket";
    private static final String OBJECT_NAME = "test.pmtiles";
    private static final int CONTENT_LENGTH = 10000;
    private static final byte[] TEST_DATA = createTestData(CONTENT_LENGTH);

    @Mock
    private Storage storage;

    @Mock
    private Blob blob;

    @Mock
    private ReadChannel readChannel;

    private GoogleCloudStorageRangeReader reader;
    private long currentSeekPosition = 0;

    /** Creates test data with a predictable pattern. */
    private static byte[] createTestData(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) (i % 256);
        }
        return data;
    }

    @BeforeEach
    void setUp() throws IOException {
        BlobId blobId = BlobId.of(BUCKET, OBJECT_NAME);

        // Reset seek position
        currentSeekPosition = 0;

        // Make mocks lenient for this test class to avoid unnecessary stubbing errors
        lenient().when(storage.get(eq(blobId), any(BlobGetOption[].class))).thenReturn(blob);
        lenient().when(blob.exists()).thenReturn(true);
        lenient().when(blob.getSize()).thenReturn((long) CONTENT_LENGTH);

        // Setup mock for blob.reader()
        lenient().when(blob.reader()).thenReturn(readChannel);

        // Setup mock for seek operation
        lenient()
                .doAnswer(invocation -> {
                    currentSeekPosition = invocation.getArgument(0);
                    return null;
                })
                .when(readChannel)
                .seek(any(Long.class));

        // Setup mock for ReadChannel behavior
        lenient().when(readChannel.read(any(ByteBuffer.class))).thenAnswer(invocation -> {
            ByteBuffer buffer = invocation.getArgument(0);
            int remaining = buffer.remaining();
            int bytesToRead = (int) Math.min(remaining, TEST_DATA.length - currentSeekPosition);

            if (bytesToRead <= 0) {
                return -1; // EOF
            }

            // Copy data from the current seek position
            for (int i = 0; i < bytesToRead; i++) {
                buffer.put(TEST_DATA[(int) currentSeekPosition + i]);
            }

            currentSeekPosition += bytesToRead;
            return bytesToRead;
        });

        // Create the reader
        reader = new GoogleCloudStorageRangeReader(storage, BUCKET, OBJECT_NAME, Optional.empty());
    }

    @Test
    void testGetSize() {
        assertEquals(CONTENT_LENGTH, reader.size().getAsLong());
        verify(storage, times(1)).get(eq(BlobId.of(BUCKET, OBJECT_NAME)), any(BlobGetOption[].class));
    }

    @Test
    void testReadEntireFile() throws IOException {
        ByteBuffer buffer = reader.readRange(0, CONTENT_LENGTH);
        buffer.flip();

        assertEquals(CONTENT_LENGTH, buffer.remaining());

        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);

        assertArrayEquals(TEST_DATA, bytes);

        verify(blob).reader();
        verify(readChannel).seek(0L);
    }

    @Test
    void testReadRange() throws IOException {
        int offset = 100;
        int length = 500;

        ByteBuffer buffer = reader.readRange(offset, length);
        buffer.flip();

        assertEquals(length, buffer.remaining());

        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);

        byte[] expectedBytes = Arrays.copyOfRange(TEST_DATA, offset, offset + length);
        assertArrayEquals(expectedBytes, bytes);

        verify(blob).reader();
        verify(readChannel).seek((long) offset);
    }

    @Test
    void testReadRangeBeyondEnd() throws IOException {
        int offset = CONTENT_LENGTH - 200;
        int length = 500; // Beyond the end
        int actualLength = 200; // Should be truncated to end of file

        ByteBuffer buffer = reader.readRange(offset, length);
        buffer.flip();

        // Should only return up to the end of the file
        assertEquals(actualLength, buffer.remaining());

        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);

        byte[] expectedBytes = Arrays.copyOfRange(TEST_DATA, offset, CONTENT_LENGTH);
        assertArrayEquals(expectedBytes, bytes);

        verify(blob).reader();
        verify(readChannel).seek((long) offset);
    }

    @Test
    void testReadZeroLength() {
        ByteBuffer buffer = reader.readRange(100, 0);
        buffer.flip();

        assertEquals(0, buffer.remaining());

        // Should not make any API calls for zero length
        verify(blob, never()).reader();
    }

    @Test
    void testReadWithNegativeOffset() {
        assertThatThrownBy(() -> reader.readRange(-1, 10)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testReadWithNegativeLength() {
        assertThatThrownBy(() -> reader.readRange(0, -1)).isInstanceOf(IllegalArgumentException.class);
    }

    @SuppressWarnings("resource")
    @Test
    void testObjectExistsReturnsFalse() {
        // Override the default behavior for this specific test
        when(blob.exists()).thenReturn(false);

        assertThatThrownBy(
                        () -> new GoogleCloudStorageRangeReader(storage, BUCKET, OBJECT_NAME, Optional.empty()).size())
                .isInstanceOf(io.tileverse.storage.NotFoundException.class);
    }

    @Test
    void testStorageExceptionDuringRead() {
        // Override the default behavior for this specific test
        when(blob.reader()).thenThrow(new StorageException(500, "Storage error"));

        assertThatThrownBy(() -> reader.readRange(0, 100)).isInstanceOf(io.tileverse.storage.StorageException.class);
    }

    @Test
    void testUnexpectedContentLength() throws IOException {
        // This test is no longer applicable with the new streaming API
        // The ReadChannel approach handles partial reads naturally
        // So we can remove this test or adapt it for different error conditions

        // Test reading when channel returns -1 (EOF) immediately
        when(blob.reader()).thenReturn(readChannel);
        when(readChannel.read(any(ByteBuffer.class))).thenReturn(-1);

        ByteBuffer buffer = reader.readRange(0, 100);
        buffer.flip();
        assertEquals(0, buffer.remaining(), "Should return empty buffer when EOF reached immediately");
    }

    @Test
    void testGetSizeFromCachedValue() {
        // First call should query the blob
        assertEquals(CONTENT_LENGTH, reader.size().getAsLong());
        verify(storage, times(1)).get(eq(BlobId.of(BUCKET, OBJECT_NAME)), any(BlobGetOption[].class));

        // Second call should use cached value
        assertEquals(CONTENT_LENGTH, reader.size().getAsLong());
        verify(storage, times(1))
                .get(eq(BlobId.of(BUCKET, OBJECT_NAME)), any(BlobGetOption[].class)); // Still only one call
    }

    @Test
    void testNullInputsInConstructor() {
        assertThatThrownBy(() -> new GoogleCloudStorageRangeReader(null, BUCKET, OBJECT_NAME, Optional.empty()))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new GoogleCloudStorageRangeReader(storage, null, OBJECT_NAME, Optional.empty()))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new GoogleCloudStorageRangeReader(storage, BUCKET, null, Optional.empty()))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testClose() {
        clearInvocations(storage);
        reader.close();
        verifyNoInteractions(storage);
    }
}
