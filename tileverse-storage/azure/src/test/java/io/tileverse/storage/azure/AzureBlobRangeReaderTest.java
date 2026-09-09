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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doThrow;
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
import com.azure.storage.blob.models.BlobDownloadResponse;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobStorageException;
import io.tileverse.storage.NotFoundException;
import io.tileverse.storage.StorageException;
import io.tileverse.storage.adapters.ByteBufferOutputStream;
import io.tileverse.storage.batch.CoalescingPolicy;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit tests for {@link AzureBlobRangeReader}: bodies stream through the SDK's download stream straight into the
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
        reader = new AzureBlobRangeReader(blobClient);
    }

    /**
     * Serves TEST_DATA as the SDK does: the requested range, truncated at the blob's end, is written to the caller's
     * OutputStream in chunks and the response has the Content-Range header. A range starting past the end fails with a
     * 416.
     */
    private void stubRangedDownloads() {
        lenient()
                .when(blobClient.downloadStreamWithResponse(any(), any(), any(), any(), anyBoolean(), any(), any()))
                .thenAnswer(invocation -> {
                    OutputStream sink = invocation.getArgument(0);
                    assertThat(sink).isInstanceOf(ByteBufferOutputStream.class);
                    BlobRange range = invocation.getArgument(1);
                    long offset = range.getOffset();
                    if (offset >= CONTENT_LENGTH) {
                        throw blobException(416, "InvalidRange");
                    }
                    int available = (int) Math.min(range.getCount(), CONTENT_LENGTH - offset);
                    writeInChunks(sink, (int) offset, available + extraBytes);
                    return downloadResponse(offset, available);
                });
    }

    private static void writeInChunks(OutputStream sink, int from, int length) throws IOException {
        byte[] body = Arrays.copyOfRange(TEST_DATA, from, from + length);
        for (int written = 0; written < length; written += CHUNK_SIZE) {
            sink.write(body, written, Math.min(CHUNK_SIZE, length - written));
        }
    }

    private static BlobDownloadResponse downloadResponse(long offset, int length) {
        BlobDownloadResponse response = mock(BlobDownloadResponse.class);
        lenient().when(response.getStatusCode()).thenReturn(206);
        HttpHeaders headers = new HttpHeaders()
                .set(
                        HttpHeaderName.CONTENT_RANGE,
                        "bytes " + offset + "-" + (offset + length - 1) + "/" + CONTENT_LENGTH);
        lenient().when(response.getHeaders()).thenReturn(headers);
        return response;
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
        when(blobClient.getProperties()).thenReturn(blobProperties);
        when(blobProperties.getBlobSize()).thenReturn((long) CONTENT_LENGTH);
    }

    private static byte[] contents(ByteBuffer buffer, int from, int length) {
        byte[] out = new byte[length];
        buffer.get(from, out);
        return out;
    }

    @Test
    void testConstructorMakesNoRequests() {
        new AzureBlobRangeReader(blobClient);
        verifyNoInteractions(blobClient);
    }

    @Test
    void testGetSize() {
        stubProperties();

        assertThat(reader.size()).hasValue(CONTENT_LENGTH);
        verify(blobClient, times(1)).getProperties();
    }

    @Test
    void testSizeIsLazyAndMemoized() {
        stubProperties();
        AzureBlobRangeReader lazy = new AzureBlobRangeReader(blobClient);
        verify(blobClient, never()).getProperties();

        assertThat(lazy.size()).hasValue(CONTENT_LENGTH);
        assertThat(lazy.size()).hasValue(CONTENT_LENGTH);
        verify(blobClient, times(1)).getProperties();
    }

    @Test
    void testSizeCapturedFromRangeResponseWithoutProperties() {
        reader.readRange(0, 10);

        assertThat(reader.size()).hasValue(CONTENT_LENGTH);
        verify(blobClient, never()).getProperties();
    }

    @Test
    void testNotFoundThrownOnFirstReadNotAtConstruction() {
        AzureBlobRangeReader missing = new AzureBlobRangeReader(blobClient);
        doThrow(blobException(404, "BlobNotFound"))
                .when(blobClient)
                .downloadStreamWithResponse(any(), any(), any(), any(), anyBoolean(), any(), any());

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

    @Test
    void testReadZeroLength() {
        ByteBuffer buffer = reader.readRange(100, 0).flip();

        assertThat(buffer.remaining()).isZero();
        verify(blobClient, never()).downloadStreamWithResponse(any(), any(), any(), any(), anyBoolean(), any(), any());
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
        doThrow(new IllegalStateException("socket closed"))
                .when(blobClient)
                .downloadStreamWithResponse(any(), any(), any(), any(), anyBoolean(), any(), any());

        assertThatThrownBy(() -> reader.readRange(0, 100))
                .isInstanceOf(StorageException.class)
                .isNotInstanceOf(NotFoundException.class)
                .hasMessageContaining("socket closed");
    }

    @Test
    void batchedReadsUseTheObjectStorePolicyOnTheSharedExecutor() {
        AzureBlobRangeReader plainReader = new AzureBlobRangeReader(mock(BlobClient.class));
        assertThat(plainReader.coalescingPolicy()).isEqualTo(CoalescingPolicy.objectStoreDefaults());
        assertThat(plainReader.maxConcurrentFetches()).isEqualTo(8);
    }
}
