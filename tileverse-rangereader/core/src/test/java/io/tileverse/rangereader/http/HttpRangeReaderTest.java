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
package io.tileverse.rangereader.http;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.head;
import static com.github.tomakehurst.wiremock.client.WireMock.headRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Comprehensive tests for HttpRangeReader using WireMock.
 */
@WireMockTest
class HttpRangeReaderTest {

    private static final String TEST_PATH = "/test-pmtiles";
    private static final byte[] TEST_DATA = createTestData(100_000); // 100KB of test data
    private static final int TEST_PORT = 8089;

    @RegisterExtension
    static WireMockExtension wm = WireMockExtension.newInstance()
            .options(wireMockConfig().port(TEST_PORT))
            .build();

    private URI testUri;
    private HttpRangeReader reader;

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
        testUri = URI.create("http://localhost:" + TEST_PORT + TEST_PATH);

        // Set up WireMock stubs
        // HEAD request to get content length
        wm.stubFor(head(urlEqualTo(TEST_PATH))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Length", String.valueOf(TEST_DATA.length))
                        .withHeader("Accept-Ranges", "bytes")));

        // GET request for entire file (without range header)
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .willReturn(aResponse().withStatus(200).withBody(TEST_DATA)));

        // Individual range request stubs - we'll create these for each test as needed

        // Create reader
        reader = HttpRangeReader.of(testUri);
    }

    @Test
    void testGetSize() throws IOException {
        assertThat(reader.size()).hasValue(TEST_DATA.length);

        // Verify that HEAD request was made
        wm.verify(headRequestedFor(urlEqualTo(TEST_PATH)));
    }

    @Test
    void testReadEntireFile() throws IOException {
        // Stub the range request
        int start = 0;
        int end = TEST_DATA.length - 1;
        byte[] responseBytes = Arrays.copyOfRange(TEST_DATA, start, end + 1);

        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=" + start + "-" + end))
                .willReturn(aResponse()
                        .withStatus(206)
                        .withHeader("Content-Range", "bytes " + start + "-" + end + "/" + TEST_DATA.length)
                        .withHeader("Content-Length", String.valueOf(responseBytes.length))
                        .withBody(responseBytes)));

        ByteBuffer buffer = ByteBuffer.allocate(TEST_DATA.length);
        reader.readRange(0, TEST_DATA.length, buffer);
        buffer.flip();

        assertEquals(TEST_DATA.length, buffer.remaining());

        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);

        // Verify first few and last few bytes match expected pattern
        for (int i = 0; i < 10; i++) {
            assertEquals((byte) (i % 256), bytes[i], "Byte mismatch at index " + i);
        }

        for (int i = TEST_DATA.length - 10; i < TEST_DATA.length; i++) {
            assertEquals((byte) (i % 256), bytes[i], "Byte mismatch at index " + i);
        }

        // Verify that range request was made
        wm.verify(getRequestedFor(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=0-" + (TEST_DATA.length - 1))));
    }

    @Test
    void testReadRange() throws IOException {
        int offset = 1000;
        int length = 500;
        int end = offset + length - 1;

        // Create range response
        byte[] responseBytes = Arrays.copyOfRange(TEST_DATA, offset, offset + length);

        // Stub the range request
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=" + offset + "-" + end))
                .willReturn(aResponse()
                        .withStatus(206)
                        .withHeader("Content-Range", "bytes " + offset + "-" + end + "/" + TEST_DATA.length)
                        .withHeader("Content-Length", String.valueOf(responseBytes.length))
                        .withBody(responseBytes)));

        ByteBuffer buffer = ByteBuffer.allocate(length);
        reader.readRange(offset, length, buffer);
        buffer.flip();

        assertEquals(length, buffer.remaining());

        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);

        // Verify that the bytes match expected pattern
        for (int i = 0; i < length; i++) {
            assertEquals((byte) ((offset + i) % 256), bytes[i], "Byte mismatch at index " + i);
        }

        // Verify that range request was made
        wm.verify(getRequestedFor(urlEqualTo(TEST_PATH)).withHeader("Range", equalTo("bytes=" + offset + "-" + end)));
    }

    @Test
    void testReadRangeFromEnd() throws IOException {
        int offset = TEST_DATA.length - 500;
        int length = 500;
        int end = offset + length - 1;

        // Create range response
        byte[] responseBytes = Arrays.copyOfRange(TEST_DATA, offset, offset + length);

        // Stub the range request
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=" + offset + "-" + end))
                .willReturn(aResponse()
                        .withStatus(206)
                        .withHeader("Content-Range", "bytes " + offset + "-" + end + "/" + TEST_DATA.length)
                        .withHeader("Content-Length", String.valueOf(responseBytes.length))
                        .withBody(responseBytes)));

        ByteBuffer buffer = ByteBuffer.allocate(length);
        reader.readRange(offset, length, buffer);
        buffer.flip();

        assertEquals(length, buffer.remaining());

        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);

        // Verify that the bytes match expected pattern
        for (int i = 0; i < length; i++) {
            assertEquals((byte) ((offset + i) % 256), bytes[i], "Byte mismatch at index " + i);
        }

        // Verify that range request was made
        wm.verify(getRequestedFor(urlEqualTo(TEST_PATH)).withHeader("Range", equalTo("bytes=" + offset + "-" + end)));
    }

    @Test
    void testReadRangeBeyondEnd() throws IOException {
        // Set up a specific stub for this test
        int offset = TEST_DATA.length - 200;
        int length = 500; // This goes beyond the end of the data
        int end = TEST_DATA.length - 1; // Only returns to the end

        // Create range response - only including available data
        byte[] responseBytes = Arrays.copyOfRange(TEST_DATA, offset, TEST_DATA.length);

        // Stub the range request
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=" + offset + "-" + end))
                .willReturn(aResponse()
                        .withStatus(206)
                        .withHeader("Content-Range", "bytes " + offset + "-" + end + "/" + TEST_DATA.length)
                        .withHeader("Content-Length", String.valueOf(responseBytes.length))
                        .withBody(responseBytes)));

        ByteBuffer buffer = ByteBuffer.allocate(length);
        reader.readRange(offset, length, buffer);
        buffer.flip();

        // Should only get back 200 bytes (to the end of file)
        assertEquals(200, buffer.remaining());

        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);

        // Verify that the bytes match expected pattern
        for (int i = 0; i < 200; i++) {
            assertEquals((byte) ((offset + i) % 256), bytes[i], "Byte mismatch at index " + i);
        }

        // Verify that range request was made with the original range
        wm.verify(getRequestedFor(urlEqualTo(TEST_PATH)).withHeader("Range", equalTo("bytes=" + offset + "-" + end)));
    }

    @Test
    void testReadZeroLength() throws IOException {
        int offset = 100;
        int length = 0;

        // The implementation now returns an empty buffer for zero-length requests without making HTTP requests
        ByteBuffer buffer = ByteBuffer.allocate(length);
        reader.readRange(offset, length, buffer);
        buffer.flip();

        assertEquals(0, buffer.remaining());

        // No HTTP request should have been made
        wm.verify(0, getRequestedFor(urlEqualTo(TEST_PATH)).withHeader("Range", matching(".*")));
    }

    @Test
    void testReadWithNegativeOffset() {
        assertThrows(IllegalArgumentException.class, () -> reader.readRange(-1, 10, ByteBuffer.allocate(1)));
    }

    @Test
    void testReadWithNegativeLength() {
        assertThrows(IllegalArgumentException.class, () -> reader.readRange(0, -1, ByteBuffer.allocate(1)));
    }

    @Test
    void testServerWithoutRangeSupport() throws IOException {
        // Create a stub for a server that doesn't support range requests
        wm.stubFor(head(urlEqualTo("/no-range"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Length", String.valueOf(TEST_DATA.length))
                        .withHeader("Accept-Ranges", "none")));

        URI noRangeUri = URI.create("http://localhost:" + TEST_PORT + "/no-range");

        // Should throw when readRange() is called (triggering range support initialization)
        HttpRangeReader reader = HttpRangeReader.of(noRangeUri);
        assertThrows(IOException.class, () -> reader.readRange(0, 100, ByteBuffer.allocate(100)));
    }

    @Test
    void testServerReturningEntireFileForRangeRequest() throws IOException {
        // Request parameters
        int offset = 1000;
        int length = 200;
        int end = offset + length - 1;

        // Create a stub for a server that ignores range requests and returns the whole file
        wm.stubFor(get(urlEqualTo("/ignore-range"))
                .withHeader("Range", equalTo("bytes=" + offset + "-" + end))
                .willReturn(aResponse()
                        .withStatus(200) // Not 206 - indicates full file, not partial content
                        .withHeader("Content-Length", String.valueOf(TEST_DATA.length))
                        .withBody(TEST_DATA)));

        wm.stubFor(head(urlEqualTo("/ignore-range"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Length", String.valueOf(TEST_DATA.length))
                        .withHeader("Accept-Ranges", "bytes")));

        URI ignoreRangeUri = URI.create("http://localhost:" + TEST_PORT + "/ignore-range");

        // Should throw IOException when server doesn't support range requests (returns 200 instead of 206)
        try (HttpRangeReader ignoreRangeReader = HttpRangeReader.of(ignoreRangeUri)) {
            assertThrows(
                    IOException.class, () -> ignoreRangeReader.readRange(offset, length, ByteBuffer.allocate(length)));
        }
    }

    @Test
    void testServerReturningErrorForRangeRequest() {
        // Specific request parameters
        int offset = 0;
        int length = 100;
        int end = offset + length - 1;

        // Create a stub for a server that returns an error for the actual range request
        wm.stubFor(get(urlEqualTo("/error"))
                .withHeader("Range", equalTo("bytes=" + offset + "-" + end))
                .willReturn(aResponse()
                        .withStatus(416) // Range Not Satisfiable
                        .withBody("Range not satisfiable")));

        wm.stubFor(head(urlEqualTo("/error"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Length", String.valueOf(TEST_DATA.length))
                        .withHeader("Accept-Ranges", "bytes")));

        URI errorUri = URI.create("http://localhost:" + TEST_PORT + "/error");

        // Should be able to create the reader
        try (HttpRangeReader errorReader = HttpRangeReader.of(errorUri)) {
            // But reading a range should throw
            assertThrows(IOException.class, () -> errorReader.readRange(offset, length, ByteBuffer.allocate(length)));

            // Verify that range request was made
            wm.verify(
                    getRequestedFor(urlEqualTo("/error")).withHeader("Range", equalTo("bytes=" + offset + "-" + end)));
        }
    }

    @Test
    void testConcurrentRangeRequests() throws Exception {
        int numThreads = 10;
        int regionSize = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        AtomicBoolean failure = new AtomicBoolean(false);
        CountDownLatch startLatch = new CountDownLatch(1);

        try {
            // Create futures for concurrent execution
            Future<?>[] futures = new Future<?>[numThreads];

            // Set up stubs for all possible range requests
            for (int i = 0; i < numThreads; i++) {
                final int regionStart = i * regionSize;
                final int regionEnd = regionStart + regionSize - 1;

                // Create range response for this region
                byte[] responseBytes = Arrays.copyOfRange(TEST_DATA, regionStart, regionStart + regionSize);

                // Stub the range request for this region
                wm.stubFor(get(urlEqualTo(TEST_PATH))
                        .withHeader("Range", equalTo("bytes=" + regionStart + "-" + regionEnd))
                        .willReturn(aResponse()
                                .withStatus(206)
                                .withHeader(
                                        "Content-Range",
                                        "bytes " + regionStart + "-" + regionEnd + "/" + TEST_DATA.length)
                                .withHeader("Content-Length", String.valueOf(responseBytes.length))
                                .withBody(responseBytes)));
            }

            // Submit tasks for concurrent execution
            for (int i = 0; i < numThreads; i++) {
                final int threadIndex = i;
                final int regionStart = threadIndex * regionSize;

                futures[i] = executor.submit(() -> {
                    try {
                        startLatch.await(); // Wait for all threads to be ready

                        // Read a region
                        ByteBuffer buffer = ByteBuffer.allocate(regionSize);
                        reader.readRange(regionStart, regionSize, buffer);
                        buffer.flip();
                        byte[] data = new byte[buffer.remaining()];
                        buffer.get(data);

                        // Verify data is correct
                        for (int j = 0; j < data.length; j++) {
                            if (data[j] != (byte) ((regionStart + j) % 256)) {
                                System.err.printf(
                                        "Thread %d: Data mismatch at index %d, expected %d but got %d%n",
                                        threadIndex, j, (regionStart + j) % 256, data[j] & 0xFF);
                                failure.set(true);
                                return;
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        failure.set(true);
                    }
                });
            }

            // Start all threads at once
            startLatch.countDown();

            // Wait for completion and check for failures
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);

            assertFalse(failure.get(), "One or more threads encountered errors during concurrent reads");

            // Verify that all range requests were made
            for (int i = 0; i < numThreads; i++) {
                final int regionStart = i * regionSize;
                final int regionEnd = regionStart + regionSize - 1;

                wm.verify(getRequestedFor(urlEqualTo(TEST_PATH))
                        .withHeader("Range", equalTo("bytes=" + regionStart + "-" + regionEnd)));
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testMissingContentLengthHeader() throws IOException {
        // Test behavior when Content-Length header is missing
        wm.stubFor(head(urlEqualTo("/no-content-length"))
                .willReturn(
                        aResponse().withStatus(200).withHeader("Accept-Ranges", "bytes")
                        // No Content-Length header
                        ));

        URI noContentLengthUri = URI.create("http://localhost:" + TEST_PORT + "/no-content-length");

        try (HttpRangeReader reader =
                HttpRangeReader.builder(noContentLengthUri).build()) {
            assertThat(reader.size()).isEmpty();
        }
    }

    @Test
    void testInvalidContentLengthHeader() throws IOException {
        // Test behavior when Content-Length header is invalid
        // Let's use a simpler approach by creating a mock HttpRangeReader that throws on size()

        // Create a valid response for the HEAD request during construction
        wm.stubFor(head(urlEqualTo("/invalid-content-length"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Accept-Ranges", "bytes")
                        .withHeader("Content-Length", "-1")));

        URI invalidContentLengthUri = URI.create("http://localhost:" + TEST_PORT + "/invalid-content-length");

        HttpRangeReader reader =
                HttpRangeReader.builder(invalidContentLengthUri).build();

        assertThat(reader.size()).isEmpty();
    }

    @Test
    void testConnectionFailure() throws IOException {
        // Test behavior when connection fails
        URI nonExistentUri = URI.create("http://non-existent-host.example/test.pmtiles");

        // Should throw when size() is called (triggering initialization)
        HttpRangeReader reader = HttpRangeReader.of(nonExistentUri);
        assertThrows(IOException.class, reader::size);
    }

    @Test
    void testServerErrorOnHead() throws IOException {
        // Test behavior when server returns error on HEAD
        wm.stubFor(head(urlEqualTo("/server-error"))
                .willReturn(aResponse().withStatus(500).withBody("Internal Server Error")));

        URI serverErrorUri = URI.create("http://localhost:" + TEST_PORT + "/server-error");

        // Should throw when size() is called (triggering initialization)
        HttpRangeReader reader = HttpRangeReader.of(serverErrorUri);
        IOException ex = assertThrows(IOException.class, reader::size);
        assertThat(ex.getMessage()).contains("Failed to connect");
    }
}
