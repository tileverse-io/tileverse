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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tileverse.rangereader.RangeReader;
import io.tileverse.rangereader.it.AbstractRangeReaderIT;
import io.tileverse.rangereader.it.TestUtil;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.NginxContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

/**
 * Integration tests for HttpRangeReader using Nginx container.
 *
 * <p>These tests verify that the HttpRangeReader can correctly read ranges of bytes from an HTTP server using range
 * requests. The tests use an Nginx container to serve a static test file that is accessed by the HttpRangeReader.
 *
 * <p>Note: For tests using authentication, see HttpRangeReaderAuthenticationIT.
 */
@Testcontainers(disabledWithoutDocker = true)
class HttpRangeReaderIT extends AbstractRangeReaderIT {

    private static final String TEST_FILE_NAME = "test.bin";

    private static URI testFileUri;
    private static Path testFilePath;
    static NginxContainer<?> nginx;

    @BeforeAll
    @SuppressWarnings("resource")
    static void setupNginx(@TempDir Path tempDir) throws IOException {
        System.out.println("Setting up Nginx container for HTTP Range Reader tests...");

        // Create local placeholder file for the abstract test class
        testFilePath = tempDir.resolve(TEST_FILE_NAME);
        TestUtil.createMockTestFile(testFilePath, TEST_FILE_SIZE);

        nginx = new NginxContainer<>("nginx:latest")
                .withCopyToContainer(
                        MountableFile.forHostPath(testFilePath), "/usr/share/nginx/html/" + TEST_FILE_NAME);
        nginx.start();

        // Set up the URI for accessing the test file via HTTP
        String baseUrl = String.format("http://%s:%d", nginx.getHost(), nginx.getFirstMappedPort());
        testFileUri = URI.create(baseUrl + "/" + TEST_FILE_NAME);
    }

    @AfterAll
    static void cleanup() {
        if (nginx != null) {
            nginx.stop();
        }
    }

    @BeforeEach
    void setUp() {
        super.testFile = testFilePath;
    }

    @Override
    protected RangeReader createBaseReader() throws IOException {
        return HttpRangeReader.of(testFileUri);
    }

    /** Additional HTTP-specific tests can go here */
    @Test
    void testHttpRangeReaderBuilder() throws IOException {
        try (RangeReader reader = HttpRangeReader.builder().uri(testFileUri).build()) {
            assertThat(reader).isInstanceOf(HttpRangeReader.class);

            assertEquals(TEST_FILE_SIZE, reader.size().getAsLong(), "File size should match");

            ByteBuffer buffer = reader.readRange(0, 10).flip();
            assertEquals(10, buffer.remaining(), "Should read 10 bytes");
        }
    }

    @Test
    void testHttpRangeReaderWithTrustAllCertificates() throws IOException {
        // Create RangeReader with trust all certificates option
        try (RangeReader reader = HttpRangeReader.builder()
                .uri(testFileUri)
                .trustAllCertificates()
                .build()) {

            // Verify it's the right implementation
            assertTrue(reader instanceof HttpRangeReader, "Should be an HttpRangeReader instance");

            // Verify size matches
            assertEquals(TEST_FILE_SIZE, reader.size().getAsLong(), "File size should match");
        }
    }

    @Test
    void testHttpRangeReaderWithInvalidUrl() {
        URI invalidUri = URI.create(testFileUri.toString() + ".does-not-exist");

        HttpRangeReader reader = HttpRangeReader.of(invalidUri);

        assertThrows(IOException.class, reader::size, "Calling size() with a nonexistent URL should throw IOException");
    }

    @Test
    void testMultipleConsecutiveRangeRequests() throws IOException {
        try (RangeReader reader = createBaseReader()) {
            assertEquals(TEST_FILE_SIZE, reader.size().getAsLong(), "File size should match");

            for (int i = 0; i < 5; i++) {
                int offset = i * 1000;
                ByteBuffer buffer = reader.readRange(offset, 100).flip();
                assertEquals(100, buffer.remaining(), "Range request " + i + " should return 100 bytes");
            }

            ByteBuffer endBuffer = reader.readRange(TEST_FILE_SIZE - 50, 100).flip();
            assertEquals(50, endBuffer.remaining(), "Range request at end of file should be truncated");
        }
    }

    @Test
    void testSmallAndLargeRangeRequests() throws IOException {

        try (RangeReader reader = createBaseReader()) {

            ByteBuffer smallBuffer = reader.readRange(100, 10).flip();
            assertEquals(10, smallBuffer.remaining(), "Small range request should return 10 bytes");

            ByteBuffer mediumBuffer = reader.readRange(5000, 1024).flip();
            assertEquals(1024, mediumBuffer.remaining(), "Medium range request should return 1024 bytes");

            ByteBuffer largeBuffer = reader.readRange(10000, 10240).flip();
            assertEquals(10240, largeBuffer.remaining(), "Large range request should return 10240 bytes");

            ByteBuffer veryLargeBuffer = reader.readRange(20000, 51200).flip();
            assertEquals(51200, veryLargeBuffer.remaining(), "Very large range request should return 51200 bytes");
        }
    }

    @Test
    void testReadingAcrossChunkBoundaries() throws IOException {

        try (RangeReader reader = createBaseReader()) {
            for (int chunkSize : new int[] {4096, 8192, 16384}) {
                int startOffset = chunkSize - 100;
                int length = 200;

                ByteBuffer buffer = reader.readRange(startOffset, length).flip();
                assertEquals(
                        length,
                        buffer.remaining(),
                        "Range crossing " + chunkSize + " chunk boundary should return " + length + " bytes");
            }
        }
    }
}
