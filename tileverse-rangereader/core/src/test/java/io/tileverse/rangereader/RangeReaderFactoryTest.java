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
package io.tileverse.rangereader;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.head;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import io.tileverse.rangereader.file.FileRangeReader;
import io.tileverse.rangereader.http.HttpRangeReader;
import io.tileverse.rangereader.spi.RangeReaderConfig;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link RangeReaderFactory} with file and HTTP providers available in core module.
 *
 * <p>Tests the factory's ability to create readers using property-based configuration
 * with different URI types (String, URI, URL, Path).
 */
class RangeReaderFactoryTest {

    private static final int TEST_PORT = 8090;
    private static final String HTTP_TEST_PATH = "/test-data.bin";
    private static final byte[] TEST_DATA = createTestData(1024);

    @RegisterExtension
    static WireMockExtension wm = WireMockExtension.newInstance()
            .options(wireMockConfig().port(TEST_PORT))
            .build();

    @TempDir
    Path tempDir;

    private Path testFile;
    private String testContent;
    private URI httpUri;

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
        // Set up test file
        testFile = tempDir.resolve("test-file.txt");
        testContent = "The quick brown fox jumps over the lazy dog.";
        Files.writeString(testFile, testContent, StandardOpenOption.CREATE);

        // Set up HTTP mock server
        httpUri = URI.create("http://localhost:" + TEST_PORT + HTTP_TEST_PATH);

        wm.stubFor(head(urlEqualTo(HTTP_TEST_PATH))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Length", String.valueOf(TEST_DATA.length))
                        .withHeader("Accept-Ranges", "bytes")));

        wm.stubFor(get(urlEqualTo(HTTP_TEST_PATH))
                .willReturn(aResponse().withStatus(200).withBody(TEST_DATA)));
    }

    @AfterEach
    void tearDown() {
        wm.resetAll();
    }

    // =========================================================================
    // File RangeReader Tests
    // =========================================================================

    @Test
    void testCreateFileReaderFromURI() throws IOException {
        Properties props = new Properties();
        props.put(RangeReaderConfig.URI_KEY, testFile.toUri());

        try (RangeReader reader = RangeReaderFactory.create(props)) {
            assertThat(reader).isInstanceOf(FileRangeReader.class);
            assertThat(reader.size()).hasValue(testContent.length());
            assertThat(reader.getSourceIdentifier()).contains(testFile.toString());
        }
    }

    @Test
    void testCreateFileReaderFromString() throws IOException {
        Properties props = new Properties();
        // Use file:// URI string for cross-platform compatibility
        props.put(RangeReaderConfig.URI_KEY, testFile.toUri().toString());

        try (RangeReader reader = RangeReaderFactory.create(props)) {
            assertThat(reader).isInstanceOf(FileRangeReader.class);
            assertThat(reader.size()).hasValue(testContent.length());
        }
    }

    @Test
    void testCreateFileReaderFromURL() throws IOException {
        Properties props = new Properties();
        URL url = testFile.toUri().toURL();
        // Put URL object directly - RangeReaderConfig should handle conversion
        props.put(RangeReaderConfig.URI_KEY, url);

        try (RangeReader reader = RangeReaderFactory.create(props)) {
            assertThat(reader).isInstanceOf(FileRangeReader.class);
            assertThat(reader.size()).hasValue(testContent.length());
        }
    }

    @Test
    void testCreateFileReaderFromPath() throws IOException {
        Properties props = new Properties();
        // Put Path object directly - RangeReaderConfig should handle conversion
        props.put(RangeReaderConfig.URI_KEY, testFile);

        try (RangeReader reader = RangeReaderFactory.create(props)) {
            assertThat(reader).isInstanceOf(FileRangeReader.class);
            assertThat(reader.size()).hasValue(testContent.length());
        }
    }

    @Test
    void testCreateFileReaderFromPathToString() throws IOException {
        Properties props = new Properties();
        // Use Path.toString() - should work on both Unix and Windows
        String pathString = testFile.toString();
        props.put(RangeReaderConfig.URI_KEY, pathString);

        try (RangeReader reader = RangeReaderFactory.create(props)) {
            assertThat(reader).isInstanceOf(FileRangeReader.class);
            assertThat(reader.size()).hasValue(testContent.length());
        }
    }

    @Test
    void testCreateFileReaderFromAbsolutePathString() throws IOException {
        Properties props = new Properties();
        // Use absolute path string - should work on both Unix and Windows
        String absolutePath = testFile.toAbsolutePath().toString();
        props.put(RangeReaderConfig.URI_KEY, absolutePath);

        try (RangeReader reader = RangeReaderFactory.create(props)) {
            assertThat(reader).isInstanceOf(FileRangeReader.class);
            assertThat(reader.size()).hasValue(testContent.length());
        }
    }

    @Test
    void testCreateFileReaderWithURIAndProperties() throws IOException {
        Properties props = new Properties();
        // Additional properties can be added here if needed

        try (RangeReader reader = RangeReaderFactory.create(testFile.toUri(), props)) {
            assertThat(reader).isInstanceOf(FileRangeReader.class);
            assertThat(reader.size()).hasValue(testContent.length());
        }
    }

    // =========================================================================
    // HTTP RangeReader Tests
    // =========================================================================

    @Test
    void testCreateHttpReaderFromURI() throws IOException {
        Properties props = new Properties();
        props.put(RangeReaderConfig.URI_KEY, httpUri);

        try (RangeReader reader = RangeReaderFactory.create(props)) {
            assertThat(reader).isInstanceOf(HttpRangeReader.class);
            assertThat(reader.size()).hasValue(TEST_DATA.length);
            assertThat(reader.getSourceIdentifier()).contains(httpUri.toString());
        }
    }

    @Test
    void testCreateHttpReaderFromString() throws IOException {
        Properties props = new Properties();
        props.put(RangeReaderConfig.URI_KEY, httpUri.toString());

        try (RangeReader reader = RangeReaderFactory.create(props)) {
            assertThat(reader).isInstanceOf(HttpRangeReader.class);
            assertThat(reader.size()).hasValue(TEST_DATA.length);
        }
    }

    @Test
    void testCreateHttpReaderFromURL() throws IOException {
        Properties props = new Properties();
        URL url = httpUri.toURL();
        props.put(RangeReaderConfig.URI_KEY, url);

        try (RangeReader reader = RangeReaderFactory.create(props)) {
            assertThat(reader).isInstanceOf(HttpRangeReader.class);
            assertThat(reader.size()).hasValue(TEST_DATA.length);
        }
    }

    @Test
    void testCreateHttpReaderWithAuthentication() throws IOException {
        Properties props = new Properties();
        props.put(RangeReaderConfig.URI_KEY, httpUri);
        props.setProperty("io.tileverse.rangereader.http.bearer-token", "test-token");

        try (RangeReader reader = RangeReaderFactory.create(props)) {
            assertThat(reader).isInstanceOf(HttpRangeReader.class);
            assertThat(reader.size()).hasValue(TEST_DATA.length);
        }
    }

    @Test
    void testCreateHttpReaderWithURIAndProperties() throws IOException {
        Properties props = new Properties();
        props.setProperty("io.tileverse.rangereader.http.timeout-millis", "10000");

        try (RangeReader reader = RangeReaderFactory.create(httpUri, props)) {
            assertThat(reader).isInstanceOf(HttpRangeReader.class);
            assertThat(reader.size()).hasValue(TEST_DATA.length);
        }
    }

    // =========================================================================
    // Provider Selection Tests
    // =========================================================================

    @Test
    void testExplicitProviderSelection() throws IOException {
        Properties props = new Properties();
        props.put(RangeReaderConfig.URI_KEY, testFile.toUri());
        props.setProperty(RangeReaderConfig.PROVIDER_ID_KEY, "file");

        try (RangeReader reader = RangeReaderFactory.create(props)) {
            assertThat(reader).isInstanceOf(FileRangeReader.class);
        }
    }

    @Test
    void testAutoProviderSelectionForFile() throws IOException {
        // No explicit provider ID - factory should auto-select FileRangeReader
        Properties props = new Properties();
        props.put(RangeReaderConfig.URI_KEY, testFile.toUri());

        try (RangeReader reader = RangeReaderFactory.create(props)) {
            assertThat(reader).isInstanceOf(FileRangeReader.class);
        }
    }

    @Test
    void testAutoProviderSelectionForHttp() throws IOException {
        // No explicit provider ID - factory should auto-select HttpRangeReader
        Properties props = new Properties();
        props.put(RangeReaderConfig.URI_KEY, httpUri);

        try (RangeReader reader = RangeReaderFactory.create(props)) {
            assertThat(reader).isInstanceOf(HttpRangeReader.class);
        }
    }
}
