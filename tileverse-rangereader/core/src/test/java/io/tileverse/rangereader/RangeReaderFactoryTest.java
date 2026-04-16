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

    private static final String HTTP_TEST_PATH = "/test-data.bin";
    private static final byte[] TEST_DATA = createTestData(1024);

    @RegisterExtension
    WireMockExtension wm = WireMockExtension.newInstance()
            .options(wireMockConfig().dynamicPort())
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
        httpUri = URI.create("http://localhost:" + wm.getPort() + HTTP_TEST_PATH);

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

    @Test
    void fileReaderCreateFromURI() throws IOException {
        Properties props = new Properties();
        props.put(RangeReaderConfig.URI_KEY, testFile.toUri());

        try (RangeReader reader = RangeReaderFactory.create(props)) {
            assertThat(reader).isInstanceOf(FileRangeReader.class);
            assertThat(reader.size()).hasValue(testContent.length());
            assertThat(reader.getSourceIdentifier()).contains(testFile.toString());
        }
    }

    @Test
    void fileReaderCreateFromString() throws IOException {
        Properties props = new Properties();

        props.put(RangeReaderConfig.URI_KEY, testFile.toUri().toString());

        try (RangeReader reader = RangeReaderFactory.create(props)) {
            assertThat(reader).isInstanceOf(FileRangeReader.class);
            assertThat(reader.size()).hasValue(testContent.length());
        }
    }

    @Test
    void fileReaderCreateFromURL() throws IOException {
        Properties props = new Properties();
        URL url = testFile.toUri().toURL();

        props.put(RangeReaderConfig.URI_KEY, url);

        try (RangeReader reader = RangeReaderFactory.create(props)) {
            assertThat(reader).isInstanceOf(FileRangeReader.class);
            assertThat(reader.size()).hasValue(testContent.length());
        }
    }

    @Test
    void fileReaderCreateFromPath() throws IOException {
        Properties props = new Properties();

        props.put(RangeReaderConfig.URI_KEY, testFile);

        try (RangeReader reader = RangeReaderFactory.create(props)) {
            assertThat(reader).isInstanceOf(FileRangeReader.class);
            assertThat(reader.size()).hasValue(testContent.length());
        }
    }

    @Test
    void fileReaderCreateFromPathToString() throws IOException {
        Properties props = new Properties();

        String pathString = testFile.toString();
        props.put(RangeReaderConfig.URI_KEY, pathString);

        try (RangeReader reader = RangeReaderFactory.create(props)) {
            assertThat(reader).isInstanceOf(FileRangeReader.class);
            assertThat(reader.size()).hasValue(testContent.length());
        }
    }

    @Test
    void fileReaderCreateFromAbsolutePathString() throws IOException {
        Properties props = new Properties();

        String absolutePath = testFile.toAbsolutePath().toString();
        props.put(RangeReaderConfig.URI_KEY, absolutePath);

        try (RangeReader reader = RangeReaderFactory.create(props)) {
            assertThat(reader).isInstanceOf(FileRangeReader.class);
            assertThat(reader.size()).hasValue(testContent.length());
        }
    }

    @Test
    void fileReaderCreateWithURIAndProperties() throws IOException {
        Properties props = new Properties();
        try (RangeReader reader = RangeReaderFactory.create(testFile.toUri(), props)) {
            assertThat(reader).isInstanceOf(FileRangeReader.class);
            assertThat(reader.size()).hasValue(testContent.length());
        }
    }

    @Test
    void httpReaderCreateFromURI() throws IOException {
        Properties props = new Properties();
        props.put(RangeReaderConfig.URI_KEY, httpUri);

        try (RangeReader reader = RangeReaderFactory.create(props)) {
            assertThat(reader).isInstanceOf(HttpRangeReader.class);
            assertThat(reader.size()).hasValue(TEST_DATA.length);
            assertThat(reader.getSourceIdentifier()).contains(httpUri.toString());
        }
    }

    @Test
    void httpReaderCreateFromString() throws IOException {
        Properties props = new Properties();
        props.put(RangeReaderConfig.URI_KEY, httpUri.toString());

        try (RangeReader reader = RangeReaderFactory.create(props)) {
            assertThat(reader).isInstanceOf(HttpRangeReader.class);
            assertThat(reader.size()).hasValue(TEST_DATA.length);
        }
    }

    @Test
    void httpReaderCreateFromURL() throws IOException {
        Properties props = new Properties();
        URL url = httpUri.toURL();
        props.put(RangeReaderConfig.URI_KEY, url);

        try (RangeReader reader = RangeReaderFactory.create(props)) {
            assertThat(reader).isInstanceOf(HttpRangeReader.class);
            assertThat(reader.size()).hasValue(TEST_DATA.length);
        }
    }

    @Test
    void httpReaderCreateWithAuthentication() throws IOException {
        Properties props = new Properties();
        props.put(RangeReaderConfig.URI_KEY, httpUri);
        props.setProperty("io.tileverse.rangereader.http.bearer-token", "test-token");

        try (RangeReader reader = RangeReaderFactory.create(props)) {
            assertThat(reader).isInstanceOf(HttpRangeReader.class);
            assertThat(reader.size()).hasValue(TEST_DATA.length);
        }
    }

    @Test
    void httpReaderCreateWithURIAndProperties() throws IOException {
        Properties props = new Properties();
        props.setProperty("io.tileverse.rangereader.http.timeout-millis", "10000");

        try (RangeReader reader = RangeReaderFactory.create(httpUri, props)) {
            assertThat(reader).isInstanceOf(HttpRangeReader.class);
            assertThat(reader.size()).hasValue(TEST_DATA.length);
        }
    }

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

        Properties props = new Properties();
        props.put(RangeReaderConfig.URI_KEY, testFile.toUri());

        try (RangeReader reader = RangeReaderFactory.create(props)) {
            assertThat(reader).isInstanceOf(FileRangeReader.class);
        }
    }

    @Test
    void testAutoProviderSelectionForHttp() throws IOException {
        Properties props = new Properties();
        props.put(RangeReaderConfig.URI_KEY, httpUri);

        try (RangeReader reader = RangeReaderFactory.create(props)) {
            assertThat(reader).isInstanceOf(HttpRangeReader.class);
        }
    }
}
