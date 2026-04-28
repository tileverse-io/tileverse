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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.tileverse.rangereader.RangeReader;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * Integration tests for HttpRangeReader authentication mechanisms.
 *
 * <p>Uses an Apache httpd container that supports all authentication types natively: Basic and Digest via
 * {@code mod_auth_basic}/{@code mod_auth_digest}, and Bearer Token, API Key, and Custom Header via {@code mod_rewrite}
 * string comparison.
 */
@Testcontainers(disabledWithoutDocker = true)
class HttpRangeReaderAuthenticationIT {

    private static final int TEST_FILE_SIZE = 102400; // 100KB
    private static final String TEST_FILE_NAME = "test-file.bin";

    // Basic auth credentials
    private static final String BASIC_AUTH_USER = "basicuser";
    private static final String BASIC_AUTH_PASSWORD = "basicpass";

    // Digest auth credentials
    private static final String DIGEST_AUTH_USER = "digestuser";
    private static final String DIGEST_AUTH_PASSWORD = "digestpass";
    private static final String DIGEST_AUTH_REALM = "Secured Digest Test Area";

    // Bearer token
    private static final String BEARER_TOKEN =
            "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0LXVzZXIiLCJuYW1lIjoiVGVzdCBVc2VyIiwiaWF0IjoxNTE2MjM5MDIyfQ.tHN1PJiNw9UWRcmRGjXBc5rNWfr3Y9Py3C5dPNMOFzg";

    // API key
    private static final String API_KEY = "api-key-test-value-12345";
    private static final String API_KEY_HEADER = "X-API-Key";

    // Custom header
    private static final String CUSTOM_HEADER_NAME = "X-Custom-Auth";
    private static final String CUSTOM_HEADER_VALUE = "custom-auth-test-value-67890";

    // URIs for the different protected endpoints
    private static URI publicFileUri;
    private static URI basicAuthUri;
    private static URI digestAuthUri;
    private static URI bearerTokenUri;
    private static URI apiKeyUri;
    private static URI customHeaderUri;

    @Container
    @SuppressWarnings("resource")
    static GenericContainer<?> httpd = new GenericContainer<>(DockerImageName.parse("httpd:alpine"))
            .withCommand("sh", "-c", """
                    dd if=/dev/urandom of=/usr/local/apache2/htdocs/%s bs=1024 count=100 2>/dev/null && \
                    for dir in basic digest bearer apikey custom; do \
                        mkdir -p /usr/local/apache2/htdocs/secured/$dir && \
                        cp /usr/local/apache2/htdocs/%s /usr/local/apache2/htdocs/secured/$dir/%s; \
                    done && \
                    htpasswd -bc /usr/local/apache2/conf/.htpasswd %s %s && \
                    HASH=$(printf '%%s' '%s:%s:%s' | md5sum | cut -d' ' -f1) && \
                    printf '%%s\\n' "%s:%s:$HASH" > /usr/local/apache2/conf/.htdigest && \
                    httpd-foreground
                    """.formatted(
                            TEST_FILE_NAME,
                            TEST_FILE_NAME,
                            TEST_FILE_NAME,
                            BASIC_AUTH_USER,
                            BASIC_AUTH_PASSWORD,
                            DIGEST_AUTH_USER,
                            DIGEST_AUTH_REALM,
                            DIGEST_AUTH_PASSWORD,
                            DIGEST_AUTH_USER,
                            DIGEST_AUTH_REALM))
            .withExposedPorts(80)
            .withCopyFileToContainer(
                    MountableFile.forClasspathResource("httpd-auth-test.conf"), "/usr/local/apache2/conf/httpd.conf")
            .waitingFor(Wait.forHttp("/").forPort(80))
            .withLogConsumer(outputFrame -> System.out.println("HTTPD: " + outputFrame.getUtf8String()));

    @BeforeAll
    static void setupURIs() {
        String baseUrl = String.format("http://%s:%d", httpd.getHost(), httpd.getFirstMappedPort());
        publicFileUri = URI.create(baseUrl + "/" + TEST_FILE_NAME);
        basicAuthUri = URI.create(baseUrl + "/secured/basic/" + TEST_FILE_NAME);
        digestAuthUri = URI.create(baseUrl + "/secured/digest/" + TEST_FILE_NAME);
        bearerTokenUri = URI.create(baseUrl + "/secured/bearer/" + TEST_FILE_NAME);
        apiKeyUri = URI.create(baseUrl + "/secured/apikey/" + TEST_FILE_NAME);
        customHeaderUri = URI.create(baseUrl + "/secured/custom/" + TEST_FILE_NAME);
    }

    @Test
    void publicAccess() throws IOException {
        try (RangeReader reader = HttpRangeReader.of(publicFileUri)) {
            assertEquals(TEST_FILE_SIZE, reader.size().orElseThrow(), "File size should match");

            ByteBuffer buffer = reader.readRange(0, 1024);
            buffer.flip();
            assertEquals(1024, buffer.remaining(), "Should read 1024 bytes");
        }
    }

    @Test
    void basicAuthNoCredentials() {
        try (HttpRangeReader reader = HttpRangeReader.of(basicAuthUri)) {
            assertThrows(
                    IOException.class,
                    reader::size,
                    "Accessing basic auth protected resource without credentials should throw IOException");
        }
    }

    @Test
    void basicAuthWithCorrectCredentials() throws IOException {
        BasicAuthentication auth = new BasicAuthentication(BASIC_AUTH_USER, BASIC_AUTH_PASSWORD);

        try (RangeReader reader =
                HttpRangeReader.builder(basicAuthUri).authentication(auth).build()) {
            assertEquals(TEST_FILE_SIZE, reader.size().orElseThrow(), "File size should match");

            ByteBuffer buffer = reader.readRange(0, 1024);
            buffer.flip();
            assertEquals(1024, buffer.remaining(), "Should read 1024 bytes");
        }
    }

    @Test
    void basicAuthWithIncorrectCredentials() {
        BasicAuthentication auth = new BasicAuthentication(BASIC_AUTH_USER, "wrongpassword");

        HttpRangeReader reader =
                HttpRangeReader.builder(basicAuthUri).authentication(auth).build();
        assertThrows(
                IOException.class,
                reader::size,
                "Accessing basic auth protected resource with incorrect credentials should throw IOException");
    }

    @Test
    void basicAuthWithBuilder() throws IOException {
        try (RangeReader reader = HttpRangeReader.builder(basicAuthUri)
                .basicAuth(BASIC_AUTH_USER, BASIC_AUTH_PASSWORD)
                .build()) {

            assertEquals(TEST_FILE_SIZE, reader.size().orElseThrow(), "File size should match");

            ByteBuffer buffer = reader.readRange(0, 1024);
            buffer.flip();
            assertEquals(1024, buffer.remaining(), "Should read 1024 bytes");
        }
    }

    @Test
    void digestNoCredentials() {
        try (HttpRangeReader reader = HttpRangeReader.of(digestAuthUri)) {
            assertThrows(
                    IOException.class,
                    reader::size,
                    "Accessing digest auth protected resource without credentials should throw IOException");
        }
    }

    @Test
    void digestWithCorrectCredentials() throws IOException {
        DigestAuthentication auth = new DigestAuthentication(DIGEST_AUTH_USER, DIGEST_AUTH_PASSWORD);

        try (RangeReader reader =
                HttpRangeReader.builder(digestAuthUri).authentication(auth).build()) {
            assertEquals(TEST_FILE_SIZE, reader.size().orElseThrow(), "File size should match");

            ByteBuffer buffer = reader.readRange(0, 1024);
            buffer.flip();
            assertEquals(1024, buffer.remaining(), "Should read 1024 bytes");
        }
    }

    @Test
    void digestWithIncorrectCredentials() {
        DigestAuthentication auth = new DigestAuthentication(DIGEST_AUTH_USER, "wrongpassword");
        HttpRangeReader reader =
                HttpRangeReader.builder(digestAuthUri).authentication(auth).build();
        assertThrows(
                IOException.class,
                reader::size,
                "Accessing digest auth protected resource with incorrect credentials should throw IOException");
    }

    @Test
    void digestMultipleRangeRequests() throws IOException {
        DigestAuthentication auth = new DigestAuthentication(DIGEST_AUTH_USER, DIGEST_AUTH_PASSWORD);

        try (RangeReader reader =
                HttpRangeReader.builder(digestAuthUri).authentication(auth).build()) {
            for (int i = 0; i < 5; i++) {
                int offset = i * 5000;
                int length = 1000;

                ByteBuffer buffer = reader.readRange(offset, length);
                buffer.flip();
                assertEquals(length, buffer.remaining(), "Range request " + i + " should return " + length + " bytes");
            }
        }
    }

    @Test
    void bearerTokenNoCredentials() {
        try (HttpRangeReader reader = HttpRangeReader.of(bearerTokenUri)) {
            assertThrows(
                    IOException.class,
                    reader::size,
                    "Accessing bearer token protected resource without token should throw IOException");
        }
    }

    @Test
    void bearerTokenWithCorrectToken() throws IOException {
        BearerTokenAuthentication auth = new BearerTokenAuthentication(BEARER_TOKEN);

        try (RangeReader reader =
                HttpRangeReader.builder(bearerTokenUri).authentication(auth).build()) {
            assertEquals(TEST_FILE_SIZE, reader.size().orElseThrow(), "File size should match");

            ByteBuffer buffer = reader.readRange(0, 1024);
            buffer.flip();
            assertEquals(1024, buffer.remaining(), "Should read 1024 bytes");
        }
    }

    @Test
    void bearerTokenWithIncorrectToken() {
        BearerTokenAuthentication auth = new BearerTokenAuthentication("wrong-token");

        HttpRangeReader reader =
                HttpRangeReader.builder(bearerTokenUri).authentication(auth).build();
        assertThrows(
                IOException.class,
                reader::size,
                "Accessing bearer token protected resource with incorrect token should throw IOException");
    }

    @Test
    void bearerTokenWithBuilder() throws IOException {
        try (RangeReader reader = HttpRangeReader.builder()
                .uri(bearerTokenUri)
                .bearerToken(BEARER_TOKEN)
                .build()) {

            assertEquals(TEST_FILE_SIZE, reader.size().orElseThrow(), "File size should match");

            ByteBuffer buffer = reader.readRange(0, 1024);
            buffer.flip();
            assertEquals(1024, buffer.remaining(), "Should read 1024 bytes");
        }
    }

    @Test
    void apiKeyNoCredentials() {
        try (HttpRangeReader reader = HttpRangeReader.builder(apiKeyUri).build()) {
            assertThrows(
                    IOException.class,
                    reader::size,
                    "Accessing API key protected resource without API key should throw IOException");
        }
    }

    @Test
    void apiKeyWithCorrectKey() throws IOException {
        try (RangeReader reader = HttpRangeReader.builder(apiKeyUri)
                .apiKey(API_KEY_HEADER, API_KEY)
                .build()) {
            assertEquals(TEST_FILE_SIZE, reader.size().orElseThrow(), "File size should match");

            ByteBuffer buffer = reader.readRange(0, 1024);
            buffer.flip();
            assertEquals(1024, buffer.remaining(), "Should read 1024 bytes");
        }
    }

    @Test
    void apiKeyWithIncorrectKey() {
        try (HttpRangeReader reader = HttpRangeReader.builder(apiKeyUri)
                .apiKey(API_KEY_HEADER, "wrong-key")
                .build()) {
            assertThrows(
                    IOException.class,
                    reader::size,
                    "Accessing API key protected resource with incorrect API key should throw IOException");
        }
    }

    @Test
    void apiKeyWithBuilder() throws IOException {
        try (RangeReader reader = HttpRangeReader.builder(apiKeyUri)
                .apiKey(API_KEY_HEADER, API_KEY)
                .build()) {

            assertEquals(TEST_FILE_SIZE, reader.size().orElseThrow(), "File size should match");

            ByteBuffer buffer = reader.readRange(0, 1024);
            buffer.flip();
            assertEquals(1024, buffer.remaining(), "Should read 1024 bytes");
        }
    }

    @Test
    void customHeaderNoCredentials() {
        try (HttpRangeReader reader = HttpRangeReader.of(customHeaderUri)) {
            assertThrows(
                    IOException.class,
                    reader::size,
                    "Accessing custom header protected resource without header should throw IOException");
        }
    }

    @Test
    void customHeaderWithCorrectValue() throws IOException {
        Map<String, String> headers = new HashMap<>();
        headers.put(CUSTOM_HEADER_NAME, CUSTOM_HEADER_VALUE);
        CustomHeaderAuthentication auth = new CustomHeaderAuthentication(headers);

        try (RangeReader reader =
                HttpRangeReader.builder(customHeaderUri).authentication(auth).build()) {
            assertEquals(TEST_FILE_SIZE, reader.size().orElseThrow(), "File size should match");

            ByteBuffer buffer = reader.readRange(0, 1024);
            buffer.flip();
            assertEquals(1024, buffer.remaining(), "Should read 1024 bytes");
        }
    }

    @Test
    void customHeaderWithIncorrectValue() {
        Map<String, String> headers = new HashMap<>();
        headers.put(CUSTOM_HEADER_NAME, "wrong-value");
        CustomHeaderAuthentication auth = new CustomHeaderAuthentication(headers);
        HttpRangeReader reader =
                HttpRangeReader.builder(customHeaderUri).authentication(auth).build();
        assertThrows(
                IOException.class,
                reader::size,
                "Accessing custom header protected resource with incorrect header value should throw IOException");
    }

    @Test
    void testMultipleAuthenticatedRangeRequests() throws IOException {
        try (RangeReader reader = HttpRangeReader.builder(basicAuthUri)
                .basicAuth(BASIC_AUTH_USER, BASIC_AUTH_PASSWORD)
                .build()) {
            for (int i = 0; i < 5; i++) {
                int offset = i * 5000;
                int length = 1000;

                ByteBuffer buffer = reader.readRange(offset, length);
                buffer.flip();
                assertEquals(length, buffer.remaining(), "Range request " + i + " should return " + length + " bytes");
            }
        }
    }
}
