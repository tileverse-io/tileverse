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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * Integration tests for HttpRangeReader authentication mechanisms.
 * <p>
 * These tests verify that the HttpRangeReader correctly handles different
 * authentication methods when accessing protected resources.
 */
@Testcontainers(disabledWithoutDocker = true)
@Disabled
public class HttpRangeReaderAuthenticationIT {

    // Test file constants
    private static final int TEST_FILE_SIZE = 102400; // 100KB
    private static final String TEST_FILE_NAME = "test-file.bin"; // Must match the name used in the container setup

    // Authentication constants
    private static final String BASIC_AUTH_USER = "basicuser";
    private static final String BASIC_AUTH_PASSWORD = "basicpass";

    // Nginx doesn't support digest auth by default, so we'll skip those tests

    private static final String BEARER_TOKEN =
            "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0LXVzZXIiLCJuYW1lIjoiVGVzdCBVc2VyIiwiaWF0IjoxNTE2MjM5MDIyfQ.tHN1PJiNw9UWRcmRGjXBc5rNWfr3Y9Py3C5dPNMOFzg";

    private static final String API_KEY = "api-key-test-value-12345";
    private static final String API_KEY_HEADER = "X-API-Key";

    private static final String CUSTOM_HEADER_NAME = "X-Custom-Auth";
    private static final String CUSTOM_HEADER_VALUE = "custom-auth-test-value-67890";

    // URIs for the different protected endpoints
    private static URI publicFileUri;
    private static URI basicAuthUri;
    // Nginx doesn't support digest auth by default, so we'll skip those tests
    private static URI bearerTokenUri;
    private static URI apiKeyUri;
    private static URI customHeaderUri;

    @TempDir
    static Path tempDir;

    @Container
    @SuppressWarnings("resource")
    static GenericContainer<?> nginx = new GenericContainer<>(DockerImageName.parse("nginx:alpine"))
            .withCommand(
                    "sh",
                    "-c",
                    """
                    # Create test directories for each authentication method
                    mkdir -p /usr/share/nginx/html && \
                    mkdir -p /usr/share/nginx/html/secured/basic && \
                    # Skipping digest auth directory as it's not supported by default in Nginx
                    mkdir -p /usr/share/nginx/html/secured/bearer && \
                    mkdir -p /usr/share/nginx/html/secured/apikey && \
                    mkdir -p /usr/share/nginx/html/secured/custom && \

                    # Create test file
                    dd if=/dev/urandom of=/usr/share/nginx/html/test-file.bin bs=1024 count=100 && \
                    cp /usr/share/nginx/html/test-file.bin /usr/share/nginx/html/secured/basic/ && \
                    cp /usr/share/nginx/html/test-file.bin /usr/share/nginx/html/secured/bearer/ && \
                    cp /usr/share/nginx/html/test-file.bin /usr/share/nginx/html/secured/apikey/ && \
                    cp /usr/share/nginx/html/test-file.bin /usr/share/nginx/html/secured/custom/ && \

                    # Create basic auth password file
                    printf "%s:$(openssl passwd -apr1 %s)\\n" > /etc/nginx/.htpasswd && \

                    # Skip creating digest auth password file

                    # Copy nginx configuration
                    cp /etc/nginx/nginx.conf.template /etc/nginx/nginx.conf && \

                    # Debug info
                    echo "--- Files created ---" && \
                    find /usr/share/nginx/html -type f | xargs ls -la && \
                    echo "--- Basic auth file ---" && \
                    cat /etc/nginx/.htpasswd && \
                    # Skip displaying digest auth file
                    echo "--- NGINX Config ---" && \
                    cat /etc/nginx/nginx.conf && \

                    # Start nginx
                    nginx -g 'daemon off;'
                    """
                            .formatted(
                                    // Only Basic auth credentials needed
                                    BASIC_AUTH_USER, BASIC_AUTH_PASSWORD))
            .withExposedPorts(80)
            .withCopyFileToContainer(
                    MountableFile.forClasspathResource("nginx-auth-test.conf"), "/etc/nginx/nginx.conf.template")
            .waitingFor(Wait.forHttp("/").forPort(80))
            .withLogConsumer(outputFrame -> System.out.println("Nginx: " + outputFrame.getUtf8String()));

    @BeforeAll
    static void setupServer() throws IOException {
        // Create test file for verification
        Path testFile = tempDir.resolve(TEST_FILE_NAME);
        try (var out = Files.newOutputStream(testFile)) {
            byte[] randomData = new byte[1024];
            for (int i = 0; i < 100; i++) {
                java.util.Random random = new java.util.Random(i); // Deterministic for verification
                random.nextBytes(randomData);
                out.write(randomData);
            }
        }

        // Prepare the Nginx configuration
        String nginxConfig =
                """
        user  nginx;
        worker_processes  auto;

        error_log  /var/log/nginx/error.log notice;
        pid        /var/run/nginx.pid;

        events {
            worker_connections  1024;
        }

        http {
            include       /etc/nginx/mime.types;
            default_type  application/octet-stream;

            log_format  main  '$remote_addr - $remote_user [$time_local] "$request" '
                            '$status $body_bytes_sent "$http_referer" '
                            '"$http_user_agent" "$http_x_forwarded_for"';

            access_log  /var/log/nginx/access.log  main;

            sendfile        on;
            keepalive_timeout  65;

            server {
                listen       80;
                server_name  localhost;

                # Public area
                location / {
                    root   /usr/share/nginx/html;
                    autoindex on;
                    add_header Accept-Ranges bytes;
                }

                # Basic Auth protected area
                location /secured/basic/ {
                    alias /usr/share/nginx/html/secured/basic/;
                    autoindex on;
                    add_header Accept-Ranges bytes;

                    auth_basic "Secured Basic Test Area";
                    auth_basic_user_file /etc/nginx/.htpasswd;
                }

                # Digest Auth protected area
                location /secured/digest/ {
                    alias /usr/share/nginx/html/secured/digest/;
                    autoindex on;
                    add_header Accept-Ranges bytes;

                    auth_digest "Secured Digest Test Area";
                    auth_digest_user_file /etc/nginx/.htpasswd.digest;
                }

                # Bearer Token protected area
                location /secured/bearer/ {
                    alias /usr/share/nginx/html/secured/bearer/;
                    autoindex on;
                    add_header Accept-Ranges bytes;

                    # Set expected Bearer token
                    set $expected_bearer_token "%s";

                    # Check authorization header
                    if ($http_authorization ~ "^Bearer (.+)$") {
                        set $auth_token $1;
                    }

                    # If no auth token or it doesn't match expected token, return 401
                    if ($auth_token != $expected_bearer_token) {
                        return 401;
                    }
                }

                # API Key protected area
                location /secured/apikey/ {
                    alias /usr/share/nginx/html/secured/apikey/;
                    autoindex on;
                    add_header Accept-Ranges bytes;

                    # Set expected API key
                    set $expected_api_key "%s";

                    # If X-API-Key header doesn't match expected key, return 401
                    if ($http_x_api_key != $expected_api_key) {
                        return 401;
                    }
                }

                # Custom Header protected area
                location /secured/custom/ {
                    alias /usr/share/nginx/html/secured/custom/;
                    autoindex on;
                    add_header Accept-Ranges bytes;

                    # Set expected custom header name and value
                    set $custom_header_name "%s";
                    set $expected_custom_header_value "%s";

                    # Check if custom header matches expected value
                    set $auth_header "";

                    # Get the header value dynamically
                    if ($http_x_custom_auth) {
                        set $auth_header $http_x_custom_auth;
                    }

                    # If header doesn't match expected value, return 401
                    if ($auth_header != $expected_custom_header_value) {
                        return 401;
                    }
                }
            }
        }
        """
                        .formatted(BEARER_TOKEN, API_KEY, CUSTOM_HEADER_NAME, CUSTOM_HEADER_VALUE);

        // Save the Nginx configuration to a file
        Path nginxConfigPath = tempDir.resolve("nginx-auth-test.conf");
        Files.writeString(nginxConfigPath, nginxConfig);

        // Make sure the Nginx config file is accessible
        Files.copy(
                nginxConfigPath,
                Path.of("src/test/resources/nginx-auth-test.conf"),
                StandardCopyOption.REPLACE_EXISTING);

        // Set up the URIs for accessing the files via HTTP
        String baseUrl = String.format("http://%s:%d", nginx.getHost(), nginx.getFirstMappedPort());
        publicFileUri = URI.create(baseUrl + "/" + TEST_FILE_NAME);
        basicAuthUri = URI.create(baseUrl + "/secured/basic/" + TEST_FILE_NAME);
        // Skip digest auth URI creation
        bearerTokenUri = URI.create(baseUrl + "/secured/bearer/" + TEST_FILE_NAME);
        apiKeyUri = URI.create(baseUrl + "/secured/apikey/" + TEST_FILE_NAME);
        customHeaderUri = URI.create(baseUrl + "/secured/custom/" + TEST_FILE_NAME);

        System.out.println("Test files available at:");
        System.out.println("Public: " + publicFileUri);
        System.out.println("Basic Auth: " + basicAuthUri);
        // Skip digest auth as it's not supported by default in Nginx
        System.out.println("Bearer Token: " + bearerTokenUri);
        System.out.println("API Key: " + apiKeyUri);
        System.out.println("Custom Header: " + customHeaderUri);
    }

    // ----- Public Access Tests -----

    @Test
    void testPublicFileAccess() throws IOException {
        // Test access to a public file (no authentication required)
        try (RangeReader reader = HttpRangeReader.of(publicFileUri)) {
            // Verify size
            assertEquals(TEST_FILE_SIZE, reader.size(), "File size should match");

            // Read some data
            ByteBuffer buffer = reader.readRange(0, 1024);
            assertEquals(1024, buffer.remaining(), "Should read 1024 bytes");
        }
    }

    // ----- Basic Authentication Tests -----

    @Test
    void testBasicAuthNoCredentials() {
        // Test that accessing basic auth protected resource without credentials fails
        assertThrows(
                IOException.class,
                () -> HttpRangeReader.of(basicAuthUri),
                "Accessing basic auth protected resource without credentials should throw IOException");
    }

    @Test
    void testBasicAuthWithCorrectCredentials() throws IOException {
        // Test accessing basic auth protected resource with correct credentials
        BasicAuthentication auth = new BasicAuthentication(BASIC_AUTH_USER, BASIC_AUTH_PASSWORD);

        try (RangeReader reader =
                HttpRangeReader.builder(basicAuthUri).authentication(auth).build()) {
            // Verify size
            assertEquals(TEST_FILE_SIZE, reader.size(), "File size should match");

            // Read some data
            ByteBuffer buffer = reader.readRange(0, 1024);
            assertEquals(1024, buffer.remaining(), "Should read 1024 bytes");
        }
    }

    @Test
    void testBasicAuthWithIncorrectCredentials() {
        // Test that accessing basic auth protected resource with incorrect credentials fails
        BasicAuthentication auth = new BasicAuthentication(BASIC_AUTH_USER, "wrongpassword");

        assertThrows(
                IOException.class,
                () -> HttpRangeReader.builder(basicAuthUri).authentication(auth).build(),
                "Accessing basic auth protected resource with incorrect credentials should throw IOException");
    }

    @Test
    void testBasicAuthWithBuilder() throws IOException {
        // Test accessing basic auth protected resource with RangeReaderBuilder
        try (RangeReader reader = HttpRangeReader.builder(basicAuthUri)
                .basicAuth(BASIC_AUTH_USER, BASIC_AUTH_PASSWORD)
                .build()) {

            // Verify size
            assertEquals(TEST_FILE_SIZE, reader.size(), "File size should match");

            // Read some data
            ByteBuffer buffer = reader.readRange(0, 1024);
            assertEquals(1024, buffer.remaining(), "Should read 1024 bytes");
        }
    }

    // ----- Digest Authentication Tests -----
    // Nginx doesn't support digest auth by default, so we'll skip these tests

    // ----- Bearer Token Authentication Tests -----

    @Test
    void testBearerTokenNoCredentials() {
        // Test that accessing bearer token protected resource without token fails
        assertThrows(
                IOException.class,
                () -> HttpRangeReader.of(bearerTokenUri),
                "Accessing bearer token protected resource without token should throw IOException");
    }

    @Test
    void testBearerTokenWithCorrectToken() throws IOException {
        // Test accessing bearer token protected resource with correct token
        BearerTokenAuthentication auth = new BearerTokenAuthentication(BEARER_TOKEN);

        try (RangeReader reader =
                HttpRangeReader.builder(bearerTokenUri).authentication(auth).build()) {
            // Verify size
            assertEquals(TEST_FILE_SIZE, reader.size(), "File size should match");

            // Read some data
            ByteBuffer buffer = reader.readRange(0, 1024);
            assertEquals(1024, buffer.remaining(), "Should read 1024 bytes");
        }
    }

    @Test
    void testBearerTokenWithIncorrectToken() {
        // Test that accessing bearer token protected resource with incorrect token fails
        BearerTokenAuthentication auth = new BearerTokenAuthentication("wrong-token");

        assertThrows(
                IOException.class,
                () -> HttpRangeReader.builder(bearerTokenUri)
                        .authentication(auth)
                        .build(),
                "Accessing bearer token protected resource with incorrect token should throw IOException");
    }

    @Test
    void testBearerTokenWithBuilder() throws IOException {
        // Test accessing bearer token protected resource with RangeReaderBuilder
        try (RangeReader reader = HttpRangeReader.builder()
                .uri(bearerTokenUri)
                .bearerToken(BEARER_TOKEN)
                .build()) {

            // Verify size
            assertEquals(TEST_FILE_SIZE, reader.size(), "File size should match");

            // Read some data
            ByteBuffer buffer = reader.readRange(0, 1024);
            assertEquals(1024, buffer.remaining(), "Should read 1024 bytes");
        }
    }

    // ----- API Key Authentication Tests -----

    @Test
    void testApiKeyNoCredentials() {
        // Test that accessing API key protected resource without API key fails
        assertThrows(
                IOException.class,
                () -> HttpRangeReader.builder(apiKeyUri).build(),
                "Accessing API key protected resource without API key should throw IOException");
    }

    @Test
    void testApiKeyWithCorrectKey() throws IOException {
        // Test accessing API key protected resource with correct API key
        try (RangeReader reader = HttpRangeReader.builder(apiKeyUri)
                .apiKey(API_KEY_HEADER, API_KEY)
                .build()) {
            // Verify size
            assertEquals(TEST_FILE_SIZE, reader.size(), "File size should match");

            // Read some data
            ByteBuffer buffer = reader.readRange(0, 1024);
            assertEquals(1024, buffer.remaining(), "Should read 1024 bytes");
        }
    }

    @Test
    void testApiKeyWithIncorrectKey() {
        // Test that accessing API key protected resource with incorrect API key fails
        assertThrows(
                IOException.class,
                () -> HttpRangeReader.builder(apiKeyUri)
                        .apiKey(API_KEY_HEADER, "wrong-key")
                        .build(),
                "Accessing API key protected resource with incorrect API key should throw IOException");
    }

    @Test
    void testApiKeyWithBuilder() throws IOException {
        // Test accessing API key protected resource with RangeReaderBuilder
        try (RangeReader reader = HttpRangeReader.builder(apiKeyUri)
                .apiKey(API_KEY_HEADER, API_KEY)
                .build()) {

            // Verify size
            assertEquals(TEST_FILE_SIZE, reader.size(), "File size should match");

            // Read some data
            ByteBuffer buffer = reader.readRange(0, 1024);
            assertEquals(1024, buffer.remaining(), "Should read 1024 bytes");
        }
    }

    // ----- Custom Header Authentication Tests -----

    @Test
    void testCustomHeaderNoCredentials() {
        // Test that accessing custom header protected resource without header fails
        assertThrows(
                IOException.class,
                () -> HttpRangeReader.of(customHeaderUri),
                "Accessing custom header protected resource without header should throw IOException");
    }

    @Test
    void testCustomHeaderWithCorrectValue() throws IOException {
        // Test accessing custom header protected resource with correct header value
        Map<String, String> headers = new HashMap<>();
        headers.put(CUSTOM_HEADER_NAME, CUSTOM_HEADER_VALUE);
        CustomHeaderAuthentication auth = new CustomHeaderAuthentication(headers);

        try (RangeReader reader =
                HttpRangeReader.builder(customHeaderUri).authentication(auth).build()) {
            // Verify size
            assertEquals(TEST_FILE_SIZE, reader.size(), "File size should match");

            // Read some data
            ByteBuffer buffer = reader.readRange(0, 1024);
            assertEquals(1024, buffer.remaining(), "Should read 1024 bytes");
        }
    }

    @Test
    void testCustomHeaderWithIncorrectValue() {
        // Test that accessing custom header protected resource with incorrect header value fails
        Map<String, String> headers = new HashMap<>();
        headers.put(CUSTOM_HEADER_NAME, "wrong-value");
        CustomHeaderAuthentication auth = new CustomHeaderAuthentication(headers);

        assertThrows(
                IOException.class,
                () -> HttpRangeReader.builder(customHeaderUri)
                        .authentication(auth)
                        .build(),
                "Accessing custom header protected resource with incorrect header value should throw IOException");
    }

    // ----- Multiple Range Requests Tests -----

    @Test
    void testMultipleAuthenticatedRangeRequests() throws IOException {
        // Test multiple consecutive authenticated range requests
        try (RangeReader reader = HttpRangeReader.builder(basicAuthUri)
                .basicAuth(API_KEY, BASIC_AUTH_PASSWORD)
                .build()) {
            // Make multiple range requests
            for (int i = 0; i < 5; i++) {
                int offset = i * 5000;
                int length = 1000;

                ByteBuffer buffer = reader.readRange(offset, length);
                assertEquals(length, buffer.remaining(), "Range request " + i + " should return " + length + " bytes");
            }
        }
    }
}
