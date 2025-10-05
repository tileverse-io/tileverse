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

import static io.tileverse.rangereader.http.HttpRangeReaderProvider.HTTP_AUTH_API_KEY;
import static io.tileverse.rangereader.http.HttpRangeReaderProvider.HTTP_AUTH_API_KEY_HEADERNAME;
import static io.tileverse.rangereader.http.HttpRangeReaderProvider.HTTP_AUTH_API_KEY_VALUE_PREFIX;
import static io.tileverse.rangereader.http.HttpRangeReaderProvider.HTTP_AUTH_BEARER_TOKEN;
import static io.tileverse.rangereader.http.HttpRangeReaderProvider.HTTP_AUTH_PASSWORD;
import static io.tileverse.rangereader.http.HttpRangeReaderProvider.HTTP_AUTH_USERNAME;
import static io.tileverse.rangereader.http.HttpRangeReaderProvider.HTTP_CONNECTION_TIMEOUT_MILLIS;
import static io.tileverse.rangereader.http.HttpRangeReaderProvider.HTTP_TRUST_ALL_SSL_CERTIFICATES;
import static org.assertj.core.api.Assertions.assertThat;

import io.tileverse.rangereader.http.HttpRangeReader.Builder;
import io.tileverse.rangereader.spi.RangeReaderConfig;
import java.net.URI;
import java.time.Duration;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test suite for {@link HttpRangeReaderProvider} configuration.
 *
 * <p>Tests that the provider correctly applies configuration parameters to the {@link HttpRangeReader.Builder}.
 */
class HttpRangeReaderProviderTest {

    private static final URI TEST_URI = URI.create("https://example.com/data.bin");

    private HttpRangeReaderProvider provider;

    @BeforeEach
    void setUp() {
        provider = new HttpRangeReaderProvider();
    }

    @Test
    void testDefaultConfiguration() throws Exception {
        // Create minimal config
        RangeReaderConfig config = createConfig(new Properties());

        // Prepare builder
        Builder builder = provider.prepareBuilder(config);

        // Verify defaults
        assertThat(builder).as("URI should be set").hasFieldOrPropertyWithValue("uri", TEST_URI);
        assertThat(builder)
                .as("Default connection timeout")
                .hasFieldOrPropertyWithValue("connectionTimeout", HttpRangeReader.Builder.DEFAULT_CONNECTION_TIMEOUT);
        assertThat(builder)
                .as("Trust all certificates should be false by default")
                .hasFieldOrPropertyWithValue("trustAllCertificates", false);
        assertThat(builder)
                .as("Authentication should be NONE by default")
                .hasFieldOrPropertyWithValue("authentication", HttpAuthentication.NONE);
    }

    @Test
    void testConnectionTimeout() throws Exception {
        // Create config with custom timeout
        Properties props = new Properties();
        props.setProperty(HTTP_CONNECTION_TIMEOUT_MILLIS.key(), "10000");

        RangeReaderConfig config = createConfig(props);
        Builder builder = provider.prepareBuilder(config);

        // Verify timeout
        assertThat(builder)
                .as("Connection timeout should be 10 seconds")
                .hasFieldOrPropertyWithValue("connectionTimeout", Duration.ofMillis(10000));
    }

    @Test
    void testTrustAllCertificates() throws Exception {
        // Create config with trust all certificates enabled
        Properties props = new Properties();
        props.setProperty(HTTP_TRUST_ALL_SSL_CERTIFICATES.key(), "true");

        RangeReaderConfig config = createConfig(props);
        Builder builder = provider.prepareBuilder(config);

        // Verify trust setting
        assertThat(builder)
                .as("Trust all certificates should be enabled")
                .hasFieldOrPropertyWithValue("trustAllCertificates", true);
    }

    @Test
    void testBasicAuthentication() throws Exception {
        // Create config with basic auth
        Properties props = new Properties();
        props.setProperty(HTTP_AUTH_USERNAME.key(), "testuser");
        props.setProperty(HTTP_AUTH_PASSWORD.key(), "testpass");

        RangeReaderConfig config = createConfig(props);
        Builder builder = provider.prepareBuilder(config);

        // Verify basic auth is configured
        assertThat(builder)
                .as("Authentication should be BasicAuthentication")
                .extracting("authentication")
                .isInstanceOf(BasicAuthentication.class)
                .hasFieldOrPropertyWithValue("username", "testuser")
                .hasFieldOrPropertyWithValue("password", "testpass");
    }

    @Test
    void testBasicAuthenticationWithOnlyUsername() throws Exception {
        // Create config with only username (no password)
        Properties props = new Properties();
        props.setProperty(HTTP_AUTH_USERNAME.key(), "testuser");

        RangeReaderConfig config = createConfig(props);
        Builder builder = provider.prepareBuilder(config);

        // Verify basic auth is NOT configured (both username and password are required)
        assertThat(builder)
                .as("Authentication should be NONE when only username is provided")
                .hasFieldOrPropertyWithValue("authentication", HttpAuthentication.NONE);
    }

    @Test
    void testBearerTokenAuthentication() throws Exception {
        // Create config with bearer token
        Properties props = new Properties();
        props.setProperty(HTTP_AUTH_BEARER_TOKEN.key(), "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9");

        RangeReaderConfig config = createConfig(props);
        Builder builder = provider.prepareBuilder(config);

        // Verify bearer token auth is configured
        assertThat(builder)
                .as("Authentication should be BearerTokenAuthentication")
                .extracting("authentication")
                .isInstanceOf(BearerTokenAuthentication.class)
                .hasFieldOrPropertyWithValue("token", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9");
    }

    @Test
    void testApiKeyAuthenticationWithoutPrefix() throws Exception {
        // Create config with API key (no prefix)
        Properties props = new Properties();
        props.setProperty(HTTP_AUTH_API_KEY_HEADERNAME.key(), "X-API-Key");
        props.setProperty(HTTP_AUTH_API_KEY.key(), "abc123xyz");

        RangeReaderConfig config = createConfig(props);
        Builder builder = provider.prepareBuilder(config);

        // Verify API key auth is configured
        assertThat(builder)
                .as("Authentication should be ApiKeyAuthentication")
                .extracting("authentication")
                .isInstanceOf(ApiKeyAuthentication.class)
                .hasFieldOrPropertyWithValue("headerName", "X-API-Key")
                .hasFieldOrPropertyWithValue("apiKey", "abc123xyz")
                .hasFieldOrPropertyWithValue("valuePrefix", "");
    }

    @Test
    void testApiKeyAuthenticationWithPrefix() throws Exception {
        // Create config with API key and prefix
        Properties props = new Properties();
        props.setProperty(HTTP_AUTH_API_KEY_HEADERNAME.key(), "Authorization");
        props.setProperty(HTTP_AUTH_API_KEY.key(), "abc123xyz");
        props.setProperty(HTTP_AUTH_API_KEY_VALUE_PREFIX.key(), "ApiKey ");

        RangeReaderConfig config = createConfig(props);
        Builder builder = provider.prepareBuilder(config);

        // Verify API key auth is configured with prefix
        assertThat(builder)
                .as("Authentication should be ApiKeyAuthentication with prefix")
                .extracting("authentication")
                .isInstanceOf(ApiKeyAuthentication.class)
                .hasFieldOrPropertyWithValue("headerName", "Authorization")
                .hasFieldOrPropertyWithValue("apiKey", "abc123xyz")
                .hasFieldOrPropertyWithValue("valuePrefix", "ApiKey ");
    }

    @Test
    void testApiKeyAuthenticationWithOnlyHeaderName() throws Exception {
        // Create config with only header name (no API key)
        Properties props = new Properties();
        props.setProperty(HTTP_AUTH_API_KEY_HEADERNAME.key(), "X-API-Key");

        RangeReaderConfig config = createConfig(props);
        Builder builder = provider.prepareBuilder(config);

        // Verify API key auth is NOT configured (both header name and key are required)
        assertThat(builder)
                .as("Authentication should be NONE when only header name is provided")
                .hasFieldOrPropertyWithValue("authentication", HttpAuthentication.NONE);
    }

    @Test
    void testCompleteConfiguration() throws Exception {
        // Create config with all parameters
        Properties props = new Properties();
        props.setProperty(HTTP_CONNECTION_TIMEOUT_MILLIS.key(), "15000");
        props.setProperty(HTTP_TRUST_ALL_SSL_CERTIFICATES.key(), "true");
        props.setProperty(HTTP_AUTH_BEARER_TOKEN.key(), "my-secret-token");

        RangeReaderConfig config = createConfig(props);
        Builder builder = provider.prepareBuilder(config);

        // Verify all settings
        assertThat(builder).as("URI should be set").extracting("uri").isNotNull();
        assertThat(builder)
                .as("Connection timeout should be 15 seconds")
                .hasFieldOrPropertyWithValue("connectionTimeout", Duration.ofMillis(15000));
        assertThat(builder)
                .as("Trust all certificates should be enabled")
                .hasFieldOrPropertyWithValue("trustAllCertificates", true);
        assertThat(builder)
                .as("Authentication should be BearerTokenAuthentication")
                .extracting("authentication")
                .isInstanceOf(BearerTokenAuthentication.class);
    }

    @Test
    void testAuthenticationPrecedence() throws Exception {
        // Test that when multiple auth methods are configured, they don't conflict
        // (last one wins in current implementation)
        Properties props = new Properties();
        props.setProperty(HTTP_AUTH_USERNAME.key(), "user");
        props.setProperty(HTTP_AUTH_PASSWORD.key(), "pass");
        props.setProperty(HTTP_AUTH_BEARER_TOKEN.key(), "token123");
        props.setProperty(HTTP_AUTH_API_KEY_HEADERNAME.key(), "X-API-Key");
        props.setProperty(HTTP_AUTH_API_KEY.key(), "apikey456");

        RangeReaderConfig config = createConfig(props);
        Builder builder = provider.prepareBuilder(config);

        // Current implementation: API key will be applied last
        assertThat(builder)
                .as("Last configured authentication method should be active")
                .extracting("authentication")
                .isInstanceOf(ApiKeyAuthentication.class);
    }

    // Helper methods

    private RangeReaderConfig createConfig(Properties props) {
        props.put(RangeReaderConfig.URI_KEY, TEST_URI);
        return RangeReaderConfig.fromProperties(props);
    }
}
