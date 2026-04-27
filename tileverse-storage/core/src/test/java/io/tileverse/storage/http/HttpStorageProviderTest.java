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
package io.tileverse.storage.http;

import static io.tileverse.storage.http.HttpStorageProvider.HTTP_AUTH_API_KEY;
import static io.tileverse.storage.http.HttpStorageProvider.HTTP_AUTH_API_KEY_HEADERNAME;
import static io.tileverse.storage.http.HttpStorageProvider.HTTP_AUTH_API_KEY_VALUE_PREFIX;
import static io.tileverse.storage.http.HttpStorageProvider.HTTP_AUTH_BEARER_TOKEN;
import static io.tileverse.storage.http.HttpStorageProvider.HTTP_AUTH_PASSWORD;
import static io.tileverse.storage.http.HttpStorageProvider.HTTP_AUTH_USERNAME;
import static io.tileverse.storage.http.HttpStorageProvider.HTTP_CONNECTION_TIMEOUT_MILLIS;
import static io.tileverse.storage.http.HttpStorageProvider.HTTP_TRUST_ALL_SSL_CERTIFICATES;
import static org.assertj.core.api.Assertions.assertThat;

import io.tileverse.storage.spi.StorageConfig;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link HttpStorageProvider#createStorage(StorageConfig)} threads HTTP-specific configuration into the
 * {@link HttpStorage} so that subsequent {@link HttpStorage#openRangeReader(String) openRangeReader} / {@code read} /
 * {@code stat} calls inherit the configured authentication, SSL trust, and connection timeout. The underlying
 * {@code HttpClient} carries SSL trust and connection timeout; the {@code HttpStorage} carries the
 * {@link HttpAuthentication}.
 */
class HttpStorageProviderTest {

    private static final URI TEST_URI = URI.create("https://example.com/data.bin");

    private HttpStorageProvider provider;

    @BeforeEach
    void setUp() {
        provider = new HttpStorageProvider();
    }

    @Test
    void defaults_ProduceNoAuthAndDefaultClient() {
        HttpStorage storage = (HttpStorage) provider.createStorage(createConfig(new Properties()));
        assertThat(storage)
                .as("Default authentication should be NONE")
                .extracting("authentication")
                .isEqualTo(HttpAuthentication.NONE);
        assertThat(storage).extracting("client").isNotNull();
    }

    @Test
    void connectionTimeout_PropagatesToHttpClient() {
        Properties props = new Properties();
        props.setProperty(HTTP_CONNECTION_TIMEOUT_MILLIS.key(), "10000");
        HttpStorage storage = (HttpStorage) provider.createStorage(createConfig(props));
        HttpClient client = storage.client();
        assertThat(client.connectTimeout()).contains(Duration.ofMillis(10000));
    }

    @Test
    void trustAllCertificates_OverridesDefaultSslContext() {
        Properties trustAll = new Properties();
        trustAll.setProperty(HTTP_TRUST_ALL_SSL_CERTIFICATES.key(), "true");
        HttpStorage trusted = (HttpStorage) provider.createStorage(createConfig(trustAll));
        HttpStorage defaulted = (HttpStorage) provider.createStorage(createConfig(new Properties()));
        HttpClient trustedClient = trusted.client();
        HttpClient defaultedClient = defaulted.client();
        assertThat(trustedClient.sslContext())
                .as("trust-all should produce a custom SSLContext distinct from the JVM default")
                .isNotSameAs(defaultedClient.sslContext());
    }

    @Test
    void basicAuthentication_RequiresBothUserAndPassword() {
        Properties both = new Properties();
        both.setProperty(HTTP_AUTH_USERNAME.key(), "testuser");
        both.setProperty(HTTP_AUTH_PASSWORD.key(), "testpass");
        HttpStorage storage = (HttpStorage) provider.createStorage(createConfig(both));
        assertThat(storage)
                .extracting("authentication")
                .isInstanceOf(BasicAuthentication.class)
                .hasFieldOrPropertyWithValue("username", "testuser")
                .hasFieldOrPropertyWithValue("password", "testpass");

        Properties userOnly = new Properties();
        userOnly.setProperty(HTTP_AUTH_USERNAME.key(), "testuser");
        HttpStorage withoutPwd = (HttpStorage) provider.createStorage(createConfig(userOnly));
        assertThat(withoutPwd).extracting("authentication").isEqualTo(HttpAuthentication.NONE);
    }

    @Test
    void bearerTokenAuthentication() {
        Properties props = new Properties();
        props.setProperty(HTTP_AUTH_BEARER_TOKEN.key(), "eyJhbGciOiJIUzI1NiJ9");
        HttpStorage storage = (HttpStorage) provider.createStorage(createConfig(props));
        assertThat(storage)
                .extracting("authentication")
                .isInstanceOf(BearerTokenAuthentication.class)
                .hasFieldOrPropertyWithValue("token", "eyJhbGciOiJIUzI1NiJ9");
    }

    @Test
    void apiKeyAuthentication_WithAndWithoutPrefix() {
        Properties noPrefix = new Properties();
        noPrefix.setProperty(HTTP_AUTH_API_KEY_HEADERNAME.key(), "X-API-Key");
        noPrefix.setProperty(HTTP_AUTH_API_KEY.key(), "abc123");
        HttpStorage s1 = (HttpStorage) provider.createStorage(createConfig(noPrefix));
        assertThat(s1)
                .extracting("authentication")
                .isInstanceOf(ApiKeyAuthentication.class)
                .hasFieldOrPropertyWithValue("headerName", "X-API-Key")
                .hasFieldOrPropertyWithValue("apiKey", "abc123");

        Properties withPrefix = new Properties();
        withPrefix.setProperty(HTTP_AUTH_API_KEY_HEADERNAME.key(), "Authorization");
        withPrefix.setProperty(HTTP_AUTH_API_KEY.key(), "xyz789");
        withPrefix.setProperty(HTTP_AUTH_API_KEY_VALUE_PREFIX.key(), "ApiKey ");
        HttpStorage s2 = (HttpStorage) provider.createStorage(createConfig(withPrefix));
        assertThat(s2)
                .extracting("authentication")
                .isInstanceOf(ApiKeyAuthentication.class)
                .hasFieldOrPropertyWithValue("headerName", "Authorization")
                .hasFieldOrPropertyWithValue("apiKey", "xyz789")
                .hasFieldOrPropertyWithValue("valuePrefix", "ApiKey ");
    }

    @Test
    void apiKeyAuthentication_RequiresBothHeaderAndKey() {
        Properties props = new Properties();
        props.setProperty(HTTP_AUTH_API_KEY_HEADERNAME.key(), "X-API-Key");
        // no key
        HttpStorage storage = (HttpStorage) provider.createStorage(createConfig(props));
        assertThat(storage).extracting("authentication").isEqualTo(HttpAuthentication.NONE);
    }

    @Test
    void authenticationPrecedence_BasicWinsOverBearerOverApiKey() {
        Properties allThree = new Properties();
        allThree.setProperty(HTTP_AUTH_USERNAME.key(), "user");
        allThree.setProperty(HTTP_AUTH_PASSWORD.key(), "pass");
        allThree.setProperty(HTTP_AUTH_BEARER_TOKEN.key(), "token");
        allThree.setProperty(HTTP_AUTH_API_KEY_HEADERNAME.key(), "X-API-Key");
        allThree.setProperty(HTTP_AUTH_API_KEY.key(), "key");
        HttpStorage storage = (HttpStorage) provider.createStorage(createConfig(allThree));
        assertThat(storage)
                .as("Basic auth has highest precedence when fully specified")
                .extracting("authentication")
                .isInstanceOf(BasicAuthentication.class);
    }

    private StorageConfig createConfig(Properties props) {
        props.put(StorageConfig.URI_KEY, TEST_URI);
        return StorageConfig.fromProperties(props);
    }
}
