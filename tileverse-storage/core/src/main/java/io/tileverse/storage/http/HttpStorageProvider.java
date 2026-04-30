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

import static io.tileverse.storage.spi.StorageParameter.SUBGROUP_AUTHENTICATION;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.spi.AbstractStorageProvider;
import io.tileverse.storage.spi.StorageConfig;
import io.tileverse.storage.spi.StorageParameter;
import io.tileverse.storage.spi.StorageProvider;
import java.net.URI;
import java.net.http.HttpClient;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Optional;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

/**
 * {@link StorageProvider} implementation for read-only HTTP/HTTPS origins. Produces an {@link HttpStorage} that exposes
 * {@code stat}, byte-range reads via {@link io.tileverse.storage.Storage#openRangeReader(String) openRangeReader}, and
 * streaming reads via {@code read}. Listing, writes, and presigning are not supported (HTTP has no general primitives
 * for them).
 *
 * <h2>Supported URI Schemes</h2>
 *
 * <ul>
 *   <li>{@code http://} - Unencrypted HTTP connections
 *   <li>{@code https://} - Encrypted HTTPS connections (recommended)
 * </ul>
 *
 * <h2>Authentication Methods</h2>
 *
 * <p>This provider supports multiple HTTP authentication methods:
 *
 * <h3>1. HTTP Basic Authentication</h3>
 *
 * <p>Configure using {@link #HTTP_AUTH_USERNAME} and {@link #HTTP_AUTH_PASSWORD}. Credentials are base64-encoded and
 * sent with each request using the {@code Authorization: Basic} header.
 *
 * <pre>{@code
 * Properties config = new Properties();
 * config.setProperty("storage.http.username", "myuser");
 * config.setProperty("storage.http.password", "mypassword");
 * try (Storage storage = StorageFactory.open(uri, config);
 *     RangeReader reader = storage.openRangeReader(key)) { ... }
 * }</pre>
 *
 * <h3>2. Bearer Token Authentication (OAuth 2.0 / JWT)</h3>
 *
 * <p>Configure using {@link #HTTP_AUTH_BEARER_TOKEN}. The token is sent with each request using the
 * {@code Authorization: Bearer} header.
 *
 * <pre>{@code
 * Properties config = new Properties();
 * config.setProperty("storage.http.bearer-token", "eyJhbGciOi...");
 * try (Storage storage = StorageFactory.open(uri, config);
 *     RangeReader reader = storage.openRangeReader(key)) { ... }
 * }</pre>
 *
 * <h3>3. API Key Authentication (Custom Headers)</h3>
 *
 * <p>Configure using {@link #HTTP_AUTH_API_KEY_HEADERNAME}, {@link #HTTP_AUTH_API_KEY}, and optionally
 * {@link #HTTP_AUTH_API_KEY_VALUE_PREFIX}. Allows sending custom authentication headers commonly used by REST APIs.
 *
 * <pre>{@code
 * Properties config = new Properties();
 * config.setProperty("storage.http.api-key-headername", "X-API-Key");
 * config.setProperty("storage.http.api-key", "abc123xyz");
 * try (Storage storage = StorageFactory.open(uri, config);
 *     RangeReader reader = storage.openRangeReader(key)) { ... }
 * }</pre>
 *
 * <h2>Connection Configuration</h2>
 *
 * <ul>
 *   <li>{@link #HTTP_CONNECTION_TIMEOUT_MILLIS} - Connection timeout (default 5000ms)
 * </ul>
 *
 * <h2>SSL/TLS Configuration</h2>
 *
 * <ul>
 *   <li>{@link #HTTP_TRUST_ALL_SSL_CERTIFICATES} - Disable certificate validation (development only)
 * </ul>
 *
 * <h2>Provider Selection</h2>
 *
 * <p>This provider handles {@code http://} and {@code https://} URIs that are not recognized as cloud provider
 * endpoints (S3, Azure, GCS). For ambiguous cases, this provider can be explicitly selected using:
 *
 * <pre>{@code
 * config.setProperty("storage.provider", "http");
 * }</pre>
 *
 * @see HttpRangeReader
 * @see HttpAuthentication
 */
public class HttpStorageProvider extends AbstractStorageProvider {

    /**
     * Key used as environment variable name to disable this range reader provider
     *
     * <pre>
     * {@code export IO_TILEVERSE_STORAGE_HTTP=false}
     * </pre>
     */
    public static final String ENABLED_KEY = "IO_TILEVERSE_STORAGE_HTTP";

    /** This range reader implementation's {@link #getId() unique identifier} */
    public static final String ID = "http";

    /**
     * HTTP connection timeout in milliseconds.
     *
     * <p>Specifies the maximum time to wait when establishing a connection to the HTTP server. Default is 5000ms (5
     * seconds).
     *
     * <p><b>Key:</b> {@code storage.http.timeout-millis}
     */
    public static final StorageParameter<Integer> HTTP_CONNECTION_TIMEOUT_MILLIS = StorageParameter.builder()
            .key("storage.http.timeout-millis")
            .title("HTTP connection timeout in milliseconds")
            .description("""
                    Specifies the maximum time to wait when establishing a connection to the HTTP server.
                    Default is 5000 milliseconds (5 seconds). Increase this value for slow or unreliable networks.
                    """)
            .type(Integer.class)
            .group(ID)
            .defaultValue(5_000)
            .build();

    /**
     * Trust all SSL/TLS certificates, including self-signed certificates.
     *
     * <p><b>WARNING:</b> This disables certificate validation and should only be used in development/testing
     * environments. In production, use properly signed certificates.
     *
     * <p><b>Key:</b> {@code storage.http.trust-all-certificates}
     */
    public static final StorageParameter<Boolean> HTTP_TRUST_ALL_SSL_CERTIFICATES = StorageParameter.builder()
            .key("storage.http.trust-all-certificates")
            .title("Trust all SSL/TLS certificates")
            .description("""
                    When set to true, disables SSL/TLS certificate validation, allowing connections to servers
                    with self-signed or invalid certificates. This should ONLY be used in development/testing
                    environments. Default is false (certificate validation enabled).
                    """)
            .type(Boolean.class)
            .group(ID)
            .build();

    /**
     * Username for HTTP Basic Authentication.
     *
     * <p>Used in combination with {@link #HTTP_AUTH_PASSWORD} to authenticate with servers that require HTTP Basic
     * Authentication. The credentials are sent with each request using the {@code Authorization: Basic} header.
     *
     * <p><b>Key:</b> {@code storage.http.username}
     */
    public static final StorageParameter<String> HTTP_AUTH_USERNAME = StorageParameter.builder()
            .key("storage.http.username")
            .title("HTTP Basic Auth username")
            .description("""
                    Username for HTTP Basic Authentication. Must be used together with the HTTP Basic Auth password parameter.
                    """)
            .type(String.class)
            .group(ID)
            .subgroup(SUBGROUP_AUTHENTICATION)
            .build();

    /**
     * Password for HTTP Basic Authentication.
     *
     * <p>Used in combination with {@link #HTTP_AUTH_USERNAME} to authenticate with servers that require HTTP Basic
     * Authentication. The credentials are sent with each request using the {@code Authorization: Basic} header.
     *
     * <p><b>Key:</b> {@code storage.http.password}
     */
    public static final StorageParameter<String> HTTP_AUTH_PASSWORD = StorageParameter.builder()
            .key("storage.http.password")
            .title("HTTP Basic Auth password")
            .description("""
                    Password for HTTP Basic Authentication. Must be used together with the HTTP Basic Auth username parameter.
                    """)
            .type(String.class)
            .group(ID)
            .subgroup(SUBGROUP_AUTHENTICATION)
            .password(true)
            .build();

    /**
     * Bearer token for OAuth 2.0 / JWT authentication.
     *
     * <p>Provides a bearer token that will be sent with each request using the {@code Authorization: Bearer <token>}
     * header. Commonly used for OAuth 2.0 and JWT-based authentication schemes.
     *
     * <p><b>Key:</b> {@code storage.http.bearer-token}
     */
    public static final StorageParameter<String> HTTP_AUTH_BEARER_TOKEN = StorageParameter.builder()
            .key("storage.http.bearer-token")
            .title("HTTP Bearer Token")
            .description("""
                    Bearer token for OAuth 2.0 or JWT authentication. The token is sent with each request \
                    using the "Authorization: Bearer <token>" header format.

                    Commonly used with modern REST APIs and cloud services.
                    """)
            .type(String.class)
            .group(ID)
            .subgroup(SUBGROUP_AUTHENTICATION)
            .password(true)
            .build();

    /**
     * Custom header name for API key authentication.
     *
     * <p>Specifies the HTTP header name to use for API key authentication. Common values include {@code X-API-Key},
     * {@code Authorization}, {@code api-key}, etc. Must be used together with {@link #HTTP_AUTH_API_KEY}.
     *
     * <p><b>Key:</b> {@code storage.http.api-key-headername}
     */
    public static final StorageParameter<String> HTTP_AUTH_API_KEY_HEADERNAME = StorageParameter.builder()
            .key("storage.http.api-key-headername")
            .title("API-Key header name")
            .description("""
                    The name of the HTTP header to use for API key authentication (e.g., "X-API-Key", \
                    "Authorization", "api-key"). Must be used together with the api-key parameter.

                    Common in REST APIs that use custom header-based authentication.
                    """)
            .type(String.class)
            .group(ID)
            .subgroup(SUBGROUP_AUTHENTICATION)
            .build();

    /**
     * API key value for custom header authentication.
     *
     * <p>The actual API key to send in the custom header specified by {@link #HTTP_AUTH_API_KEY_HEADERNAME}.
     * Optionally, a prefix can be specified using {@link #HTTP_AUTH_API_KEY_VALUE_PREFIX}.
     *
     * <p><b>Key:</b> {@code storage.http.api-key}
     */
    public static final StorageParameter<String> HTTP_AUTH_API_KEY = StorageParameter.builder()
            .key("storage.http.api-key")
            .title("API key value")
            .description("""
                    The API key value to send in the custom header. Must be used together with the \
                    api-key-headername parameter. Can optionally include a prefix specified by \
                    the "API key value prefix" parameter (e.g., "ApiKey " or "Token ").
                    """)
            .type(String.class)
            .group(ID)
            .subgroup(SUBGROUP_AUTHENTICATION)
            .password(true)
            .build();

    /**
     * Optional prefix for the API key value.
     *
     * <p>Some APIs require a prefix before the API key value (e.g., {@code ApiKey }, {@code Token }). This parameter
     * specifies that prefix, which will be prepended to the {@link #HTTP_AUTH_API_KEY} value before sending.
     *
     * <p><b>Key:</b> {@code storage.http.api-key-value-prefix}
     *
     * <p><b>Example:</b> If prefix is {@code "ApiKey "} and key is {@code "abc123"}, the header value will be
     * {@code "ApiKey abc123"}.
     */
    public static final StorageParameter<String> HTTP_AUTH_API_KEY_VALUE_PREFIX = StorageParameter.builder()
            .key("storage.http.api-key-value-prefix")
            .title("API key value prefix")
            .description("""
                    Optional prefix for the API key value (e.g., "ApiKey ", "Token "). If specified, \
                    this prefix will be prepended to the API key before sending it in the header.

                    Example: prefix "ApiKey " with key "abc123" results in header value "ApiKey abc123".
                    """)
            .type(String.class)
            .group(ID)
            .subgroup(SUBGROUP_AUTHENTICATION)
            .build();

    /**
     * Creates a new HttpRangeReaderProvider with support for caching parameters
     *
     * @see AbstractStorageProvider#MEMORY_CACHE_ENABLED
     * @see AbstractStorageProvider#MEMORY_CACHE_BLOCK_ALIGNED
     * @see AbstractStorageProvider#MEMORY_CACHE_BLOCK_SIZE
     */
    public HttpStorageProvider() {
        super(true);
    }

    @Override
    public String getId() {
        return ID;
    }

    @Override
    public boolean isAvailable() {
        return StorageProvider.isEnabled(ENABLED_KEY);
    }

    @Override
    public String getDescription() {
        return "Generic HTTP/HTTPS server provider.";
    }

    @Override
    public boolean canProcess(StorageConfig config) {
        return StorageConfig.matches(config, getId(), "http", "https");
    }

    /**
     * Opens a {@link Storage} backed by the supplied {@link HttpClient}, bypassing the SPI configuration path. Useful
     * when the caller manages their own pool, SSL context, or proxy chain.
     *
     * <p>The returned {@code Storage} <b>borrows</b> the supplied client; closing the {@code Storage} does NOT close
     * the client. The caller retains ownership.
     *
     * @param baseUri base URI under which {@link Storage#openRangeReader(String) openRangeReader} keys are resolved
     * @param client a pre-configured HTTP client; not closed by the returned {@code Storage}
     * @return a borrowed-client {@code HttpStorage}
     */
    public static Storage open(URI baseUri, HttpClient client) {
        return open(baseUri, client, HttpAuthentication.NONE);
    }

    /**
     * Opens a {@link Storage} backed by the supplied {@link HttpClient} and {@link HttpAuthentication}, bypassing the
     * SPI configuration path.
     *
     * <p>The returned {@code Storage} <b>borrows</b> both the supplied client and authentication strategy; closing the
     * {@code Storage} does NOT close the client. The caller retains ownership.
     *
     * @param baseUri base URI under which {@link Storage#openRangeReader(String) openRangeReader} keys are resolved
     * @param client a pre-configured HTTP client; not closed by the returned {@code Storage}
     * @param authentication authentication strategy to apply to every request; pass {@link HttpAuthentication#NONE} for
     *     none
     * @return a borrowed-client {@code HttpStorage}
     */
    public static Storage open(URI baseUri, HttpClient client, HttpAuthentication authentication) {
        return new HttpStorage(baseUri, client, authentication);
    }

    @Override
    public Storage createStorage(StorageConfig config) {
        URI uri = config.uri();
        if (uri == null) {
            throw new IllegalArgumentException("StorageConfig.uri() is required for HttpStorage");
        }
        return new HttpStorage(uri, buildClient(config), buildAuthentication(config));
    }

    @Override
    public RangeReader openRangeReader(StorageConfig leafConfig) {
        URI uri = leafConfig.uri();
        if (uri == null) {
            throw new IllegalArgumentException("StorageConfig.uri() is required");
        }
        // HTTP doesn't have a container concept; build a reader directly against the leaf URL. The Java HttpClient
        // doesn't need explicit close, so no OwnedRangeReader wrapping is required.
        return new HttpRangeReader(uri, buildClient(leafConfig), buildAuthentication(leafConfig));
    }

    private static HttpClient buildClient(StorageConfig config) {
        HttpClient.Builder clientBuilder = HttpClient.newBuilder();
        config.getParameter(HTTP_CONNECTION_TIMEOUT_MILLIS)
                .map(Duration::ofMillis)
                .ifPresent(clientBuilder::connectTimeout);
        if (config.getParameter(HTTP_TRUST_ALL_SSL_CERTIFICATES).orElse(false)) {
            clientBuilder.sslContext(trustAllSslContext());
        }
        return clientBuilder.build();
    }

    /**
     * Builds an {@link SSLContext} that accepts any server certificate without chain validation. Only used when
     * {@link #HTTP_TRUST_ALL_SSL_CERTIFICATES} is enabled (development-only escape hatch for self-signed certs whose
     * issuing CA isn't in the JDK truststore).
     *
     * <p>Note: this disables the certificate-chain check only, not hostname verification. JDK {@link HttpClient}
     * applies endpoint identification ({@code SSLParameters.endpointIdentificationAlgorithm = "HTTPS"}) regardless, so
     * a self-signed cert whose subject doesn't match the request host still fails. Importing the cert into a truststore
     * is the more robust fix for any non-throwaway use; this method exists for the snake-oil-cert case.
     */
    private static SSLContext trustAllSslContext() {
        TrustManager trustAll = new X509TrustManager() {
            @Override
            public void checkClientTrusted(X509Certificate[] chain, String authType) {
                // no-op: accept any client cert (we are the client; this method is unused here)
            }

            @Override
            public void checkServerTrusted(X509Certificate[] chain, String authType) {
                // no-op: accept any server cert without chain validation (the whole point of trust-all)
            }

            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
            }
        };
        try {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, new TrustManager[] {trustAll}, new SecureRandom());
            return sslContext;
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Failed to configure trust-all SSL context", e);
        }
    }

    private static HttpAuthentication buildAuthentication(StorageConfig config) {
        Optional<String> basicAuthUser = config.getParameter(HTTP_AUTH_USERNAME);
        Optional<String> basicAuthPwd = config.getParameter(HTTP_AUTH_PASSWORD);
        if (basicAuthUser.isPresent() && basicAuthPwd.isPresent()) {
            return new BasicAuthentication(basicAuthUser.orElseThrow(), basicAuthPwd.orElseThrow());
        }
        if (config.getParameter(HTTP_AUTH_BEARER_TOKEN).isPresent()) {
            return new BearerTokenAuthentication(
                    config.getParameter(HTTP_AUTH_BEARER_TOKEN).orElseThrow());
        }
        Optional<String> apiKeyHeader = config.getParameter(HTTP_AUTH_API_KEY_HEADERNAME);
        Optional<String> apiKey = config.getParameter(HTTP_AUTH_API_KEY);
        if (apiKeyHeader.isPresent() && apiKey.isPresent()) {
            String prefix = config.getParameter(HTTP_AUTH_API_KEY_VALUE_PREFIX).orElse(null);
            return new ApiKeyAuthentication(apiKeyHeader.orElseThrow(), apiKey.orElseThrow(), prefix);
        }
        return HttpAuthentication.NONE;
    }
}
