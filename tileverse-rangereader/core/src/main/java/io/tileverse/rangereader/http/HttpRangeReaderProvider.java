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

import static io.tileverse.rangereader.spi.RangeReaderParameter.SUBGROUP_AUTHENTICATION;

import io.tileverse.rangereader.RangeReader;
import io.tileverse.rangereader.http.HttpRangeReader.Builder;
import io.tileverse.rangereader.spi.AbstractRangeReaderProvider;
import io.tileverse.rangereader.spi.RangeReaderConfig;
import io.tileverse.rangereader.spi.RangeReaderParameter;
import io.tileverse.rangereader.spi.RangeReaderProvider;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Optional;

/**
 * A {@link RangeReaderProvider} for creating {@link HttpRangeReader} instances
 * that read from generic HTTP/HTTPS servers.
 *
 * <h2>Supported URI Schemes</h2>
 * <ul>
 *   <li>{@code http://} - Unencrypted HTTP connections</li>
 *   <li>{@code https://} - Encrypted HTTPS connections (recommended)</li>
 * </ul>
 *
 * <h2>Authentication Methods</h2>
 * <p>This provider supports multiple HTTP authentication methods:
 *
 * <h3>1. HTTP Basic Authentication</h3>
 * <p>Configure using {@link #HTTP_AUTH_USERNAME} and {@link #HTTP_AUTH_PASSWORD}.
 * Credentials are base64-encoded and sent with each request using the
 * {@code Authorization: Basic} header.
 *
 * <pre>{@code
 * Properties config = new Properties();
 * config.setProperty("io.tileverse.rangereader.http.username", "myuser");
 * config.setProperty("io.tileverse.rangereader.http.password", "mypassword");
 * RangeReader reader = RangeReaderFactory.create(uri, config);
 * }</pre>
 *
 * <h3>2. Bearer Token Authentication (OAuth 2.0 / JWT)</h3>
 * <p>Configure using {@link #HTTP_AUTH_BEARER_TOKEN}. The token is sent with each
 * request using the {@code Authorization: Bearer} header.
 *
 * <pre>{@code
 * Properties config = new Properties();
 * config.setProperty("io.tileverse.rangereader.http.bearer-token", "eyJhbGciOi...");
 * RangeReader reader = RangeReaderFactory.create(uri, config);
 * }</pre>
 *
 * <h3>3. API Key Authentication (Custom Headers)</h3>
 * <p>Configure using {@link #HTTP_AUTH_API_KEY_HEADERNAME}, {@link #HTTP_AUTH_API_KEY},
 * and optionally {@link #HTTP_AUTH_API_KEY_VALUE_PREFIX}. Allows sending custom
 * authentication headers commonly used by REST APIs.
 *
 * <pre>{@code
 * Properties config = new Properties();
 * config.setProperty("io.tileverse.rangereader.http.api-key-headername", "X-API-Key");
 * config.setProperty("io.tileverse.rangereader.http.api-key", "abc123xyz");
 * RangeReader reader = RangeReaderFactory.create(uri, config);
 * }</pre>
 *
 * <h2>Connection Configuration</h2>
 * <ul>
 *   <li>{@link #HTTP_CONNECTION_TIMEOUT_MILLIS} - Connection timeout (default 5000ms)</li>
 * </ul>
 *
 * <h2>SSL/TLS Configuration</h2>
 * <ul>
 *   <li>{@link #HTTP_TRUST_ALL_SSL_CERTIFICATES} - Disable certificate validation (development only)</li>
 * </ul>
 *
 * <h2>Provider Selection</h2>
 * <p>This provider handles {@code http://} and {@code https://} URIs that are not
 * recognized as cloud provider endpoints (S3, Azure, GCS). For ambiguous cases,
 * this provider can be explicitly selected using:
 * <pre>{@code
 * config.setProperty("io.tileverse.rangereader.provider", "http");
 * }</pre>
 *
 * @see HttpRangeReader
 * @see HttpAuthentication
 */
public class HttpRangeReaderProvider extends AbstractRangeReaderProvider {

    /**
     * Key used as environment variable name to disable this range reader provider
     * <pre>
     * {@code export IO_TILEVERSE_RANGEREADER_HTTP=false}
     * </pre>
     */
    public static final String ENABLED_KEY = "IO_TILEVERSE_RANGEREADER_HTTP";
    /**
     * This range reader implementation's {@link #getId() unique identifier}
     */
    public static final String ID = "http";

    /**
     * HTTP connection timeout in milliseconds.
     * <p>Specifies the maximum time to wait when establishing a connection to the HTTP server.
     * Default is 5000ms (5 seconds).
     * <p><b>Key:</b> {@code io.tileverse.rangereader.http.timeout-millis}
     */
    public static final RangeReaderParameter<Integer> HTTP_CONNECTION_TIMEOUT_MILLIS = RangeReaderParameter.builder()
            .key("io.tileverse.rangereader.http.timeout-millis")
            .title("HTTP connection timeout in milliseconds")
            .description(
                    """
                    Specifies the maximum time to wait when establishing a connection to the HTTP server.
                    Default is 5000 milliseconds (5 seconds). Increase this value for slow or unreliable networks.
                    """)
            .type(Integer.class)
            .group(ID)
            .defaultValue(5_000)
            .build();

    /**
     * Trust all SSL/TLS certificates, including self-signed certificates.
     * <p><b>WARNING:</b> This disables certificate validation and should only be used in
     * development/testing environments. In production, use properly signed certificates.
     * <p><b>Key:</b> {@code io.tileverse.rangereader.http.trust-all-certificates}
     */
    public static final RangeReaderParameter<Boolean> HTTP_TRUST_ALL_SSL_CERTIFICATES = RangeReaderParameter.builder()
            .key("io.tileverse.rangereader.http.trust-all-certificates")
            .title("Trust all SSL/TLS certificates")
            .description(
                    """
                    When set to true, disables SSL/TLS certificate validation, allowing connections to servers
                    with self-signed or invalid certificates. This should ONLY be used in development/testing
                    environments. Default is false (certificate validation enabled).
                    """)
            .type(Boolean.class)
            .group(ID)
            .build();

    /**
     * Username for HTTP Basic Authentication.
     * <p>Used in combination with {@link #HTTP_AUTH_PASSWORD} to authenticate with servers
     * that require HTTP Basic Authentication. The credentials are sent with each request
     * using the {@code Authorization: Basic} header.
     * <p><b>Key:</b> {@code io.tileverse.rangereader.http.username}
     */
    public static final RangeReaderParameter<String> HTTP_AUTH_USERNAME = RangeReaderParameter.builder()
            .key("io.tileverse.rangereader.http.username")
            .title("HTTP Basic Auth username")
            .description(
                    """
                    Username for HTTP Basic Authentication. Must be used together with the HTTP Basic Auth password parameter.
                    """)
            .type(String.class)
            .group(ID)
            .subgroup(SUBGROUP_AUTHENTICATION)
            .build();

    /**
     * Password for HTTP Basic Authentication.
     * <p>Used in combination with {@link #HTTP_AUTH_USERNAME} to authenticate with servers
     * that require HTTP Basic Authentication. The credentials are sent with each request
     * using the {@code Authorization: Basic} header.
     * <p><b>Key:</b> {@code io.tileverse.rangereader.http.password}
     */
    public static final RangeReaderParameter<String> HTTP_AUTH_PASSWORD = RangeReaderParameter.builder()
            .key("io.tileverse.rangereader.http.password")
            .title("HTTP Basic Auth password")
            .description(
                    """
                    Password for HTTP Basic Authentication. Must be used together with the HTTP Basic Auth username parameter.
                    """)
            .type(String.class)
            .group(ID)
            .subgroup(SUBGROUP_AUTHENTICATION)
            .password(true)
            .build();

    /**
     * Bearer token for OAuth 2.0 / JWT authentication.
     * <p>Provides a bearer token that will be sent with each request using the
     * {@code Authorization: Bearer <token>} header. Commonly used for OAuth 2.0 and JWT-based
     * authentication schemes.
     * <p><b>Key:</b> {@code io.tileverse.rangereader.http.bearer-token}
     */
    public static final RangeReaderParameter<String> HTTP_AUTH_BEARER_TOKEN = RangeReaderParameter.builder()
            .key("io.tileverse.rangereader.http.bearer-token")
            .title("HTTP Bearer Token")
            .description(
                    """
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
     * <p>Specifies the HTTP header name to use for API key authentication. Common values include
     * {@code X-API-Key}, {@code Authorization}, {@code api-key}, etc. Must be used together with
     * {@link #HTTP_AUTH_API_KEY}.
     * <p><b>Key:</b> {@code io.tileverse.rangereader.http.api-key-headername}
     */
    public static final RangeReaderParameter<String> HTTP_AUTH_API_KEY_HEADERNAME = RangeReaderParameter.builder()
            .key("io.tileverse.rangereader.http.api-key-headername")
            .title("API-Key header name")
            .description(
                    """
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
     * <p>The actual API key to send in the custom header specified by {@link #HTTP_AUTH_API_KEY_HEADERNAME}.
     * Optionally, a prefix can be specified using {@link #HTTP_AUTH_API_KEY_VALUE_PREFIX}.
     * <p><b>Key:</b> {@code io.tileverse.rangereader.http.api-key}
     */
    public static final RangeReaderParameter<String> HTTP_AUTH_API_KEY = RangeReaderParameter.builder()
            .key("io.tileverse.rangereader.http.api-key")
            .title("API key value")
            .description(
                    """
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
     * <p>Some APIs require a prefix before the API key value (e.g., {@code ApiKey }, {@code Token }).
     * This parameter specifies that prefix, which will be prepended to the {@link #HTTP_AUTH_API_KEY}
     * value before sending.
     * <p><b>Key:</b> {@code io.tileverse.rangereader.http.api-key-value-prefix}
     * <p><b>Example:</b> If prefix is {@code "ApiKey "} and key is {@code "abc123"}, the header
     * value will be {@code "ApiKey abc123"}.
     */
    public static final RangeReaderParameter<String> HTTP_AUTH_API_KEY_VALUE_PREFIX = RangeReaderParameter.builder()
            .key("io.tileverse.rangereader.http.api-key-value-prefix")
            .title("API key value prefix")
            .description(
                    """
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
     * @see AbstractRangeReaderProvider#MEMORY_CACHE_ENABLED
     * @see AbstractRangeReaderProvider#MEMORY_CACHE_BLOCK_ALIGNED
     * @see AbstractRangeReaderProvider#MEMORY_CACHE_BLOCK_SIZE
     */
    public HttpRangeReaderProvider() {
        super(true);
    }

    @Override
    public String getId() {
        return ID;
    }

    @Override
    public boolean isAvailable() {
        return RangeReaderProvider.isEnabled(ENABLED_KEY);
    }

    @Override
    public String getDescription() {
        return "Reads ranges from a generic HTTP/HTTPS server.";
    }

    @Override
    public boolean canProcess(RangeReaderConfig config) {
        return RangeReaderConfig.matches(config, getId(), "http", "https");
    }

    /**
     * Creates an {@link HttpRangeReader} configured with the specified options.
     *
     * <p>This method extracts HTTP-specific configuration from the provided {@link RangeReaderConfig}
     * and applies it to the reader builder:
     *
     * <ul>
     *   <li><b>Connection timeout:</b> Applied from {@link #HTTP_CONNECTION_TIMEOUT_MILLIS}</li>
     *   <li><b>SSL/TLS:</b> Trust settings from {@link #HTTP_TRUST_ALL_SSL_CERTIFICATES}</li>
     *   <li><b>Authentication:</b> Configures one of:
     *     <ul>
     *       <li>Basic Auth (if both username and password are present)</li>
     *       <li>Bearer Token (if token is present)</li>
     *       <li>API Key (if header name and key are present)</li>
     *     </ul>
     *   </li>
     * </ul>
     *
     * <p><b>Note:</b> Only one authentication method should be configured. If multiple methods
     * are provided, the precedence is: Basic Auth > Bearer Token > API Key.
     *
     * @param opts the configuration containing URI and optional HTTP-specific parameters
     * @return a configured {@link HttpRangeReader} instance
     * @throws IOException if the reader cannot be created
     */
    @Override
    protected RangeReader createInternal(RangeReaderConfig opts) throws IOException {
        Builder builder = prepareBuilder(opts);

        return builder.build();
    }

    // visible for testing
    Builder prepareBuilder(RangeReaderConfig opts) {
        URI uri = opts.uri();
        Builder builder = HttpRangeReader.builder().uri(uri);

        // Apply connection timeout
        opts.getParameter(HTTP_CONNECTION_TIMEOUT_MILLIS)
                .map(Duration::ofMillis)
                .ifPresent(builder::connectionTimeout);

        // Apply SSL/TLS trust settings
        opts.getParameter(HTTP_TRUST_ALL_SSL_CERTIFICATES).ifPresent(builder::trustAllCertificates);

        // Configure Basic Authentication if both username and password are present
        Optional<String> basicAuthUser = opts.getParameter(HTTP_AUTH_USERNAME);
        Optional<String> basicAuthPwd = opts.getParameter(HTTP_AUTH_PASSWORD);
        if (basicAuthUser.isPresent() && basicAuthPwd.isPresent()) {
            builder.basicAuth(basicAuthUser.orElseThrow(), basicAuthPwd.orElseThrow());
        }

        // Configure Bearer Token authentication if present
        opts.getParameter(HTTP_AUTH_BEARER_TOKEN).ifPresent(builder::bearerToken);

        // Configure API Key authentication if header name and key are both present
        Optional<String> apiKeyHeaderName = opts.getParameter(HTTP_AUTH_API_KEY_HEADERNAME);
        Optional<String> apiKey = opts.getParameter(HTTP_AUTH_API_KEY);
        if (apiKeyHeaderName.isPresent() && apiKey.isPresent()) {
            String valuePrefix =
                    opts.getParameter(HTTP_AUTH_API_KEY_VALUE_PREFIX).orElse(null);
            builder.apiKey(apiKeyHeaderName.orElseThrow(), apiKey.orElseThrow(), valuePrefix);
        }
        return builder;
    }
}
