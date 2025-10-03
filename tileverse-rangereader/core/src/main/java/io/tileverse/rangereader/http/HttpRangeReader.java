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

import static java.util.Objects.requireNonNull;

import io.tileverse.rangereader.AbstractRangeReader;
import io.tileverse.rangereader.RangeReader;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import lombok.NonNull;

/**
 * A RangeReader implementation that reads from an HTTP(S) URL using range requests.
 * <p>
 * This class enables reading data from web servers that support HTTP range requests,
 * which is essential for efficient cloud-optimized access to large files.
 * <p>
 * By default, this implementation accepts all SSL certificates, allowing connections to
 * servers with self-signed or otherwise untrusted certificates. This can be controlled
 * through the appropriate constructor.
 * <p>
 * It also supports various authentication methods through the HttpAuthentication interface.
 * <p>
 * Uses the modern Java 11+ {@linkplain HttpClient} API for better performance and features.
 */
public class HttpRangeReader extends AbstractRangeReader implements RangeReader {

    private static final Logger LOGGER = Logger.getLogger(HttpRangeReader.class.getName());

    private final URI uri;
    private final HttpClient httpClient;
    private final HttpAuthentication authentication;

    private volatile OptionalLong contentLength;
    private volatile boolean rangeInitialized = false;
    private volatile HttpResponse<Void> cachedHeadResponse = null;

    /**
     * Creates a new HttpRangeReader with a custom HTTP client and authentication.
     *
     * @param uri The URI to read from
     * @param httpClient The HttpClient to use
     * @param authentication The authentication mechanism to use, or null for no authentication
     */
    HttpRangeReader(@NonNull URI uri, @NonNull HttpClient httpClient, HttpAuthentication authentication) {
        this.uri = requireNonNull(uri);
        this.httpClient = requireNonNull(httpClient);
        this.authentication = requireNonNull(authentication);
        // Content length and range support will be checked when size() is first called
    }

    @Override
    protected int readRangeNoFlip(final long offset, final int actualLength, ByteBuffer target) throws IOException {
        checkServerSupportsByteRanges();

        try {
            return getRange(offset, actualLength, target);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Request was interrupted", e);
        }
    }

    private int getRange(final long offset, final int length, ByteBuffer target)
            throws IOException, InterruptedException {

        final long start = System.nanoTime();
        HttpResponse<InputStream> response = sendRangeRequest(offset, length);

        int totalRead = 0;
        try (InputStream in = response.body()) {
            ReadableByteChannel channel = Channels.newChannel(in);
            int read = 0;
            while (totalRead < length) {
                read = channel.read(target);
                if (read == -1) {
                    break;
                }
                totalRead += read;
            }
        }

        if (LOGGER.isLoggable(Level.FINE)) {
            final long end = System.nanoTime();
            final long millis = Duration.ofNanos(end - start).toMillis();
            LOGGER.fine("range:[%,d +%,d], time: %,dms]".formatted(offset, length, millis));
        }
        return totalRead;
    }

    private HttpResponse<InputStream> sendRangeRequest(final long offset, final int length)
            throws IOException, InterruptedException {

        final HttpRequest request = buildRangeRequest(offset, length);

        HttpResponse<InputStream> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
        } catch (HttpConnectTimeoutException timeout) {
            throw rethrow(timeout);
        }

        checkStatusCode(response);
        checkContentLength(length, response);
        return response;
    }

    private void checkContentLength(final int length, HttpResponse<InputStream> response) {
        OptionalLong contentLength = response.headers().firstValueAsLong("Content-Length");
        contentLength.ifPresent(returns -> {
            if (returns > length) {
                throw new IllegalStateException(
                        "Server returned more data than requested. Requested %,d bytes, returned %,d"
                                .formatted(length, returns));
            }
        });
    }

    private void checkStatusCode(HttpResponse<?> response) throws IOException {
        int statusCode = response.statusCode();
        if (statusCode == 401 || statusCode == 403) {
            throw new IOException("Authentication failed for URI: " + uri + ", status code: " + statusCode);
        } else if (statusCode != 206) {
            throw new IOException("Failed to get range from URI: " + uri + ", status code: " + statusCode);
        }
    }

    private HttpRequest buildRangeRequest(final long offset, final int length) {
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .GET()
                .uri(uri)
                .header("Range", "bytes=" + offset + "-" + (offset + length - 1));

        requestBuilder = authentication.authenticate(httpClient, requestBuilder);

        return requestBuilder.build();
    }

    private void checkServerSupportsByteRanges() throws IOException {
        // Initialize range support on first read
        if (!rangeInitialized) {
            synchronized (this) {
                if (!rangeInitialized) {
                    initializeRangeSupport();
                    rangeInitialized = true;
                }
            }
        }
    }

    @Override
    public OptionalLong size() throws IOException {
        if (contentLength == null) {
            synchronized (this) {
                if (contentLength == null) {
                    initializeSize();
                }
            }
        }
        return contentLength;
    }

    /**
     * Makes a HEAD request to the server and caches the response for reuse.
     * This method is thread-safe and ensures the HEAD request is made only once.
     *
     * @return the cached HEAD response
     * @throws IOException If an I/O error occurs during the HEAD request
     */
    private HttpResponse<Void> getHeadResponse() throws IOException {
        if (cachedHeadResponse == null) {
            synchronized (this) {
                if (cachedHeadResponse == null) {
                    try {
                        HttpRequest.Builder requestBuilder =
                                HttpRequest.newBuilder().uri(uri).method("HEAD", HttpRequest.BodyPublishers.noBody());

                        // Apply authentication if provided
                        requestBuilder = authentication.authenticate(httpClient, requestBuilder);

                        HttpRequest headRequest = requestBuilder.build();
                        HttpResponse<Void> headResponse =
                                httpClient.send(headRequest, HttpResponse.BodyHandlers.discarding());

                        int statusCode = headResponse.statusCode();
                        if (statusCode == 401 || statusCode == 403) {
                            throw new IOException(
                                    "Authentication failed for URI: " + uri + ", status code: " + statusCode);
                        } else if (statusCode != 200) {
                            throw new IOException("Failed to connect to URI: " + uri + ", status code: " + statusCode);
                        }

                        cachedHeadResponse = headResponse;

                    } catch (HttpConnectTimeoutException timeout) {
                        throw rethrow(timeout);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Request was interrupted", e);
                    }
                }
            }
        }
        return cachedHeadResponse;
    }

    private IOException rethrow(HttpConnectTimeoutException timeout) {
        String duration = httpClient
                .connectTimeout()
                .map(d -> d.toMillis() + " milliseconds")
                .orElse("default timeout");

        String message = "Connection timeout after " + duration + " to " + uri;
        IOException ex = new IOException(message);
        ex.addSuppressed(timeout);
        return ex;
    }

    /**
     * Initializes the content length from the cached HEAD response.
     *
     * @throws IOException If an I/O error occurs or the content length is invalid
     */
    private void initializeSize() throws IOException {

        HttpResponse<Void> headResponse = getHeadResponse();

        // Get content length
        this.contentLength = headResponse.headers().firstValueAsLong("Content-Length");
        if (this.contentLength.isEmpty()) {
            LOGGER.warning("Content-Length unkown for " + uri);
        } else if (this.contentLength.getAsLong() < 0) {
            this.contentLength = OptionalLong.empty();
        }
    }

    /**
     * Initializes range support by checking the Accept-Ranges header from the cached HEAD response.
     *
     * @throws IOException If the server doesn't support range requests
     */
    private void initializeRangeSupport() throws IOException {
        HttpResponse<Void> headResponse = getHeadResponse();

        // Check for explicit range support denial
        List<String> acceptRanges = headResponse.headers().allValues("Accept-Ranges");
        if (acceptRanges.stream().map(String::toLowerCase).noneMatch("bytes"::equals)) {
            throw new IOException("Server explicitly does not support range requests (Accept-Ranges: none)");
        }
    }

    @Override
    public String getSourceIdentifier() {
        return uri.toString();
    }

    @Override
    public void close() {
        // HttpClient implements AutoCloseable starting with Java 21, but it also gets
        // shutdown() and shutdownNow(), the later being the one we want to immediately
        // discard ongoing requests
        if (httpClient instanceof AutoCloseable closeable) {
            try {
                Method shutDownNow = httpClient.getClass().getMethod("shutdownNow");
                shutDownNow.invoke(httpClient);
                return;
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Error shutting down HttpClient for " + uri, e);
            }
            try { // may something had gone wrong, just try close()
                closeable.close();
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Error closing HttpClient for " + uri, e);
            }
        }
    }

    /**
     * Creates a new HttpRangeReader for the specified URL string with default settings.
     *
     * <p>This is the simplest way to create an HttpRangeReader for basic HTTP/HTTPS access
     * without authentication or custom SSL configuration. Uses default SSL certificate
     * validation (does not trust all certificates).
     *
     * <p>This is equivalent to:
     * <pre>{@code
     * HttpRangeReader.builder(url).build();
     * }</pre>
     *
     * @param url the HTTP or HTTPS URL string to read from
     * @return a new HttpRangeReader instance with default configuration
     * @throws IllegalArgumentException if the URL is malformed or has an unsupported scheme
     */
    public static HttpRangeReader of(String url) {
        return builder(url).build();
    }

    /**
     * Creates a new HttpRangeReader for the specified URI with default settings.
     *
     * <p>This is the simplest way to create an HttpRangeReader for basic HTTP/HTTPS access
     * without authentication or custom SSL configuration. Uses default SSL certificate
     * validation (does not trust all certificates).
     *
     * <p>This is equivalent to:
     * <pre>{@code
     * HttpRangeReader.builder(uri).build();
     * }</pre>
     *
     * @param url the HTTP or HTTPS URI to read from
     * @return a new HttpRangeReader instance with default configuration
     * @throws IllegalArgumentException if the URI has an unsupported scheme (must be http or https)
     */
    public static HttpRangeReader of(URI url) {
        return builder(url).build();
    }

    /**
     * Creates a new HttpRangeReader for the specified URL with default settings.
     *
     * <p>This is a convenience method for creating an HttpRangeReader from a java.net.URL
     * instance. The URL is converted to a URI internally. Uses default SSL certificate
     * validation (does not trust all certificates).
     *
     * <p>This is equivalent to:
     * <pre>{@code
     * HttpRangeReader.builder(url.toURI()).build();
     * }</pre>
     *
     * @param url the HTTP or HTTPS URL to read from
     * @return a new HttpRangeReader instance with default configuration
     * @throws IllegalArgumentException if the URL cannot be converted to a valid URI
     *                                  or has an unsupported scheme (must be http or https)
     */
    public static HttpRangeReader of(URL url) {
        try {
            return builder(url.toURI()).build();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Creates a new builder for HttpRangeReader.
     *
     * @return a new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Create a builder for the given URL
     * @param url the URL to pre configure the builder for
     * @return builder ready for url
     */
    public static Builder builder(String url) {
        return new Builder(URI.create(url));
    }

    /**
     * Create a builder for the given URL
     * @param url the URL to pre configure the builder for
     * @return builder ready for url
     */
    public static Builder builder(URI url) {
        return new Builder(url);
    }

    /**
     * Builder for HttpRangeReader.
     */
    public static class Builder {

        /**
         * The default connection timeout for the HTTP client.
         */
        public static final Duration DEFAULT_CONNECTION_TIMEOUT = Duration.ofSeconds(5);

        private URI uri;
        private boolean trustAllCertificates = false;
        private HttpAuthentication authentication = HttpAuthentication.NONE;
        /**
         * The connection timeout for the HTTP client.
         */
        public Duration connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;

        private HttpClient suppliedHttpClient;

        Builder() {}

        /**
         * Creates a new builder with the specified URI.
         *
         * @param uri The URI to read from.
         */
        public Builder(URI uri) {
            this.uri = uri;
        }

        /**
         * Sets the HTTP URI.
         *
         * @param uri the HTTP URI
         * @return this builder
         */
        public Builder uri(URI uri) {
            Objects.requireNonNull(uri, "URI cannot be null");
            String scheme = uri.getScheme().toLowerCase();
            if (!"http".equals(scheme) && !"https".equals(scheme)) {
                throw new IllegalArgumentException("URI must have http or https scheme: " + uri);
            }
            this.uri = uri;
            return this;
        }

        /**
         * Sets the connection timeout for the HTTP client.
         *
         * @param connectionTimeout The connection timeout.
         * @return This builder instance.
         */
        public Builder connectionTimeout(Duration connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        /**
         * Enables trusting all SSL certificates.
         *
         * @return this builder
         */
        public Builder trustAllCertificates() {
            this.trustAllCertificates = true;
            return this;
        }

        /**
         * Sets HTTP authentication.
         *
         * @param authentication the authentication
         * @return this builder
         */
        public Builder authentication(io.tileverse.rangereader.http.HttpAuthentication authentication) {
            this.authentication = Objects.requireNonNull(authentication, "Authentication cannot be null");
            return this;
        }

        /**
         * Sets HTTP Basic Authentication.
         *
         * @param username the username
         * @param password the password
         * @return this builder
         */
        public Builder basicAuth(String username, String password) {
            this.authentication = new io.tileverse.rangereader.http.BasicAuthentication(username, password);
            return this;
        }

        /**
         * Sets HTTP Bearer Token Authentication.
         *
         * @param token the bearer token
         * @return this builder
         */
        public Builder bearerToken(String token) {
            this.authentication = new io.tileverse.rangereader.http.BearerTokenAuthentication(token);
            return this;
        }

        /**
         * Sets API Key Authentication with a custom header.
         *
         * @param headerName the name of the header (e.g., "X-API-Key")
         * @param apiKey the API key value
         * @return this builder
         */
        public Builder apiKey(String headerName, String apiKey) {
            this.authentication = new io.tileverse.rangereader.http.ApiKeyAuthentication(headerName, apiKey);
            return this;
        }

        /**
         * Sets API Key Authentication with a custom header and value prefix.
         *
         * @param headerName the name of the header (e.g., "Authorization")
         * @param apiKey the API key value
         * @param valuePrefix the prefix for the API key value (e.g., "ApiKey ")
         * @return this builder
         */
        public Builder apiKey(String headerName, String apiKey, String valuePrefix) {
            this.authentication =
                    new io.tileverse.rangereader.http.ApiKeyAuthentication(headerName, apiKey, valuePrefix);
            return this;
        }

        /**
         * Alternative to provide a pre-configured {@link HttpClient}.
         * @param client The {@link HttpClient} to use.
         * @return this
         */
        public Builder httpClient(HttpClient client) {
            this.suppliedHttpClient = client;
            return this;
        }

        /**
         * Builds the HttpRangeReader.
         *
         * @return a new HttpRangeReader instance
         */
        public HttpRangeReader build() {
            if (uri == null) {
                throw new IllegalStateException("URI must be set");
            }

            HttpClient httpClient = this.suppliedHttpClient;
            if (httpClient == null) {
                httpClient = buildClient();
            }

            return new HttpRangeReader(uri, httpClient, authentication);
        }

        private HttpClient buildClient() {
            SSLContext sslContext = createSSLContext();

            HttpClient.Builder httpClientBuilder = HttpClient.newBuilder().sslContext(sslContext);
            if (connectionTimeout == null) {
                LOGGER.log(Level.WARNING, "c");
            } else {
                httpClientBuilder.connectTimeout(connectionTimeout);
            }

            return httpClientBuilder.build();
        }

        private SSLContext createSSLContext() {
            if (trustAllCertificates) {
                try {
                    return createTrustAllCertificatesContext();
                } catch (NoSuchAlgorithmException | KeyManagementException e) {
                    LOGGER.log(Level.WARNING, "Failed to create trust-all SSL context, falling back to default", e);
                }
            }
            try {
                return SSLContext.getDefault();
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException(e);
            }
        }

        private SSLContext createTrustAllCertificatesContext() throws NoSuchAlgorithmException, KeyManagementException {
            // Create a trust manager that accepts all certificates
            TrustManager[] trustAllCerts = new TrustManager[] {
                new X509TrustManager() {
                    @Override
                    public X509Certificate[] getAcceptedIssuers() {
                        return new X509Certificate[0];
                    }

                    @Override
                    public void checkClientTrusted(X509Certificate[] certs, String authType) {
                        // Accept all
                    }

                    @Override
                    public void checkServerTrusted(X509Certificate[] certs, String authType) {
                        // Accept all
                    }
                }
            };

            // Create SSL context with our trust manager
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, trustAllCerts, new SecureRandom());
            return sslContext;
        }
    }
}
