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

import static java.util.Objects.requireNonNull;

import io.tileverse.storage.AbstractRangeReader;
import io.tileverse.storage.AccessDeniedException;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.StorageException;
import io.tileverse.storage.TransientStorageException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandler;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * A RangeReader implementation that reads from an HTTP(S) URL using range requests.
 *
 * <p>This class enables reading data from web servers that support HTTP range requests, which is essential for
 * efficient cloud-optimized access to large files.
 *
 * <p>By default, this implementation accepts all SSL certificates, allowing connections to servers with self-signed or
 * otherwise untrusted certificates. This can be controlled through the appropriate constructor.
 *
 * <p>It also supports various authentication methods through the HttpAuthentication interface.
 *
 * <p>Uses the modern Java 11+ {@linkplain HttpClient} API for better performance and features.
 */
@Slf4j
final class HttpRangeReader extends AbstractRangeReader implements RangeReader {

    private final URI uri;
    private final HttpClient httpClient;
    private final HttpAuthentication authentication;

    /** @see #getHeadResponse() */
    private final AtomicReference<HeadResponse> headResponse = new AtomicReference<>(null);

    private record HeadResponse(
            boolean supportsByteRange,
            OptionalLong contentLength,
            Optional<String> etag,
            Optional<String> lastModified) {}

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
    public OptionalLong size() {
        return getHeadResponse().contentLength();
    }

    @Override
    public String getSourceIdentifier() {
        return uri.toString();
    }

    @Override
    public void close() {
        // HttpClient is owned by HttpStorage (and ultimately by HttpClientCache)
        // per-reader close must not shut it down.
        // Mirrors S3RangeReader / AzureBlobRangeReader / GoogleCloudStorageRangeReader.
    }

    @Override
    protected int readRangeNoFlip(final long offset, final int actualLength, ByteBuffer target) {
        checkServerSupportsByteRanges();

        try {
            return getRange(offset, actualLength, target);
        } catch (IOException e) {
            throw new TransientStorageException("Range read failed for " + uri, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TransientStorageException("Request was interrupted for " + uri, e);
        }
    }

    private int getRange(final long offset, final int length, ByteBuffer target)
            throws IOException, InterruptedException {

        final long start = System.nanoTime();
        HttpResponse<InputStream> response = sendRangeRequest(offset, length);

        int totalRead = 0;
        try (InputStream in = response.body();
                ReadableByteChannel channel = Channels.newChannel(in)) {
            int read = 0;
            while (totalRead < length) {
                read = channel.read(target);
                if (read == -1) {
                    break;
                }
                totalRead += read;
            }
        }

        if (log.isDebugEnabled()) {
            long end = System.nanoTime();
            long millis = Duration.ofNanos(end - start).toMillis();
            log.debug("range:[{} +{}], time: {}ms]", offset, length, millis);
        }
        return totalRead;
    }

    /**
     * Fetches a specific byte range synchronously using {@link HttpClient#send(HttpRequest, BodyHandler)}.
     *
     * <p><b>Memory Efficiency:</b> This method uses {@link HttpResponse.BodyHandlers#ofInputStream()} to minimize heap
     * pressure. Unlike {@code ofByteArray()}, which accumulates the entire range into a single contiguous byte array,
     * the {@code InputStream} approach provides a streaming view over the client's internal {@code List<ByteBuffer>}.
     * This avoids redundant copies and large heap allocations during the traversal of massive files.
     *
     * <p><b>Thread Scheduling:</b>
     *
     * <ul>
     *   <li><b>Virtual Threads (Java 21+):</b> This method is highly efficient. When blocking on I/O, the virtual
     *       thread is unmounted, freeing the underlying carrier thread for other tasks.
     *   <li><b>Platform Threads (Java 17):</b> This method blocks the operating system thread for the duration of the
     *       request. High concurrency with platform threads may lead to increased memory usage due to stack overhead
     *       and potential thread exhaustion.
     * </ul>
     *
     * <p><b>Efficiency:</b> This synchronous approach provides performance parity with {@link HttpClient#sendAsync()}
     * because both utilize the {@code HttpClient}'s internal NIO-based executor for I/O operations. By using
     * {@code send()}, the application reduces heap pressure by avoiding {@code CompletableFuture} allocations and
     * lambda capture states.
     *
     * @param offset The starting byte position.
     * @param length The number of bytes to fetch.
     * @return The HTTP response containing the {@link InputStream} of the range.
     * @throws IOException if an I/O error occurs or the connection times out.
     * @throws InterruptedException if the operation is interrupted.
     */
    private HttpResponse<InputStream> sendRangeRequest(final long offset, final int length)
            throws IOException, InterruptedException {

        final HttpRequest request = buildRangeRequest(offset, length);

        HttpResponse<InputStream> response;
        try {
            response = httpClient.send(request, BodyHandlers.ofInputStream());
        } catch (HttpConnectTimeoutException timeout) {
            throw rethrow(timeout);
        }
        checkStatusCode(response);
        checkContentLength(length, response);
        return response;
    }

    private void checkContentLength(final int requestedLength, HttpResponse<InputStream> response) {
        OptionalLong contentLength = response.headers().firstValueAsLong("Content-Length");
        contentLength.ifPresent(returns -> {
            if (returns > requestedLength) {
                throw new IllegalStateException(
                        "Server returned more data than requested. Requested %,d bytes, returned %,d"
                                .formatted(requestedLength, returns));
            }
        });
    }

    private void checkStatusCode(HttpResponse<?> response) {
        int statusCode = response.statusCode();
        switch (statusCode) {
            case 206:
                // all good
                break;
            case 401, 403:
                throw new AccessDeniedException(
                        "Authentication failed for URI: " + uri + ", status code: " + statusCode);
            default:
                throw new StorageException("Failed to get range from URI: " + uri + ", status code: " + statusCode);
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

    private void checkServerSupportsByteRanges() {
        HeadResponse head = getHeadResponse();
        if (!head.supportsByteRange()) {
            throw new StorageException("Server explicitly does not support range requests (Accept-Ranges: none)");
        }
    }

    /**
     * Makes a HEAD request to the server and caches the response for reuse. This method is thread-safe and ensures the
     * HEAD request is made only once.
     *
     * @return the cached HEAD response
     * @throws StorageException If a storage error occurs during the HEAD request
     */
    private HeadResponse getHeadResponse() {

        HeadResponse response = headResponse.get();
        if (response == null) {
            response = headResponse.updateAndGet(this::fetchHeadResponse);
        }
        return response;
    }

    private HeadResponse fetchHeadResponse(HeadResponse currValue) {
        if (currValue != null) {
            // another thread already populated the cache while we were racing into updateAndGet, skip the redundant
            // HEAD and adopt their result.
            return currValue;
        }
        try {
            HttpRequest.Builder requestBuilder =
                    HttpRequest.newBuilder().uri(uri).method("HEAD", BodyPublishers.noBody());

            requestBuilder = authentication.authenticate(httpClient, requestBuilder);

            HttpRequest request = requestBuilder.build();
            HttpResponse<Void> response = httpClient.send(request, BodyHandlers.discarding());

            check200StatusCode(response);

            final boolean supportsByteRanges = supportsByteRanges(response);
            if (!supportsByteRanges) {
                log.warn("{} does not support byte ranges", uri);
            }
            OptionalLong contentLength = contentLength(response);
            if (contentLength.isEmpty()) {
                log.warn("Content-Length unknown for {}", uri);
            } else if (contentLength.getAsLong() < 0) {
                contentLength = OptionalLong.empty();
            }
            Optional<String> etag = etag(response);
            Optional<String> lastModified = lastModified(response);
            return new HeadResponse(supportsByteRanges, contentLength, etag, lastModified);
        } catch (HttpConnectTimeoutException timeout) {
            throw rethrow(timeout);
        } catch (IOException e) {
            throw new TransientStorageException("HEAD request failed for " + uri, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TransientStorageException("Request was interrupted for " + uri, e);
        }
    }

    private void check200StatusCode(HttpResponse<Void> response) {
        final int statusCode = response.statusCode();
        if (statusCode == 401 || statusCode == 403) {
            throw new AccessDeniedException("Authentication failed for URI: " + uri + ", status code: " + statusCode);
        } else if (statusCode != 200) {
            throw new StorageException("Failed to connect to URI: " + uri + ", status code: " + statusCode);
        }
    }

    private boolean supportsByteRanges(HttpResponse<Void> response) {
        List<String> acceptRanges = response.headers().allValues("Accept-Ranges");
        return acceptRanges.stream().anyMatch("bytes"::equalsIgnoreCase);
    }

    private OptionalLong contentLength(HttpResponse<Void> response) {
        return response.headers().firstValueAsLong("Content-Length");
    }

    private Optional<String> etag(HttpResponse<Void> response) {
        return response.headers().firstValue("ETag");
    }

    private Optional<String> lastModified(HttpResponse<Void> response) {
        return response.headers().firstValue("Last-Modified");
    }

    private TransientStorageException rethrow(HttpConnectTimeoutException timeout) {
        String duration = httpClient
                .connectTimeout()
                .map(d -> d.toMillis() + " milliseconds")
                .orElse("default timeout");

        String message = "Connection timeout after " + duration + " to " + uri;
        TransientStorageException ex = new TransientStorageException(message);
        ex.addSuppressed(timeout);
        return ex;
    }
}
