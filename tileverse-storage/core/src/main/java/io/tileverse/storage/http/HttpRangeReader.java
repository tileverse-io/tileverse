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

import io.tileverse.io.ByteBufferPool;
import io.tileverse.io.ByteBufferPool.PooledByteBuffer;
import io.tileverse.io.ByteRange;
import io.tileverse.storage.AbstractRangeReader;
import io.tileverse.storage.AccessDeniedException;
import io.tileverse.storage.ContentRange;
import io.tileverse.storage.NotFoundException;
import io.tileverse.storage.RangeNotSatisfiableException;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.RangeRequest;
import io.tileverse.storage.StorageException;
import io.tileverse.storage.TransientStorageException;
import io.tileverse.storage.batch.BatchExecutors;
import io.tileverse.storage.batch.BatchPlanner;
import io.tileverse.storage.batch.BatchRunner;
import io.tileverse.storage.batch.CoalescingPolicy;
import io.tileverse.storage.batch.PlannedFetch;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
 *
 * <p><b>Batched Reads:</b> {@link #readRanges} reads a batch with as few round trips as the server allows: planned
 * fetches travel as multi-range GETs, and each {@code multipart/byteranges} part is routed to its fetches by
 * {@code Content-Range}.
 */
@Slf4j
final class HttpRangeReader extends AbstractRangeReader implements RangeReader {

    private final URI uri;
    private final HttpClient httpClient;
    private final HttpAuthentication authentication;

    private record Metadata(OptionalLong contentLength, Optional<String> etag, Optional<String> lastModified) {}

    /** Populated by the first HEAD (size() before any read) or the first range response. */
    private final AtomicReference<Metadata> metadata = new AtomicReference<>(null);

    /** Upper bound of range specs per multi-range GET; groups beyond it run as extra concurrent requests. */
    private static final int MAX_RANGE_SPECS_PER_REQUEST = 100;

    /** Fetch and group parallelism for batched reads. */
    private static final int MAX_CONCURRENT_FETCHES = 8;

    private static final String ETAG_HEADER = "ETag";

    private static final String LAST_MODIFIED_HEADER = "Last-Modified";

    private static final Pattern BOUNDARY_PARAMETER =
            Pattern.compile("boundary=(?:\"([^\"]+)\"|([^;\\s]+))", Pattern.CASE_INSENSITIVE);

    /**
     * Set once a server answers a multi-range GET with 200 or an uncovering single range; from then on batches run one
     * GET per fetch.
     */
    private volatile boolean multiRangeUnsupported;

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
        // Content length will be checked when size() is first called
    }

    @Override
    public OptionalLong size() {
        Metadata known = metadata.get();
        if (known == null) {
            known = metadata.updateAndGet(this::fetchMetadata);
        }
        return known.contentLength();
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
        try {
            return getRange(offset, actualLength, target);
        } catch (IOException e) {
            throw new TransientStorageException("Range read failed for " + uri, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TransientStorageException("Request was interrupted for " + uri, e);
        }
    }

    @Override
    protected CoalescingPolicy coalescingPolicy() {
        return CoalescingPolicy.httpDefaults();
    }

    @Override
    protected int maxConcurrentFetches() {
        return MAX_CONCURRENT_FETCHES;
    }

    /**
     * Reads a batch with as few round trips as the server allows: planned fetches travel as multi-range GETs
     * ({@code Range: bytes=a-b,c-d,...}, at most {@value #MAX_RANGE_SPECS_PER_REQUEST} ranges per request, extra groups
     * running concurrently on the shared batch executor), and each {@code multipart/byteranges} part is routed to its
     * fetches by {@code Content-Range} (parts may arrive reordered or coalesced). A single-range 206 covering the group
     * is consumed by streaming with gap skips.
     *
     * <p>A 200, or a single-range 206 not covering the group, closes the body unread, restores the target positions,
     * and re-runs the whole batch through the batched-read template (one GET per planned fetch); the reader remembers
     * the refusal and skips multi-range GETs from then on. A 416 means nothing in that group is satisfiable and its
     * entries report 0, exactly like the single-read 416 translation. Size, ETag, and Last-Modified are captured from
     * the first multipart response like from single-range responses.
     *
     * <p>A policy with merging disabled ({@link CoalescingPolicy#maxGapBytes()} negative) routes the whole batch
     * through the batched-read template instead, one GET per planned fetch: its direct fetches are unsorted and not
     * deduplicated, which the multipart router cannot safely group into shared multi-range GETs.
     *
     * <p>Worst-case amplification: the requested bytes plus the gaps the HTTP policy merges, at most
     * {@link CoalescingPolicy#maxFetchBytes()} per fetch.
     *
     * @param requests the ranges to read and the buffers they land in
     * @return the number of bytes read per request, in request order
     */
    @Override
    public int[] readRanges(List<RangeRequest> requests) {
        if (multiRangeUnsupported) {
            return super.readRanges(requests);
        }
        RangeRequest.validate(requests);
        if (requests.isEmpty()) {
            return new int[0];
        }
        List<PlannedFetch> fetches = BatchPlanner.plan(requests, coalescingPolicy());
        if (fetches.isEmpty()) {
            return new int[requests.size()];
        }
        if (fetches.size() == 1) {
            return BatchRunner.run(requests, fetches, this::readRange, 1, BatchExecutors::shared);
        }
        if (coalescingPolicy().maxGapBytes() < 0) {
            // merging disabled: the plan is unsorted, duplicate-preserving direct fetches, which the multipart
            // router cannot safely group into shared multi-range GETs; the template reads them one GET at a time.
            return super.readRanges(requests);
        }
        int[] initialPositions = targetPositions(requests);
        int[] counts = new int[requests.size()];
        List<List<PlannedFetch>> groups = partition(fetches, MAX_RANGE_SPECS_PER_REQUEST);
        try {
            BatchRunner.runConcurrently(
                    groups.size(),
                    group -> readGroup(groups.get(group), requests, counts),
                    MAX_CONCURRENT_FETCHES,
                    BatchExecutors::shared);
            return counts;
        } catch (MultiRangeRefused refused) {
            multiRangeUnsupported = true;
            log.debug("{} rejected a multi-range request; batches now run one GET per fetch", uri);
            restorePositions(requests, initialPositions);
            return super.readRanges(requests);
        }
    }

    /** Sends one multi-range GET for a group of planned fetches and routes its response. */
    private void readGroup(List<PlannedFetch> group, List<RangeRequest> requests, int[] counts) {
        try {
            HttpResponse<InputStream> response = sendMultiRangeRequest(group);
            routeMultiRangeResponse(response, group, requests, counts);
        } catch (IOException e) {
            throw new TransientStorageException("Multi-range read failed for " + uri, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TransientStorageException("Request was interrupted for " + uri, e);
        }
    }

    private HttpResponse<InputStream> sendMultiRangeRequest(List<PlannedFetch> group)
            throws IOException, InterruptedException {
        StringBuilder rangeSpecs = new StringBuilder("bytes=");
        for (int i = 0; i < group.size(); i++) {
            ByteRange range = group.get(i).range();
            if (i > 0) {
                rangeSpecs.append(',');
            }
            rangeSpecs.append(range.offset()).append('-').append(range.end() - 1);
        }
        HttpRequest.Builder requestBuilder =
                HttpRequest.newBuilder().GET().uri(uri).header("Range", rangeSpecs.toString());
        requestBuilder = authentication.authenticate(httpClient, requestBuilder);
        try {
            return httpClient.send(requestBuilder.build(), BodyHandlers.ofInputStream());
        } catch (HttpConnectTimeoutException timeout) {
            throw rethrow(timeout);
        }
    }

    private void routeMultiRangeResponse(
            HttpResponse<InputStream> response, List<PlannedFetch> group, List<RangeRequest> requests, int[] counts)
            throws IOException {
        int statusCode = response.statusCode();
        if (statusCode == 416) {
            // nothing in this group is satisfiable; its entries stay 0
            closeBodyQuietly(response);
            return;
        }
        if (statusCode == 200) {
            closeBodyQuietly(response);
            throw new MultiRangeRefused();
        }
        if (statusCode != 206) {
            closeBodyQuietly(response);
            throw statusFailure(statusCode);
        }
        Optional<String> boundary =
                multipartBoundary(response.headers().firstValue("Content-Type").orElse(""));
        try (InputStream body = response.body()) {
            if (boundary.isPresent()) {
                routeParts(
                        MultipartByteRangesParser.multipart(body, boundary.get()), response, group, requests, counts);
                return;
            }
            ContentRange.Bytes single = ContentRange.bytesOf(
                            response.headers().firstValue("Content-Range").orElse(null))
                    .orElse(null);
            if (single == null || !covers(single, group)) {
                throw new MultiRangeRefused();
            }
            routeParts(MultipartByteRangesParser.singlePart(body, single), response, group, requests, counts);
        }
    }

    /**
     * Returns whether a single-range response can satisfy the whole group: it starts at or before the first fetch and
     * reaches the last fetch's end, or the end of the object for a group straddling EOF.
     */
    private static boolean covers(ContentRange.Bytes single, List<PlannedFetch> group) {
        long firstNeeded = group.get(0).range().offset();
        long lastNeeded = group.get(group.size() - 1).range().end() - 1;
        if (single.firstPos() > firstNeeded) {
            return false;
        }
        if (single.lastPos() >= lastNeeded) {
            return true;
        }
        return single.total().isPresent() && single.lastPos() == single.total().getAsLong() - 1;
    }

    private void routeParts(
            MultipartByteRangesParser parser,
            HttpResponse<InputStream> response,
            List<PlannedFetch> group,
            List<RangeRequest> requests,
            int[] counts)
            throws IOException {
        ContentRange.Bytes part;
        while ((part = parser.nextPart()) != null) {
            capturePartMetadata(response, part);
            routeOnePart(parser, part, group, requests, counts);
        }
    }

    /** Scatters one part's bytes to every fetch it covers; the group is in ascending offset order. */
    private void routeOnePart(
            MultipartByteRangesParser parser,
            ContentRange.Bytes part,
            List<PlannedFetch> group,
            List<RangeRequest> requests,
            int[] counts)
            throws IOException {
        long consumed = 0;
        for (PlannedFetch fetch : group) {
            long offsetInPart = fetch.range().offset() - part.firstPos();
            if (offsetInPart < 0 || offsetInPart >= part.length()) {
                continue;
            }
            parser.skipBody(offsetInPart - consumed);
            int available = (int) Math.min(fetch.range().length(), part.length() - offsetInPart);
            try (PooledByteBuffer pooled = ByteBufferPool.heapBuffer(available)) {
                ByteBuffer scratch = pooled.buffer();
                int read = parser.readBody(scratch, available);
                fetch.scatter(scratch, read, requests, counts);
                consumed = offsetInPart + read;
            }
        }
    }

    /** Commits size, ETag, and Last-Modified from a part's Content-Range total plus the response headers, once. */
    private void capturePartMetadata(HttpResponse<InputStream> response, ContentRange.Bytes part) {
        if (metadata.get() != null || part.total().isEmpty()) {
            return;
        }
        Optional<String> etag = response.headers().firstValue(ETAG_HEADER);
        Optional<String> lastModified = response.headers().firstValue(LAST_MODIFIED_HEADER);
        metadata.compareAndSet(null, new Metadata(OptionalLong.of(part.total().getAsLong()), etag, lastModified));
    }

    /** Extracts the boundary parameter of a {@code multipart/byteranges} Content-Type, quoted or bare. */
    private static Optional<String> multipartBoundary(String contentType) {
        String value = contentType.trim();
        if (!value.regionMatches(true, 0, "multipart/byteranges", 0, "multipart/byteranges".length())) {
            return Optional.empty();
        }
        Matcher boundary = BOUNDARY_PARAMETER.matcher(value);
        if (!boundary.find()) {
            return Optional.empty();
        }
        return Optional.of(boundary.group(1) != null ? boundary.group(1) : boundary.group(2));
    }

    private static List<List<PlannedFetch>> partition(List<PlannedFetch> fetches, int groupSize) {
        List<List<PlannedFetch>> groups = new ArrayList<>();
        for (int from = 0; from < fetches.size(); from += groupSize) {
            groups.add(fetches.subList(from, Math.min(from + groupSize, fetches.size())));
        }
        return groups;
    }

    private static int[] targetPositions(List<RangeRequest> requests) {
        int[] positions = new int[requests.size()];
        for (int i = 0; i < positions.length; i++) {
            positions[i] = requests.get(i).target().position();
        }
        return positions;
    }

    private static void restorePositions(List<RangeRequest> requests, int[] positions) {
        for (int i = 0; i < positions.length; i++) {
            requests.get(i).target().position(positions[i]);
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
        captureMetadataFrom(response);
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

    private void checkStatusCode(HttpResponse<InputStream> response) {
        int statusCode = response.statusCode();
        if (statusCode == 206) {
            return;
        }
        closeBodyQuietly(response);
        if (statusCode == 200) {
            throw new StorageException("Server ignored the Range header (HTTP 200) for URI: " + uri
                    + "; range requests are not supported by this server");
        }
        if (statusCode == 416) {
            throw new RangeNotSatisfiableException("Requested range not satisfiable for URI: " + uri);
        }
        throw statusFailure(statusCode);
    }

    private StorageException statusFailure(int statusCode) {
        switch (statusCode) {
            case 401, 403:
                return new AccessDeniedException(
                        "Authentication failed for URI: " + uri + ", status code: " + statusCode);
            case 404:
                return new NotFoundException("Resource not found: " + uri);
            default:
                return new StorageException("Failed to get range from URI: " + uri + ", status code: " + statusCode);
        }
    }

    private static void closeBodyQuietly(HttpResponse<InputStream> response) {
        try (InputStream body = response.body()) {
            // drain nothing; closing releases the connection
        } catch (IOException ignored) {
            // the connection is being abandoned anyway
        }
    }

    /**
     * Captures size, ETag, and Last-Modified from the first range response, when not already known, sparing
     * {@link #size()} a HEAD request. Only commits when the {@code Content-Range} total parses: a server that omits or
     * malforms it on a 206 leaves the metadata unset, and a later {@link #size()} call then falls back to a HEAD
     * instead of memoizing an unresolved size forever.
     */
    private void captureMetadataFrom(HttpResponse<InputStream> response) {
        if (metadata.get() != null) {
            return;
        }
        OptionalLong total = ContentRange.totalOf(
                response.headers().firstValue("Content-Range").orElse(null));
        Optional<String> etag = response.headers().firstValue(ETAG_HEADER);
        Optional<String> lastModified = response.headers().firstValue(LAST_MODIFIED_HEADER);
        total.ifPresent(size -> metadata.compareAndSet(null, new Metadata(OptionalLong.of(size), etag, lastModified)));
    }

    private HttpRequest buildRangeRequest(final long offset, final int length) {
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .GET()
                .uri(uri)
                .header("Range", "bytes=" + offset + "-" + (offset + length - 1));

        requestBuilder = authentication.authenticate(httpClient, requestBuilder);

        return requestBuilder.build();
    }

    private Metadata fetchMetadata(Metadata currValue) {
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

            OptionalLong contentLength = contentLength(response);
            if (contentLength.isEmpty()) {
                log.warn("Content-Length unknown for {}", uri);
            } else if (contentLength.getAsLong() < 0) {
                contentLength = OptionalLong.empty();
            }
            Optional<String> etag = etag(response);
            Optional<String> lastModified = lastModified(response);
            return new Metadata(contentLength, etag, lastModified);
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
        } else if (statusCode == 404) {
            throw new NotFoundException("Resource not found: " + uri);
        } else if (statusCode != 200) {
            throw new StorageException("Failed to connect to URI: " + uri + ", status code: " + statusCode);
        }
    }

    private OptionalLong contentLength(HttpResponse<Void> response) {
        return response.headers().firstValueAsLong("Content-Length");
    }

    private Optional<String> etag(HttpResponse<Void> response) {
        return response.headers().firstValue(ETAG_HEADER);
    }

    private Optional<String> lastModified(HttpResponse<Void> response) {
        return response.headers().firstValue(LAST_MODIFIED_HEADER);
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

    /** Internal control-flow signal: this server cannot serve multi-range GETs; the caller falls back. */
    private static final class MultiRangeRefused extends RuntimeException {
        MultiRangeRefused() {
            super(null, null, false, false);
        }
    }
}
