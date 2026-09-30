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
import io.tileverse.storage.BatchReadResult;
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
import io.tileverse.storage.batch.BatchSettings;
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
import java.util.concurrent.atomic.AtomicLong;
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
 * {@code Content-Range}. The merge policy and the in-flight bound come from the {@link BatchSettings} of the Storage
 * the reader was opened from.
 */
@Slf4j
final class HttpRangeReader extends AbstractRangeReader implements RangeReader {

    private final URI uri;
    private final HttpClient httpClient;
    private final HttpAuthentication authentication;
    private final BatchSettings batchSettings;

    private record Metadata(OptionalLong contentLength, Optional<String> etag, Optional<String> lastModified) {}

    /** Populated by the first HEAD (size() before any read) or the first range response. */
    private final AtomicReference<Metadata> metadata = new AtomicReference<>(null);

    /** Upper bound of range specs per multi-range GET; groups beyond it run as extra concurrent requests. */
    private static final int MAX_RANGE_SPECS_PER_REQUEST = 100;

    private static final Pattern BOUNDARY_PARAMETER =
            Pattern.compile("boundary=(?:\"([^\"]+)\"|([^;\\s]+))", Pattern.CASE_INSENSITIVE);

    /**
     * Set once a server answers a multi-range GET with 200, or with a single range not holding every fetch starting
     * before the end of the object; from then on batches run one GET per fetch.
     */
    private volatile boolean multiRangeUnsupported;

    /**
     * Creates a new HttpRangeReader with a custom HTTP client and authentication, batching under the
     * {@link BatchSettings#httpDefaults() HTTP defaults}.
     *
     * @param uri The URI to read from
     * @param httpClient The HttpClient to use
     * @param authentication The authentication mechanism to use
     */
    HttpRangeReader(@NonNull URI uri, @NonNull HttpClient httpClient, HttpAuthentication authentication) {
        this(uri, httpClient, authentication, BatchSettings.httpDefaults());
    }

    /**
     * Creates a new HttpRangeReader with a custom HTTP client, authentication and batch settings.
     *
     * @param uri The URI to read from
     * @param httpClient The HttpClient to use
     * @param authentication The authentication mechanism to use
     * @param batchSettings the merge policy and in-flight bound for batched reads
     */
    HttpRangeReader(
            @NonNull URI uri,
            @NonNull HttpClient httpClient,
            HttpAuthentication authentication,
            BatchSettings batchSettings) {
        this.uri = requireNonNull(uri);
        this.httpClient = requireNonNull(httpClient);
        this.authentication = requireNonNull(authentication);
        this.batchSettings = requireNonNull(batchSettings, "batchSettings cannot be null");
        // Content length will be checked when size() is first called
    }

    BatchSettings batchSettings() {
        return batchSettings;
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
        return batchSettings.coalescingPolicy();
    }

    @Override
    protected int maxConcurrentFetches() {
        return batchSettings.concurrencyCap();
    }

    /**
     * Reads a batch with as few round trips as the server allows: planned fetches travel as multi-range GETs
     * ({@code Range: bytes=a-b,c-d,...}, at most {@value #MAX_RANGE_SPECS_PER_REQUEST} ranges per request, extra groups
     * running concurrently on the shared batch executor), and each {@code multipart/byteranges} part is routed to its
     * fetches by {@code Content-Range} (parts may arrive reordered or coalesced). A single-range 206 holding every
     * fetch starting before the end of the object is consumed by streaming with gap skips.
     *
     * <p>A 200, or a single-range 206 not holding every fetch starting before the end of the object, closes the body
     * unread, restores the target positions, and re-runs the whole batch through the batched-read template (one GET per
     * planned fetch); the reader remembers the refusal and skips multi-range GETs from then on. A fetch missing from a
     * 206 answer, or cut short before the end of the object, is read with a GET of its own; one starting at or past the
     * object size reported by the parts reads as end of file. A 416 may reject the whole group for a single range past
     * the end: the fetches starting before the object size (from the 416's {@code Content-Range}, or the size already
     * known) are read with GETs of their own, and the others report 0 like the single-read 416 translation. With no
     * size known, every fetch of the group gets a GET of its own. Size, ETag, and Last-Modified are captured from the
     * first multipart response like from single-range responses.
     *
     * <p>A policy with merging disabled ({@link CoalescingPolicy#maxGapBytes()} negative) routes the whole batch
     * through the batched-read template instead, one GET per planned fetch, in request order and duplicates included.
     *
     * <p>Worst-case amplification: the requested bytes plus the gaps the HTTP policy merges, at most
     * {@link CoalescingPolicy#maxFetchBytes()} per fetch. The result counts one fetch per GET sent and, as bytes
     * transferred, the payload of every part received and of every fetch read with a GET of its own. The fetch count
     * leaves out the extra GETs sent by a single-range read to complete an answer given in part. A refused multi-range
     * GET counts as a fetch of no bytes on top of the per-fetch GETs that replace it. After a refusal, the result also
     * counts the GETs and bytes already spent on the other groups, and the per-fetch GETs read those bytes again.
     *
     * @param requests the ranges to read and the buffers they land in
     * @return the bytes read per request, in request order, and what the call cost
     */
    @Override
    public BatchReadResult readRanges(List<RangeRequest> requests) {
        if (multiRangeUnsupported) {
            return super.readRanges(requests);
        }
        RangeRequest.validate(requests);
        if (requests.isEmpty()) {
            return BatchReadResult.EMPTY;
        }
        List<PlannedFetch> fetches = BatchPlanner.plan(requests, coalescingPolicy());
        if (fetches.isEmpty()) {
            return BatchReadResult.of(requests, new int[requests.size()], 0, 0, 0);
        }
        if (fetches.size() == 1) {
            return BatchRunner.run(requests, fetches, this::readRange, 1, BatchExecutors::shared);
        }
        if (coalescingPolicy().maxGapBytes() < 0) {
            // merging disabled: the template sends one GET per planned fetch, in request order
            return super.readRanges(requests);
        }
        int[] initialPositions = targetPositions(requests);
        int[] counts = new int[requests.size()];
        List<List<PlannedFetch>> groups = partition(fetches, MAX_RANGE_SPECS_PER_REQUEST);
        BatchCost cost = new BatchCost();
        try {
            BatchRunner.runConcurrently(
                    groups.size(),
                    group -> readGroup(groups.get(group), requests, counts, cost),
                    maxConcurrentFetches(),
                    BatchExecutors::shared);
            return cost.toResult(requests, counts);
        } catch (MultiRangeRefused refused) {
            multiRangeUnsupported = true;
            log.debug("{} rejected a multi-range request; batches now run one GET per fetch", uri);
            restorePositions(requests, initialPositions);
            BatchReadResult refusedRequests = cost.toResult(List.of(), new int[0]);
            return super.readRanges(requests).merge(refusedRequests);
        }
    }

    /**
     * Sends one multi-range GET for a group of planned fetches, routes its response, and reads every fetch left out of
     * the response with a GET of its own.
     */
    private void readGroup(List<PlannedFetch> group, List<RangeRequest> requests, int[] counts, BatchCost cost) {
        GroupAnswer answer = new GroupAnswer(group);
        try {
            requestGroup(answer, requests, counts);
        } finally {
            // a refused multi-range GET still counts as a fetch
            cost.add(1, answer.payload());
        }
        List<PlannedFetch> unanswered = answer.unansweredFetches();
        readUnansweredFetches(unanswered, requests, counts, cost);
    }

    /** Sends the group's multi-range GET and routes its response. */
    private void requestGroup(GroupAnswer answer, List<RangeRequest> requests, int[] counts) {
        try {
            HttpResponse<InputStream> response = sendMultiRangeRequest(answer.fetches());
            routeMultiRangeResponse(response, answer, requests, counts);
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

    /** Routes a group's response to its fetches and records what it answered. */
    private void routeMultiRangeResponse(
            HttpResponse<InputStream> response, GroupAnswer answer, List<RangeRequest> requests, int[] counts)
            throws IOException {
        int statusCode = response.statusCode();
        if (statusCode == 416) {
            // a server may reject the whole set of ranges for a single one past the end
            ResponseBodies.closeQuietly(response);
            answer.rangesNotSatisfiable(objectSizeAfter416(response));
            return;
        }
        if (statusCode == 200) {
            ResponseBodies.closeQuietly(response);
            throw new MultiRangeRefused();
        }
        if (statusCode != 206) {
            ResponseBodies.closeQuietly(response);
            throw statusFailure(statusCode);
        }
        String contentType =
                response.headers().firstValue(HttpHeaderNames.CONTENT_TYPE).orElse("");
        Optional<String> boundary = multipartBoundary(contentType);
        try (InputStream body = response.body()) {
            if (boundary.isPresent()) {
                MultipartByteRangesParser parser = MultipartByteRangesParser.multipart(body, boundary.get());
                routeParts(parser, response, answer, requests, counts);
                return;
            }
            String contentRange =
                    response.headers().firstValue(HttpHeaderNames.CONTENT_RANGE).orElse(null);
            ContentRange.Bytes single = ContentRange.bytesOf(contentRange).orElse(null);
            if (single == null || !answersGroup(single, answer.fetches())) {
                throw new MultiRangeRefused();
            }
            MultipartByteRangesParser parser = MultipartByteRangesParser.singlePart(body, single);
            routeParts(parser, response, answer, requests, counts);
        }
    }

    /**
     * Returns the object size given by a 416's {@code Content-Range} ("bytes &#42;/N"), or the size already known to
     * this reader when the header is absent.
     */
    private OptionalLong objectSizeAfter416(HttpResponse<InputStream> response) {
        String contentRange =
                response.headers().firstValue(HttpHeaderNames.CONTENT_RANGE).orElse(null);
        OptionalLong reported = ContentRange.totalOf(contentRange);
        if (reported.isPresent()) {
            return reported;
        }
        Metadata known = metadata.get();
        if (known == null) {
            return OptionalLong.empty();
        }
        return known.contentLength();
    }

    /**
     * Returns whether a single-range response holds every fetch of the group starting before the end of the object.
     * Servers drop the ranges past the end from a multi-range request and may answer the one left as a single range.
     */
    private static boolean answersGroup(ContentRange.Bytes single, List<PlannedFetch> group) {
        long objectEnd = single.total().orElse(Long.MAX_VALUE);
        for (PlannedFetch fetch : group) {
            if (fetch.range().offset() < objectEnd && !holdsWhole(single, fetch)) {
                return false;
            }
        }
        return true;
    }

    /** Routes every part to its fetches. */
    private void routeParts(
            MultipartByteRangesParser parser,
            HttpResponse<InputStream> response,
            GroupAnswer answer,
            List<RangeRequest> requests,
            int[] counts)
            throws IOException {
        ContentRange.Bytes part;
        while ((part = parser.nextPart()) != null) {
            capturePartMetadata(response, part);
            routeOnePart(parser, part, answer, requests, counts);
            answer.partReceived(part);
        }
    }

    /**
     * Scatters one part's bytes to every fetch held whole by the part; the group is in ascending offset order. A fetch
     * cut short before the end of the object stays unanswered: a server may answer a subset of a range.
     */
    private void routeOnePart(
            MultipartByteRangesParser parser,
            ContentRange.Bytes part,
            GroupAnswer answer,
            List<RangeRequest> requests,
            int[] counts)
            throws IOException {
        List<PlannedFetch> group = answer.fetches();
        long consumed = 0;
        for (int index = 0; index < group.size(); index++) {
            PlannedFetch fetch = group.get(index);
            if (answer.isAnswered(index) || !holdsWhole(part, fetch)) {
                continue;
            }
            long offsetInPart = fetch.range().offset() - part.firstPos();
            parser.skipBody(offsetInPart - consumed);
            int available = (int) Math.min(fetch.range().length(), part.length() - offsetInPart);
            try (PooledByteBuffer pooled = ByteBufferPool.heapBuffer(available)) {
                ByteBuffer scratch = pooled.buffer();
                int read = parser.readBody(scratch, available);
                fetch.scatter(scratch, read, requests, counts);
                consumed = offsetInPart + read;
            }
            answer.markAnswered(index);
        }
    }

    /**
     * Returns whether a part holds a fetch from its first byte through its last, or through the end of the object for a
     * fetch straddling it. A part with an unknown total must reach the fetch's end.
     */
    private static boolean holdsWhole(ContentRange.Bytes part, PlannedFetch fetch) {
        long first = fetch.range().offset();
        if (first < part.firstPos() || first > part.lastPos()) {
            return false;
        }
        return part.lastPos() >= lastByteNeeded(fetch.range().end(), part);
    }

    /** Returns the last byte of a range ending at {@code rangeEnd}, or of the object when it ends first. */
    private static long lastByteNeeded(long rangeEnd, ContentRange.Bytes answer) {
        long objectEnd = answer.total().orElse(Long.MAX_VALUE);
        return Math.min(rangeEnd, objectEnd) - 1;
    }

    /** Commits size, ETag, and Last-Modified from a part's Content-Range total plus the response headers, once. */
    private void capturePartMetadata(HttpResponse<InputStream> response, ContentRange.Bytes part) {
        if (metadata.get() != null || part.total().isEmpty()) {
            return;
        }
        Optional<String> etag = response.headers().firstValue(HttpHeaderNames.ETAG);
        Optional<String> lastModified = response.headers().firstValue(HttpHeaderNames.LAST_MODIFIED);
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

    /**
     * Reads each fetch with a GET of its own, as the batched-read template does for a planned fetch. A server may drop
     * a satisfiable range from a multipart answer or answer a subset of it.
     */
    private void readUnansweredFetches(
            List<PlannedFetch> unanswered, List<RangeRequest> requests, int[] counts, BatchCost cost) {
        if (unanswered.isEmpty()) {
            return;
        }
        // one at a time: this group already holds one of the batch's in-flight slots
        BatchReadResult reread = BatchRunner.run(requests, unanswered, this::readRange, 1, BatchExecutors::shared);
        for (PlannedFetch fetch : unanswered) {
            copySliceCounts(fetch, reread, counts);
        }
        cost.add(reread.fetches(), reread.bytesTransferred());
    }

    private static void copySliceCounts(PlannedFetch fetch, BatchReadResult source, int[] counts) {
        for (PlannedFetch.Slice slice : fetch.slices()) {
            int requestIndex = slice.requestIndex();
            counts[requestIndex] = source.bytesRead(requestIndex);
        }
    }

    /**
     * Splits the fetches, in ascending offset order, into groups of at most {@code maxGroupSize} ascending,
     * non-overlapping fetches. The part router reads each part forward only and cannot hand the same bytes to two
     * fetches of one group.
     */
    private static List<List<PlannedFetch>> partition(List<PlannedFetch> fetches, int maxGroupSize) {
        List<List<PlannedFetch>> groups = new ArrayList<>();
        List<PlannedFetch> group = new ArrayList<>();
        for (PlannedFetch fetch : fetches) {
            if (group.size() == maxGroupSize || startsBeforeGroupEnd(fetch, group)) {
                groups.add(group);
                group = new ArrayList<>();
            }
            group.add(fetch);
        }
        if (!group.isEmpty()) {
            groups.add(group);
        }
        return groups;
    }

    /** Returns whether {@code fetch} starts before the end of the last fetch of an ascending, non-overlapping group. */
    private static boolean startsBeforeGroupEnd(PlannedFetch fetch, List<PlannedFetch> group) {
        if (group.isEmpty()) {
            return false;
        }
        PlannedFetch last = group.get(group.size() - 1);
        return fetch.range().offset() < last.range().end();
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

    /**
     * Reads one range into {@code target}. RFC 9110 section 15.3.7 lets a server answer a subset of a range: the rest
     * is read with further GETs, each landing after the bytes already read, until the range or the object ends. A GET
     * of the rest answered from another version of the object than the first answer fails the read with a
     * {@link TransientStorageException}.
     */
    private int getRange(final long offset, final int length, ByteBuffer target)
            throws IOException, InterruptedException {

        final long start = System.nanoTime();
        HttpResponse<InputStream> firstResponse = sendRangeRequest(offset, length);
        requireRangeAnswer(firstResponse, length);
        AnsweredVersion version = AnsweredVersion.of(firstResponse);
        RangeAnswer answer = readAnswer(firstResponse, offset, length, target);
        captureMetadataFrom(firstResponse);
        int totalRead = answer.bytesRead();
        // an answer leaving a remainder holds at least one byte: every pass moves the range forward
        while (answer.leavesARemainder()) {
            answer = readRemainder(offset + totalRead, length - totalRead, target, version);
            totalRead += answer.bytesRead();
        }

        if (log.isDebugEnabled()) {
            long end = System.nanoTime();
            long millis = Duration.ofNanos(end - start).toMillis();
            log.debug("range:[{} +{}], time: {}ms]", offset, length, millis);
        }
        return totalRead;
    }

    /**
     * Reads the rest of a range answered in part from the {@code version} of the object named by the first answer, as
     * decided by {@link AnsweredVersion}.
     */
    private RangeAnswer readRemainder(long offset, int length, ByteBuffer target, AnsweredVersion version)
            throws IOException, InterruptedException {
        HttpResponse<InputStream> response = sendRangeRequest(offset, length);
        if (version.endsTheRead(response, offset, uri)) {
            return RangeAnswer.NOTHING;
        }
        requireRangeAnswer(response, length);
        version.requireSameVersion(response, uri);
        RangeAnswer answer = readAnswer(response, offset, length, target);
        captureMetadataFrom(response);
        return answer;
    }

    /**
     * Reads the body of an answer to a range GET into {@code target} and consumes it through its end before closing it.
     * The JDK client cancels an HTTP/2 stream whose body is closed before its final frame arrived, and servers count
     * those cancellations against their rapid-reset protection; the extra read after the requested bytes both observes
     * the end of the stream and catches a server sending more than requested.
     */
    private RangeAnswer readAnswer(HttpResponse<InputStream> response, long offset, int length, ByteBuffer target)
            throws IOException {
        int bytesRead;
        try (InputStream in = response.body();
                ReadableByteChannel channel = Channels.newChannel(in)) {
            bytesRead = readAtMost(channel, length, target);
            if (bytesRead == length && channel.read(ByteBuffer.allocate(1)) != -1) {
                throw new StorageException(
                        "Server returned more data than requested (" + length + " bytes) for URI: " + uri);
            }
        }
        String contentRange =
                response.headers().firstValue(HttpHeaderNames.CONTENT_RANGE).orElse(null);
        return checkAnswer(offset, length, bytesRead, contentRange);
    }

    /**
     * Reads up to {@code length} bytes into {@code target} at its position, stopping early at end of stream. The
     * temporary limit keeps a longer-than-requested body out of the target.
     */
    private static int readAtMost(ReadableByteChannel channel, int length, ByteBuffer target) throws IOException {
        int oldLimit = target.limit();
        target.limit(target.position() + length);
        int totalRead = 0;
        try {
            while (totalRead < length) {
                int read = channel.read(target);
                if (read == -1) {
                    break;
                }
                totalRead += read;
            }
        } finally {
            target.limit(oldLimit);
        }
        return totalRead;
    }

    /**
     * Checks the bytes read against the response's {@code Content-Range}, as RFC 9110 section 15.3.7 asks of a client.
     * Without a usable {@code Content-Range} the body stands for the requested bytes.
     */
    private RangeAnswer checkAnswer(long offset, int length, int bytesRead, String contentRange) {
        Optional<ContentRange.Bytes> parsed = ContentRange.bytesOf(contentRange);
        if (parsed.isEmpty()) {
            return RangeAnswer.withoutRemainder(bytesRead);
        }
        ContentRange.Bytes answered = parsed.get();
        if (answered.firstPos() != offset) {
            long last = offset + length - 1;
            throw new StorageException("Server answered other bytes than requested (bytes=" + offset + "-" + last
                    + ", Content-Range " + contentRange + ") for URI: " + uri);
        }
        if (bytesRead != answered.length()) {
            throw new TransientStorageException("Response body and Content-Range disagree (" + bytesRead
                    + " bytes, Content-Range " + contentRange + ") for URI: " + uri);
        }
        boolean leavesARemainder = endsEarly(answered, offset + length);
        return new RangeAnswer(bytesRead, leavesARemainder);
    }

    /** Returns whether an answer ends before the end of the range and before the end of the object. */
    private static boolean endsEarly(ContentRange.Bytes answered, long rangeEnd) {
        return answered.lastPos() < lastByteNeeded(rangeEnd, answered);
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
     * @return The HTTP response to the range request, whatever its status.
     * @throws IOException if an I/O error occurs or the connection times out.
     * @throws InterruptedException if the operation is interrupted.
     */
    private HttpResponse<InputStream> sendRangeRequest(final long offset, final int length)
            throws IOException, InterruptedException {

        final HttpRequest request = buildRangeRequest(offset, length);

        try {
            return httpClient.send(request, BodyHandlers.ofInputStream());
        } catch (HttpConnectTimeoutException timeout) {
            throw rethrow(timeout);
        }
    }

    /** Fails an answer to a range request with a status other than 206, or declaring more bytes than requested. */
    private void requireRangeAnswer(HttpResponse<InputStream> response, int requestedLength) {
        checkStatusCode(response);
        checkContentLength(requestedLength, response);
    }

    private void checkContentLength(final int requestedLength, HttpResponse<InputStream> response) {
        OptionalLong contentLength = response.headers().firstValueAsLong(HttpHeaderNames.CONTENT_LENGTH);
        if (contentLength.isEmpty()) {
            return;
        }
        long declaredLength = contentLength.getAsLong();
        if (declaredLength > requestedLength) {
            ResponseBodies.closeQuietly(response);
            throw new StorageException("Server returned more data than requested (" + requestedLength
                    + " bytes, Content-Length " + declaredLength + ") for URI: " + uri);
        }
    }

    private void checkStatusCode(HttpResponse<InputStream> response) {
        int statusCode = response.statusCode();
        if (statusCode == 206) {
            return;
        }
        ResponseBodies.closeQuietly(response);
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

    /**
     * Captures size, ETag, and Last-Modified from the first accepted range response, when not already known, sparing
     * {@link #size()} a HEAD request. Only commits when the {@code Content-Range} total parses: a server that omits or
     * malforms it on a 206 leaves the metadata unset, and a later {@link #size()} call then falls back to a HEAD
     * instead of memoizing an unresolved size forever.
     */
    private void captureMetadataFrom(HttpResponse<InputStream> response) {
        if (metadata.get() != null) {
            return;
        }
        OptionalLong total = ContentRange.totalOf(
                response.headers().firstValue(HttpHeaderNames.CONTENT_RANGE).orElse(null));
        Optional<String> etag = response.headers().firstValue(HttpHeaderNames.ETAG);
        Optional<String> lastModified = response.headers().firstValue(HttpHeaderNames.LAST_MODIFIED);
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
        return response.headers().firstValueAsLong(HttpHeaderNames.CONTENT_LENGTH);
    }

    private Optional<String> etag(HttpResponse<Void> response) {
        return response.headers().firstValue(HttpHeaderNames.ETAG);
    }

    private Optional<String> lastModified(HttpResponse<Void> response) {
        return response.headers().firstValue(HttpHeaderNames.LAST_MODIFIED);
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

    /**
     * What one group's response answered: the fetches answered whole by its parts, the object size reported by them or
     * by a 416, and their payload. Confined to the thread reading the group.
     */
    private static final class GroupAnswer {
        private final List<PlannedFetch> fetches;
        private final boolean[] answered;
        private OptionalLong objectSize = OptionalLong.empty();
        private long payload;

        GroupAnswer(List<PlannedFetch> fetches) {
            this.fetches = fetches;
            this.answered = new boolean[fetches.size()];
        }

        List<PlannedFetch> fetches() {
            return fetches;
        }

        long payload() {
            return payload;
        }

        void partReceived(ContentRange.Bytes part) {
            payload += part.length();
            if (objectSize.isEmpty()) {
                objectSize = part.total();
            }
        }

        void markAnswered(int index) {
            answered[index] = true;
        }

        boolean isAnswered(int index) {
            return answered[index];
        }

        /** Records a 416 for the whole group: no fetch was answered, and those at or past the size read as EOF. */
        void rangesNotSatisfiable(OptionalLong reportedObjectSize) {
            objectSize = reportedObjectSize;
        }

        /** Returns the fetches left unanswered, except those at or past the object size: they read as end of file. */
        List<PlannedFetch> unansweredFetches() {
            List<PlannedFetch> unanswered = new ArrayList<>();
            for (int index = 0; index < fetches.size(); index++) {
                PlannedFetch fetch = fetches.get(index);
                if (!answered[index] && !startsAtOrPastEnd(fetch)) {
                    unanswered.add(fetch);
                }
            }
            return unanswered;
        }

        private boolean startsAtOrPastEnd(PlannedFetch fetch) {
            return objectSize.isPresent() && fetch.range().offset() >= objectSize.getAsLong();
        }
    }

    /** Fetches sent and bytes transferred by the groups of one batch, added to concurrently. */
    private static final class BatchCost {
        private final AtomicLong fetches = new AtomicLong();
        private final AtomicLong bytesTransferred = new AtomicLong();

        void add(long fetchCount, long byteCount) {
            fetches.addAndGet(fetchCount);
            bytesTransferred.addAndGet(byteCount);
        }

        BatchReadResult toResult(List<RangeRequest> requests, int[] counts) {
            return BatchReadResult.of(requests, counts, fetches.get(), bytesTransferred.get(), 0);
        }
    }

    /** Internal control-flow signal: this server cannot serve multi-range GETs; the caller falls back. */
    private static final class MultiRangeRefused extends RuntimeException {
        MultiRangeRefused() {
            super(null, null, false, false);
        }
    }

    /** The bytes read from one range response, and whether the range goes on past them before the end of the object. */
    private record RangeAnswer(int bytesRead, boolean leavesARemainder) {

        static final RangeAnswer NOTHING = new RangeAnswer(0, false);

        static RangeAnswer withoutRemainder(int bytesRead) {
            return new RangeAnswer(bytesRead, false);
        }
    }
}
