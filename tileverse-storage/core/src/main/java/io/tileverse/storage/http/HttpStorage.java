/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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

import io.tileverse.storage.ContentRange;
import io.tileverse.storage.CopyOptions;
import io.tileverse.storage.DeleteResult;
import io.tileverse.storage.ListOptions;
import io.tileverse.storage.NotFoundException;
import io.tileverse.storage.PreconditionFailedException;
import io.tileverse.storage.PresignWriteOptions;
import io.tileverse.storage.RangeNotSatisfiableException;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.ReadHandle;
import io.tileverse.storage.ReadOptions;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageCapabilities;
import io.tileverse.storage.StorageEntry;
import io.tileverse.storage.StorageException;
import io.tileverse.storage.StorageOutputStream;
import io.tileverse.storage.TransientStorageException;
import io.tileverse.storage.UnsupportedCapabilityException;
import io.tileverse.storage.WriteOptions;
import io.tileverse.storage.batch.BatchSettings;
import io.tileverse.storage.http.RangeCompletingInputStream.RestAnswer;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

/**
 * Read-only Storage backed by an HTTP/HTTPS origin. Listing and writes are not supported; the HTTP base URI is treated
 * as a prefix and key resolution appends the relative key to the base.
 */
final class HttpStorage implements Storage {

    private final URI baseUri;
    private final HttpClientHandle clientHandle;
    private final HttpAuthentication authentication;
    private final BatchSettings batchSettings;
    private final StorageCapabilities capabilities;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Construct an {@code HttpStorage} that uses the {@link HttpClient} of {@code clientHandle} and applies
     * {@code authentication} to every request issued by {@link #stat}, {@link #read(String, ReadOptions)}, and the
     * {@link HttpRangeReader} returned by {@link #openRangeReader}, which batches under {@code batchSettings}.
     * {@link #close()} closes the handle (which either releases a cache lease or is a no-op for borrowed clients).
     */
    HttpStorage(
            URI baseUri,
            HttpClientHandle clientHandle,
            HttpAuthentication authentication,
            BatchSettings batchSettings) {
        if (baseUri == null) {
            throw new IllegalArgumentException("baseUri required");
        }
        this.baseUri = baseUri;
        this.clientHandle = clientHandle;
        this.authentication = authentication == null ? HttpAuthentication.NONE : authentication;
        this.batchSettings = Objects.requireNonNull(batchSettings, "batchSettings cannot be null");
        this.capabilities = StorageCapabilities.builder()
                .rangeReads(true)
                .streamingReads(true)
                .stat(true)
                .strongReadAfterWrite(true)
                .build();
    }

    @Override
    public URI baseUri() {
        return baseUri;
    }

    @Override
    public StorageCapabilities capabilities() {
        return capabilities;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            clientHandle.close();
        }
    }

    HttpClient client() {
        return clientHandle.client();
    }

    @Override
    public Optional<StorageEntry.File> stat(String key) {
        requireOpen();
        URI uri = resolve(key);
        HttpRequest request = authenticated(
                        HttpRequest.newBuilder(uri).method("HEAD", HttpRequest.BodyPublishers.noBody()))
                .build();
        try {
            HttpResponse<Void> response = client().send(request, HttpResponse.BodyHandlers.discarding());
            if (response.statusCode() == 404 || response.statusCode() == 405) {
                return Optional.empty();
            }
            if (response.statusCode() >= 400) {
                throw new StorageException("HEAD failed for " + uri + ": " + response.statusCode());
            }
            return Optional.of(metadataFromHeaders(key, response));
        } catch (IOException e) {
            throw new TransientStorageException("HEAD failed for " + uri, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TransientStorageException("Interrupted during HEAD", e);
        }
    }

    @Override
    public Stream<StorageEntry> list(String prefix, ListOptions options) {
        throw new UnsupportedCapabilityException("list");
    }

    @Override
    public RangeReader openRangeReader(String key) {
        requireOpen();
        return new HttpRangeReader(resolve(key), client(), authentication, batchSettings);
    }

    @Override
    public ReadHandle read(String key, ReadOptions options) {
        requireOpen();
        URI uri = resolve(key);
        HttpRequest request = buildGetRequest(uri, options);
        try {
            HttpResponse<InputStream> response = client().send(request, HttpResponse.BodyHandlers.ofInputStream());
            checkStatus(uri, options, response);
            StorageEntry.File metadata = metadataFromHeaders(key, response);
            InputStream content = requestedContent(uri, options, response);
            return new ReadHandle(content, metadata);
        } catch (IOException e) {
            throw new TransientStorageException("GET failed for " + uri, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TransientStorageException("Interrupted during GET", e);
        }
    }

    /**
     * Fails a read unless its answer holds content: a 200 to a read of the whole object, a 206 to a ranged read. A
     * redirect reaching this check was not followed, and its body is not the object's.
     */
    private static void checkStatus(URI uri, ReadOptions options, HttpResponse<InputStream> response) {
        int status = response.statusCode();
        if (status == expectedStatus(options)) {
            return;
        }
        ResponseBodies.closeQuietly(response);
        throw statusFailure(uri, status);
    }

    private static int expectedStatus(ReadOptions options) {
        if (setsARange(options)) {
            return 206;
        }
        return 200;
    }

    /** Returns the failure of a read answered with {@code status}; a 200 fails only a ranged read. */
    private static StorageException statusFailure(URI uri, int status) {
        return switch (status) {
            case 200 -> new StorageException("Server ignored the Range header (HTTP 200) for " + uri);
            case 301, 302, 303, 307, 308 -> new StorageException("Redirect not followed for " + uri + ": " + status);
            case 404 -> new NotFoundException("Not found: " + uri);
            case 412 -> new PreconditionFailedException("Precondition failed for: " + uri);
            case 416 -> new RangeNotSatisfiableException("Range not satisfiable for: " + uri);
            default -> new StorageException("GET failed for " + uri + ": " + status);
        };
    }

    /**
     * Returns the content of an answer to a read. The answer to a ranged read must hold the requested bytes, and one
     * answering the range in part is completed with GETs of the rest, as RFC 9110 section 15.3.7 lets a server answer a
     * subset of a range. Each body must hold the bytes named by its {@code Content-Range}; without a usable one, the
     * body stands for at most the requested bytes. Every GET of the rest must answer from the version of the object
     * named by the first answer.
     */
    private InputStream requestedContent(URI uri, ReadOptions options, HttpResponse<InputStream> response) {
        if (!setsARange(options)) {
            return response.body();
        }
        Optional<ContentRange.Bytes> answered = checkRangeAnswer(uri, options, response);
        if (answered.isEmpty()) {
            return requestedBytes(uri, options, response);
        }
        ContentRange.Bytes answeredBytes = answered.get();
        InputStream body = new AnsweredRangeInputStream(response.body(), answeredBytes, uri);
        long rangeEnd = requestedEnd(options);
        if (!endsEarly(answeredBytes, rangeEnd)) {
            return body;
        }
        long endOfRead = readEnd(answeredBytes, rangeEnd);
        AnsweredVersion version = AnsweredVersion.of(response);
        return new RangeCompletingInputStream(
                body, options.offset(), endOfRead, offset -> openRestOfRange(uri, options, version, offset));
    }

    private static boolean setsARange(ReadOptions options) {
        return options.offset() > 0L || options.length().isPresent();
    }

    /**
     * Checks the answer to a ranged GET: a {@code Content-Range} naming other bytes than requested fails the read.
     * Returns the answered range, empty without a usable {@code Content-Range}.
     */
    private static Optional<ContentRange.Bytes> checkRangeAnswer(
            URI uri, ReadOptions options, HttpResponse<InputStream> response) {
        String contentRange =
                response.headers().firstValue(HttpHeaderNames.CONTENT_RANGE).orElse(null);
        Optional<ContentRange.Bytes> answered = ContentRange.bytesOf(contentRange);
        if (answered.isPresent() && !withinRange(answered.get(), options)) {
            ResponseBodies.closeQuietly(response);
            String requested = rangeHeader(options).orElse("");
            throw new StorageException("Server answered other bytes than requested (" + requested + ", Content-Range "
                    + contentRange + ") for " + uri);
        }
        return answered;
    }

    /** Returns whether an answer starts at the requested offset and ends within the range. */
    private static boolean withinRange(ContentRange.Bytes answered, ReadOptions options) {
        return answered.firstPos() == options.offset() && answered.lastPos() < requestedEnd(options);
    }

    /** Returns the body of an answer without a usable {@code Content-Range}, held to at most the requested bytes. */
    private static InputStream requestedBytes(URI uri, ReadOptions options, HttpResponse<InputStream> response) {
        if (options.length().isEmpty()) {
            return response.body();
        }
        long requestedLength = options.length().getAsLong();
        return new RequestedRangeInputStream(response.body(), requestedLength, uri);
    }

    /** Returns the offset past the last requested byte, or {@link Long#MAX_VALUE} for a read to the end. */
    private static long requestedEnd(ReadOptions options) {
        if (options.length().isEmpty()) {
            return Long.MAX_VALUE;
        }
        long length = options.length().getAsLong();
        if (length > Long.MAX_VALUE - options.offset()) {
            return Long.MAX_VALUE;
        }
        return options.offset() + length;
    }

    /** Returns whether an answer ends before the end of the range and before the end of the object. */
    private static boolean endsEarly(ContentRange.Bytes answered, long rangeEnd) {
        return answered.lastPos() < readEnd(answered, rangeEnd) - 1;
    }

    /** Returns the offset past the last byte to read: the end of the range, or of the object when it ends first. */
    private static long readEnd(ContentRange.Bytes answered, long rangeEnd) {
        long objectEnd = answered.total().orElse(Long.MAX_VALUE);
        return Math.min(rangeEnd, objectEnd);
    }

    /**
     * Opens the answer to a GET of a ranged read from {@code offset} on, or returns empty when the object ends before
     * {@code offset}. Failures are IOExceptions, as required of a stream read.
     */
    private Optional<RestAnswer> openRestOfRange(URI uri, ReadOptions options, AnsweredVersion version, long offset)
            throws IOException {
        ReadOptions rest = rangeFrom(options, offset);
        HttpRequest request = buildGetRequest(uri, rest);
        HttpResponse<InputStream> response = sendFromStream(request);
        try {
            return restOfRange(uri, rest, version, response);
        } catch (StorageException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    /** Returns {@code options} with the range cut to start at {@code offset}. */
    private static ReadOptions rangeFrom(ReadOptions options, long offset) {
        OptionalLong length = OptionalLong.empty();
        if (options.length().isPresent()) {
            length = OptionalLong.of(requestedEnd(options) - offset);
        }
        return new ReadOptions(offset, length, options.ifMatchEtag(), options.versionId(), options.ifModifiedSince());
    }

    /** Sends a GET for a stream read: an interrupt fails it with an IOException, as required of a stream read. */
    private HttpResponse<InputStream> sendFromStream(HttpRequest request) throws IOException {
        try {
            return client().send(request, HttpResponse.BodyHandlers.ofInputStream());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            InterruptedIOException interrupted = new InterruptedIOException("Interrupted during GET");
            interrupted.initCause(e);
            throw interrupted;
        }
    }

    /**
     * Returns the content of an answer to a GET of the rest from the {@code version} of the object named by the first
     * answer, as decided by {@link AnsweredVersion}, or empty when the object ends before the rest.
     */
    private static Optional<RestAnswer> restOfRange(
            URI uri, ReadOptions rest, AnsweredVersion version, HttpResponse<InputStream> response) {
        if (version.endsTheRead(response, rest.offset(), uri)) {
            return Optional.empty();
        }
        checkStatus(uri, rest, response);
        version.requireSameVersion(response, uri);
        return Optional.of(restContent(uri, rest, response));
    }

    /**
     * Returns the answer to a GET of the rest, its body held to the bytes named by its {@code Content-Range}; without a
     * usable one, the body stands for the whole rest and is held to at most its bytes.
     */
    private static RestAnswer restContent(URI uri, ReadOptions rest, HttpResponse<InputStream> response) {
        Optional<ContentRange.Bytes> answered = checkRangeAnswer(uri, rest, response);
        if (answered.isEmpty()) {
            InputStream requested = requestedBytes(uri, rest, response);
            return new RestAnswer(requested, true);
        }
        InputStream body = new AnsweredRangeInputStream(response.body(), answered.get(), uri);
        return new RestAnswer(body, false);
    }

    @Override
    public StorageEntry.File put(String key, byte[] data, WriteOptions options) {
        throw new UnsupportedCapabilityException("writes");
    }

    @Override
    public StorageEntry.File put(String key, Path source, WriteOptions options) {
        throw new UnsupportedCapabilityException("writes");
    }

    @Override
    public StorageOutputStream openOutputStream(String key, WriteOptions options) {
        throw new UnsupportedCapabilityException("writes");
    }

    @Override
    public void delete(String key) {
        throw new UnsupportedCapabilityException("writes");
    }

    @Override
    public DeleteResult deleteAll(Collection<String> keys) {
        throw new UnsupportedCapabilityException("writes");
    }

    @Override
    public StorageEntry.File copy(String srcKey, String dstKey, CopyOptions options) {
        throw new UnsupportedCapabilityException("writes");
    }

    @Override
    public StorageEntry.File copy(String srcKey, Storage dst, String dstKey, CopyOptions options) {
        throw new UnsupportedCapabilityException("writes");
    }

    @Override
    public StorageEntry.File move(String srcKey, String dstKey, CopyOptions options) {
        throw new UnsupportedCapabilityException("writes");
    }

    @Override
    public URI presignGet(String key, Duration ttl) {
        throw new UnsupportedCapabilityException("presignGet");
    }

    @Override
    public URI presignPut(String key, Duration ttl, PresignWriteOptions options) {
        throw new UnsupportedCapabilityException("presignPut");
    }

    private void requireOpen() {
        if (closed.get()) {
            throw new IllegalStateException("HttpStorage is closed");
        }
    }

    private URI resolve(String key) {
        Storage.requireSafeKey(key);
        String base = baseUri.toString();
        if (!base.endsWith("/")) {
            base = base + "/";
        }
        String stripped = key.startsWith("/") ? key.substring(1) : key;
        return URI.create(base + stripped);
    }

    private HttpRequest.Builder authenticated(HttpRequest.Builder builder) {
        return authentication.authenticate(client(), builder);
    }

    private HttpRequest buildGetRequest(URI uri, ReadOptions options) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri).GET();
        rangeHeader(options).ifPresent(value -> builder.header("Range", value));
        options.ifMatchEtag().ifPresent(etag -> builder.header("If-Match", etag));
        options.ifModifiedSince().ifPresent(instant -> builder.header("If-Modified-Since", instant.toString()));
        return authenticated(builder).build();
    }

    private static Optional<String> rangeHeader(ReadOptions options) {
        if (!setsARange(options)) {
            return Optional.empty();
        }
        if (options.length().isPresent()) {
            long endInclusive = options.offset() + options.length().getAsLong() - 1L;
            return Optional.of("bytes=" + options.offset() + "-" + endInclusive);
        }
        return Optional.of("bytes=" + options.offset() + "-");
    }

    private static StorageEntry.File metadataFromHeaders(String key, HttpResponse<?> response) {
        HttpHeaders headers = response.headers();
        long contentLength =
                headers.firstValueAsLong(HttpHeaderNames.CONTENT_LENGTH).orElse(-1L);
        long size = Math.max(0L, contentLength);
        Optional<String> etag = headers.firstValue(HttpHeaderNames.ETAG);
        Optional<String> contentType = headers.firstValue(HttpHeaderNames.CONTENT_TYPE);
        Instant lastModified = parseHttpDate(headers.firstValue(HttpHeaderNames.LAST_MODIFIED));
        return new StorageEntry.File(key, size, lastModified, etag, Optional.empty(), contentType, Map.of());
    }

    /** Returns the date of an HTTP date header, or {@link Instant#EPOCH} when the header is absent or unparseable. */
    private static Instant parseHttpDate(Optional<String> rawHeader) {
        if (rawHeader.isEmpty()) {
            return Instant.EPOCH;
        }
        try {
            return DateTimeFormatter.RFC_1123_DATE_TIME.parse(rawHeader.get(), Instant::from);
        } catch (DateTimeParseException unparseable) {
            return Instant.EPOCH;
        }
    }
}
