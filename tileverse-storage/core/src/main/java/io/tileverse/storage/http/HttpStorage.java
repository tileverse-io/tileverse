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
import io.tileverse.storage.TransientStorageException;
import io.tileverse.storage.UnsupportedCapabilityException;
import io.tileverse.storage.WriteOptions;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
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
    private final StorageCapabilities capabilities;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Construct an {@code HttpStorage} that uses the {@link HttpClient} carried by {@code clientHandle} and applies
     * {@code authentication} to every request issued by {@link #stat}, {@link #read(String, ReadOptions)}, and the
     * {@link HttpRangeReader} returned by {@link #openRangeReader}. {@link #close()} closes the handle (which either
     * releases a cache lease or is a no-op for borrowed clients).
     */
    HttpStorage(URI baseUri, HttpClientHandle clientHandle, HttpAuthentication authentication) {
        if (baseUri == null) throw new IllegalArgumentException("baseUri required");
        this.baseUri = baseUri;
        this.clientHandle = clientHandle;
        this.authentication = authentication == null ? HttpAuthentication.NONE : authentication;
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
        return new HttpRangeReader(resolve(key), client(), authentication);
    }

    @Override
    public ReadHandle read(String key, ReadOptions options) {
        requireOpen();
        URI uri = resolve(key);
        HttpRequest request = buildGetRequest(uri, options);
        try {
            HttpResponse<InputStream> response = client().send(request, HttpResponse.BodyHandlers.ofInputStream());
            throwOnErrorStatus(uri, response);
            StorageEntry.File metadata = metadataFromHeaders(key, response);
            return new ReadHandle(response.body(), metadata);
        } catch (IOException e) {
            throw new TransientStorageException("GET failed for " + uri, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TransientStorageException("Interrupted during GET", e);
        }
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
    public OutputStream openOutputStream(String key, WriteOptions options) {
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
        if (options.offset() == 0L && options.length().isEmpty()) {
            return Optional.empty();
        }
        if (options.length().isPresent()) {
            long endInclusive = options.offset() + options.length().getAsLong() - 1L;
            return Optional.of("bytes=" + options.offset() + "-" + endInclusive);
        }
        return Optional.of("bytes=" + options.offset() + "-");
    }

    private static void throwOnErrorStatus(URI uri, HttpResponse<InputStream> response) throws IOException {
        int status = response.statusCode();
        if (status < 400) {
            return;
        }
        response.body().close();
        switch (status) {
            case 404:
                throw new NotFoundException("Not found: " + uri);
            case 412:
                throw new PreconditionFailedException("Precondition failed for: " + uri);
            case 416:
                throw new RangeNotSatisfiableException("Range not satisfiable for: " + uri);
            default:
                throw new StorageException("GET failed for " + uri + ": " + status);
        }
    }

    private static StorageEntry.File metadataFromHeaders(String key, HttpResponse<?> response) {
        long size = Math.max(
                0L, response.headers().firstValueAsLong("Content-Length").orElse(-1L));
        Optional<String> etag = response.headers().firstValue("ETag");
        Optional<String> contentType = response.headers().firstValue("Content-Type");
        Instant lastModified = parseHttpDate(response.headers().firstValue("Last-Modified"));
        return new StorageEntry.File(key, size, lastModified, etag, Optional.empty(), contentType, Map.of());
    }

    private static Instant parseHttpDate(Optional<String> rawHeader) {
        if (rawHeader.isEmpty()) {
            return Instant.EPOCH;
        }
        return DateTimeFormatter.RFC_1123_DATE_TIME.parse(rawHeader.get(), Instant::from);
    }
}
