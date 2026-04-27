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
package io.tileverse.storage;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * A root-bound view of a blob storage container (S3 bucket + optional prefix, Azure container + prefix, GCS bucket +
 * prefix, local directory, HTTP base URL). All keys passed to a Storage are relative to {@link #baseUri()}.
 *
 * <p><b>Keys</b> are forward-slash separated, have no leading slash, and use UTF-8 character semantics.
 *
 * <p><b>Thread safety:</b> implementations are thread-safe. SDK clients are shared (reference-counted) across all calls
 * on a Storage instance and across sibling Storage instances against the same account; closing the last lease releases
 * the underlying client.
 *
 * <p><b>Capabilities:</b> not every method is supported on every backend. Use {@link #capabilities()} to interrogate
 * before calling, or rely on the documented {@link UnsupportedCapabilityException} thrown by unsupported methods.
 */
public interface Storage extends Closeable {

    /**
     * The root URI this Storage is bound to (bucket / container / directory / HTTP base, with optional prefix path).
     * All keys passed to other methods are interpreted as relative to this URI.
     *
     * @return the immutable root URI; never null
     */
    URI baseUri();

    /**
     * Report what this backend instance supports. Capability flags reflect runtime detection where applicable (for
     * example, GCS HNS detection happens at open time and updates {@code realDirectories} and {@code atomicMove}).
     *
     * @return a snapshot of the backend's declared capabilities; never null
     */
    StorageCapabilities capabilities();

    /**
     * Look up metadata for {@code key} (HEAD-style request). Returns {@link Optional#empty()} if no object exists at
     * that key.
     *
     * @param key key relative to {@link #baseUri()}
     * @return the entry's metadata, or empty if the key does not exist
     * @throws StorageException on transport or authorization failures
     */
    Optional<StorageEntry.File> stat(String key);

    /**
     * Convenience predicate: {@code stat(key).isPresent()}.
     *
     * @param key key relative to {@link #baseUri()}
     * @return true iff an object exists at that key
     * @throws StorageException on transport or authorization failures
     */
    default boolean exists(String key) {
        return stat(key).isPresent();
    }

    /**
     * List entries matching {@code pattern}, where {@code pattern} is a shell-style glob (or plain prefix). See
     * {@link StoragePattern} for parsing rules.
     *
     * <p>Examples:
     *
     * <pre>{@code
     * storage.list("");                     // immediate children of root
     * storage.list("data/");                // immediate children of data/
     * storage.list("data/**");              // all descendants of data/
     * storage.list("data/**\/*.parquet");   // recursive walk + glob filter
     * storage.list("data/*.parquet");       // immediate children matching .parquet
     * }</pre>
     *
     * <p>The returned stream is lazy and holds backend pagination resources; callers MUST close it
     * (try-with-resources). Glob filtering, when present in the pattern, is applied client-side over the canonical full
     * key form.
     *
     * <p>Use {@link #list(String, ListOptions)} for paging and metadata-fetch tuning; the no-options overload uses
     * {@link ListOptions#defaults()}.
     *
     * @param pattern shell-style glob or plain prefix; may be {@code ""} for root
     * @param options pagination and metadata-fetch tuning
     * @return a lazy stream of matching entries; close to release pagination state
     * @throws UnsupportedCapabilityException if the backend does not support listing (for example, the HTTP backend)
     * @throws StorageException on transport or authorization failures
     */
    Stream<StorageEntry> list(String pattern, ListOptions options);

    /**
     * Convenience overload: list with {@link ListOptions#defaults()}.
     *
     * @param pattern shell-style glob or plain prefix
     * @return a lazy stream of matching entries; close to release pagination state
     * @throws StorageException on transport or authorization failures
     * @see #list(String, ListOptions)
     */
    default Stream<StorageEntry> list(String pattern) {
        return list(pattern, ListOptions.defaults());
    }

    /**
     * Open a thread-safe {@link RangeReader} for byte-range reads of the named blob. The reader is owned by the caller
     * and must be closed; closing the reader releases per-blob state but leaves this Storage open. Closing the Storage
     * closes the underlying SDK client and invalidates any open readers obtained from it.
     *
     * <p>The returned reader can be wrapped with the standard decorators ({@code CachingRangeReader},
     * {@code BlockAlignedRangeReader}, {@code DiskCachingRangeReader}) just like a directly-constructed reader.
     *
     * @param key key relative to {@link #baseUri()}
     * @return a thread-safe RangeReader bound to the blob
     * @throws NotFoundException if no object exists at the key
     * @throws StorageException on transport or authorization failures
     */
    RangeReader openRangeReader(String key);

    /**
     * Open a sequentially-readable {@link ReadHandle} for the blob at {@code key}, returning the stream together with
     * the post-fetch object metadata. The handle MUST be closed by the caller to release the underlying connection.
     *
     * <p>Unlike {@link #openRangeReader}, the returned content stream is not seekable and does not support concurrent
     * reads from multiple threads. Use {@code openRangeReader} when you need random access.
     *
     * @param key key relative to {@link #baseUri()}
     * @param options offset, length, conditional-read headers, version selection, and timeout
     * @return an open {@code ReadHandle} that the caller MUST close
     * @throws NotFoundException if no object exists at the key
     * @throws RangeNotSatisfiableException if {@code options.offset()} exceeds the object size
     * @throws PreconditionFailedException if a conditional-read header is not satisfied
     * @throws StorageException on transport or authorization failures
     */
    ReadHandle read(String key, ReadOptions options);

    /**
     * Convenience overload: read with {@link ReadOptions#defaults()} (full object, no conditional headers, no caller
     * timeout - empty timeout falls through to the SDK / backend default).
     *
     * @param key key relative to {@link #baseUri()}
     * @return an open ReadHandle that the caller MUST close
     * @throws StorageException on transport or authorization failures
     */
    default ReadHandle read(String key) {
        return read(key, ReadOptions.defaults());
    }

    /**
     * Atomically write {@code data} as the object at {@code key}, replacing any existing object. The object becomes
     * visible only after the call returns successfully (atomic on object stores; on local FS, an atomic rename idiom is
     * used internally).
     *
     * <p>For larger payloads, prefer {@link #put(String, Path, WriteOptions)} (which uses multipart upload
     * transparently) or {@link #openOutputStream}.
     *
     * @param key key relative to {@link #baseUri()}
     * @param data bytes to upload as the object
     * @param options content type, user metadata, conditional-write headers, etc.
     * @return metadata for the newly written object (post-write stat)
     * @throws PreconditionFailedException if {@code options.ifNotExists()} is true and the key already exists, or if
     *     {@code options.ifMatchEtag()} does not match
     * @throws StorageException on transport or authorization failures
     */
    StorageEntry.File put(String key, byte[] data, WriteOptions options);

    /**
     * Atomically upload the file at {@code source} as the object at {@code key}, replacing any existing object. On
     * cloud backends the upload uses multipart transfer above {@link StorageCapabilities#multipartThresholdBytes()};
     * pass {@code WriteOptions.builder().disableMultipart(true).build()} to force a single-shot upload.
     *
     * @param key key relative to {@link #baseUri()}
     * @param source local path to the file to upload
     * @param options content type, user metadata, conditional-write headers, etc.
     * @return metadata for the newly written object (post-write stat)
     * @throws PreconditionFailedException if a conditional-write precondition fails
     * @throws StorageException on transport, authorization, or local I/O failures
     */
    StorageEntry.File put(String key, Path source, WriteOptions options);

    /**
     * Open an {@link OutputStream} for streaming write of the object at {@code key}. The object is not visible until
     * {@code close()} returns successfully (atomic on object stores; on local FS an atomic rename-on-close idiom is
     * used). Closing the stream without writing creates a zero-length object.
     *
     * <p>The caller MUST close the stream; the close call is what triggers the upload commit on cloud backends.
     *
     * @param key key relative to {@link #baseUri()}
     * @param options content type, user metadata, conditional-write headers, etc.
     * @return an open OutputStream that the caller MUST close to commit the write
     * @throws PreconditionFailedException if {@code options.ifNotExists()} is true and the key already exists at open
     *     time (some backends defer the check to close)
     * @throws StorageException on transport, authorization, or local I/O failures
     */
    OutputStream openOutputStream(String key, WriteOptions options);

    /**
     * Convenience overload: put with {@link WriteOptions#defaults()}.
     *
     * @param key key relative to {@link #baseUri()}
     * @param data bytes to upload as the object
     * @return metadata for the newly written object (post-write stat)
     * @throws StorageException on transport or authorization failures
     * @see #put(String, byte[], WriteOptions)
     */
    default StorageEntry.File put(String key, byte[] data) {
        return put(key, data, WriteOptions.defaults());
    }

    /** Single delete; idempotent: silent if the key does not exist. */
    void delete(String key);

    /**
     * Delete many keys. Implementations chunk per backend limits (S3 1000, Azure 256, GCS ~100). Per-key success or
     * error is reported in the result; deleting a non-existent key counts as success (idempotent).
     */
    DeleteResult deleteAll(Collection<String> keys);

    /** Server-side copy within the same Storage. */
    StorageEntry.File copy(String srcKey, String dstKey, CopyOptions options);

    /**
     * Copy to another {@code Storage} owned by the same backend (e.g. across S3 buckets, across Azure containers,
     * across GCS buckets). The implementation uses server-side copy where the SDK supports it.
     *
     * <p>Cross-backend copy (e.g. S3 to GCS) throws {@code UnsupportedCapabilityException}.
     */
    StorageEntry.File copy(String srcKey, Storage dst, String dstKey, CopyOptions options);

    /**
     * Move {@code srcKey} to {@code dstKey} with the given options. If the backend supports atomic rename
     * ({@code capabilities().atomicMove()} is true) the operation is atomic; otherwise it is implemented as copy +
     * delete and is NOT atomic.
     *
     * @throws PreconditionFailedException if {@code options.ifNotExistsAtDestination()} is true and the destination
     *     already exists
     */
    StorageEntry.File move(String srcKey, String dstKey, CopyOptions options);

    /** Convenience overload: move with {@link CopyOptions#defaults()}. */
    default StorageEntry.File move(String srcKey, String dstKey) {
        return move(srcKey, dstKey, CopyOptions.defaults());
    }

    /**
     * Generate a presigned GET URL for {@code key} valid for {@code ttl}. The returned URL grants read access to the
     * named object without further authentication; share it with clients that don't have credentials.
     *
     * @param key key relative to {@link #baseUri()}
     * @param ttl how long the URL remains valid; must not exceed {@link StorageCapabilities#maxPresignTtl()}
     * @return a presigned GET URL valid for {@code ttl}
     * @throws UnsupportedCapabilityException if the backend does not support presigned URLs (local FS, HTTP, Azure
     *     DataLake Gen2)
     * @throws IllegalArgumentException if {@code ttl} exceeds the backend's maximum
     * @throws StorageException on transport or authorization failures
     */
    URI presignGet(String key, Duration ttl);

    /**
     * Generate a presigned PUT URL that allows uploading the object at {@code key} without further authentication. The
     * {@code options} parameter constrains what the uploader is allowed to write (for example, content type and user
     * metadata). Streaming knobs such as {@code disableMultipart}, {@code timeout}, and {@code contentLength} are not
     * in scope here; they govern client-side upload behavior and cannot be embedded in a URL signature.
     *
     * @param key key relative to {@link #baseUri()}
     * @param ttl how long the URL remains valid; must not exceed {@link StorageCapabilities#maxPresignTtl()}
     * @param options write constraints to embed in the presigned URL signature; see {@link PresignWriteOptions}
     * @return a presigned PUT URL valid for {@code ttl}
     * @throws UnsupportedCapabilityException if the backend does not support presigned URLs (local FS, HTTP, Azure
     *     DataLake Gen2)
     * @throws IllegalArgumentException if {@code ttl} exceeds the backend's maximum
     * @throws StorageException on transport or authorization failures
     */
    URI presignPut(String key, Duration ttl, PresignWriteOptions options);

    /**
     * Release the underlying SDK client and connection pool. After close, subsequent operations on this Storage and on
     * any {@link RangeReader} obtained from it throw {@link IllegalStateException}. Close is idempotent.
     *
     * <p>SDK clients are shared internally via reference-counted leases keyed by (provider, account, region,
     * credentials). Closing one Storage decrements its lease; the underlying client is only torn down when the last
     * sibling closes. Callers should close Storage instances when done (try-with-resources).
     */
    @Override
    void close() throws IOException;
}
