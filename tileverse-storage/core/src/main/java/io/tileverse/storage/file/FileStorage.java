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
package io.tileverse.storage.file;

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
import io.tileverse.storage.StoragePattern;
import io.tileverse.storage.UnsupportedCapabilityException;
import io.tileverse.storage.WriteOptions;
import java.io.File;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Local filesystem implementation of {@link Storage}.
 *
 * <p><b>Atomic writes</b>: {@link #put(String, byte[], WriteOptions)} and {@link #openOutputStream} write to a sibling
 * tempfile in the same directory and atomically rename to the destination on completion. The target object is not
 * visible until the rename succeeds.
 *
 * <p><b>Interrupt hazard</b>: Java's {@code FileChannel} (used by the underlying {@code FileRangeReader} and by
 * streaming reads/writes here) is an {@link java.nio.channels.InterruptibleChannel}. If a reading or writing thread is
 * interrupted via {@link Thread#interrupt()}, the channel closes and a
 * {@link java.nio.channels.ClosedByInterruptException} is thrown. Subsequent operations on that channel throw
 * {@code ClosedChannelException}. This makes concurrent cancellation hostile to long-running file I/O. The
 * implementation mitigates by reopening channels on demand (matching the pattern in {@code FileRangeReader}).
 */
final class FileStorage implements Storage {

    private final Path root;
    private final URI baseUri;
    private final Duration idleTimeout;
    private final StorageCapabilities capabilities;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    FileStorage(Path root) {
        this(root, FileRangeReader.DEFAULT_IDLE_TIMEOUT);
    }

    FileStorage(Path root, Duration idleTimeout) {
        this.root = root.toAbsolutePath().normalize();
        this.idleTimeout = Objects.requireNonNull(idleTimeout, "idleTimeout");
        this.baseUri = this.root.toUri();
        this.capabilities = StorageCapabilities.builder()
                .rangeReads(true)
                .streamingReads(true)
                .stat(true)
                .list(true)
                .hierarchicalList(true)
                .realDirectories(true)
                .writes(true)
                .conditionalWrite(true)
                .bulkDelete(true)
                .bulkDeleteBatchLimit(Integer.MAX_VALUE)
                .deleteReportsExistence(true)
                .serverSideCopy(true)
                .atomicMove(true)
                .strongReadAfterWrite(true)
                .userMetadata(false)
                .multipartUpload(false)
                .multipartThresholdBytes(0L)
                .presignedUrls(false)
                .versioning(false)
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
        closed.set(true);
    }

    private void requireOpen() {
        if (closed.get()) {
            throw new IllegalStateException("FileStorage is closed");
        }
    }

    Path resolve(String key) {
        Storage.requireSafeKey(key);
        Path resolved = root.resolve(key).normalize();
        // Defense-in-depth: the lexical requireSafeKey check above catches '..' segments split on '/', but on Windows
        // backslash is also a path separator that Path.resolve interprets, and a key whose normalized form is somehow
        // outside root (symlink target, ".." that survived because of an unusual key shape) must still be rejected
        // before we touch the filesystem.
        if (!resolved.startsWith(root)) {
            throw new IllegalArgumentException("Key escapes storage root: " + key);
        }
        return resolved;
    }

    @Override
    public Optional<StorageEntry.File> stat(String key) {
        requireOpen();
        Path p = resolve(key);
        if (!Files.exists(p) || Files.isDirectory(p)) {
            return Optional.empty();
        }
        try {
            BasicFileAttributes attrs = Files.readAttributes(p, BasicFileAttributes.class);
            String contentTypeRaw = Files.probeContentType(p);
            return Optional.of(new StorageEntry.File(
                    key,
                    attrs.size(),
                    attrs.lastModifiedTime().toInstant(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.ofNullable(contentTypeRaw),
                    Map.of()));
        } catch (IOException e) {
            throw new StorageException("stat failed for key: " + key, e);
        }
    }

    @Override
    public Stream<StorageEntry> list(String pattern, ListOptions options) {
        requireOpen();
        Storage.requireSafePattern(pattern);
        StoragePattern parsed = StoragePattern.parse(pattern);
        Path base = parsed.prefix().isEmpty() ? root : resolve(parsed.prefix());
        if (!Files.exists(base)) {
            return Stream.empty();
        }
        Predicate<String> matcher = parsed.matcher().orElse(k -> true);
        Stream<Path> paths = walk(base, parsed.walkDescendants(), pattern);
        return paths.filter(p -> !p.equals(base))
                .map(path -> toEntry(path, parsed.walkDescendants()))
                .filter(Objects::nonNull)
                .filter(entry -> matcher.test(entry.key()));
    }

    private static Stream<Path> walk(Path base, boolean recursive, String pattern) {
        try {
            return recursive ? Files.walk(base) : Files.list(base);
        } catch (IOException e) {
            throw new StorageException("list failed for pattern: " + pattern, e);
        }
    }

    private StorageEntry toEntry(Path path, boolean recursive) {
        String relativeKey = root.relativize(path).toString().replace(File.separatorChar, '/');
        BasicFileAttributes attrs;
        try {
            attrs = Files.readAttributes(path, BasicFileAttributes.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (attrs.isDirectory()) {
            // Recursive walks (a glob containing **) emit File entries only; non-recursive walks
            // surface intermediate directories as Directory entries so callers can navigate.
            if (recursive) {
                return null;
            }
            return new StorageEntry.Directory(
                    relativeKey + "/", Optional.of(attrs.lastModifiedTime().toInstant()));
        }
        return new StorageEntry.File(
                relativeKey,
                attrs.size(),
                attrs.lastModifiedTime().toInstant(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Map.of());
    }

    @Override
    public RangeReader openRangeReader(String key) {
        requireOpen();
        Path p = resolve(key);
        if (!Files.exists(p)) {
            throw new NotFoundException("File not found: " + key);
        }
        try {
            return new FileRangeReader(p, idleTimeout);
        } catch (IOException e) {
            throw new StorageException("openRangeReader failed for key: " + key, e);
        }
    }

    @Override
    public ReadHandle read(String key, ReadOptions options) {
        requireOpen();
        Path p = resolve(key);
        try {
            BasicFileAttributes attrs = Files.readAttributes(p, BasicFileAttributes.class);
            InputStream raw = Files.newInputStream(p);
            long offset = options.offset();
            if (offset > 0L) {
                long skipped = raw.skip(offset);
                if (skipped < offset) {
                    raw.close();
                    throw new RangeNotSatisfiableException(p.toString());
                }
            }
            InputStream content = options.length().isPresent()
                    ? new BoundedInputStream(raw, options.length().getAsLong())
                    : raw;
            StorageEntry.File metadata = new StorageEntry.File(
                    key,
                    attrs.size(),
                    attrs.lastModifiedTime().toInstant(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Map.of());
            return new ReadHandle(content, metadata);
        } catch (NoSuchFileException e) {
            throw new NotFoundException(p.toString(), e);
        } catch (IOException e) {
            throw new StorageException("read failed for key: " + key, e);
        }
    }

    private static final class BoundedInputStream extends FilterInputStream {
        private long remaining;

        BoundedInputStream(InputStream in, long limit) {
            super(in);
            this.remaining = limit;
        }

        @Override
        public int read() throws IOException {
            if (remaining <= 0) return -1;
            int b = super.read();
            if (b >= 0) remaining--;
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (remaining <= 0) return -1;
            int n = super.read(b, off, (int) Math.min(len, remaining));
            if (n > 0) remaining -= n;
            return n;
        }

        @Override
        public long skip(long n) throws IOException {
            long actual = super.skip(Math.min(n, remaining));
            remaining -= actual;
            return actual;
        }
    }

    @Override
    public StorageEntry.File put(String key, byte[] data, WriteOptions options) {
        requireOpen();
        Path target = resolve(key);
        if (options.ifNotExists() && Files.exists(target)) {
            throw new PreconditionFailedException("Key already exists: " + key);
        }
        try {
            Files.createDirectories(target.getParent());
            Path tmp = createTempFile(target);
            try {
                Files.write(tmp, data, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
                Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                Files.deleteIfExists(tmp);
                throw e;
            }
        } catch (IOException e) {
            throw new StorageException("put failed for key: " + key, e);
        }
        return stat(key).orElseThrow(() -> new StorageException("Wrote key but stat failed: " + key));
    }

    @Override
    public StorageEntry.File put(String key, Path source, WriteOptions options) {
        requireOpen();
        Path target = resolve(key);
        if (options.ifNotExists() && Files.exists(target)) {
            throw new PreconditionFailedException("Key already exists: " + key);
        }
        try {
            Files.createDirectories(target.getParent());
            Path tmp = createTempFile(target);
            try {
                Files.copy(source, tmp, StandardCopyOption.REPLACE_EXISTING);
                Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                Files.deleteIfExists(tmp);
                throw e;
            }
        } catch (IOException e) {
            throw new StorageException("put failed for key: " + key, e);
        }
        return stat(key).orElseThrow(() -> new StorageException("Wrote key but stat failed: " + key));
    }

    @Override
    public OutputStream openOutputStream(String key, WriteOptions options) {
        requireOpen();
        Path target = resolve(key);
        if (options.ifNotExists() && Files.exists(target)) {
            throw new PreconditionFailedException("Key already exists: " + key);
        }
        Path tmp;
        OutputStream raw;
        try {
            Files.createDirectories(target.getParent());
            tmp = createTempFile(target);
            raw = Files.newOutputStream(tmp, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new StorageException("openOutputStream failed for key: " + key, e);
        }
        final Path tmpFinal = tmp;
        return new FilterOutputStream(raw) {
            private boolean alreadyClosed;

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                out.write(b, off, len);
            }

            @Override
            public void close() throws IOException {
                if (alreadyClosed) return;
                alreadyClosed = true;
                try {
                    super.close();
                    Files.move(tmpFinal, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                } catch (IOException e) {
                    Files.deleteIfExists(tmpFinal);
                    throw e;
                }
            }
        };
    }

    @Override
    public void delete(String key) {
        requireOpen();
        try {
            Files.deleteIfExists(resolve(key));
        } catch (IOException e) {
            throw new StorageException("delete failed for key: " + key, e);
        }
    }

    @Override
    public DeleteResult deleteAll(Collection<String> keys) {
        requireOpen();
        Set<String> deleted = new HashSet<>();
        Set<String> didNotExist = new HashSet<>();
        Map<String, StorageException> failed = new HashMap<>();
        for (String key : keys) {
            Path p = resolve(key);
            try {
                if (Files.deleteIfExists(p)) {
                    deleted.add(key);
                } else {
                    didNotExist.add(key);
                }
            } catch (IOException e) {
                failed.put(key, new StorageException("delete failed for: " + key, e));
            }
        }
        return new DeleteResult(deleted, didNotExist, failed);
    }

    @Override
    public StorageEntry.File copy(String srcKey, String dstKey, CopyOptions options) {
        requireOpen();
        Path src = resolve(srcKey);
        Path dst = resolve(dstKey);
        if (!Files.exists(src)) {
            throw new NotFoundException("Source not found: " + srcKey);
        }
        if (options.ifNotExistsAtDestination() && Files.exists(dst)) {
            throw new PreconditionFailedException("Destination already exists: " + dstKey);
        }
        try {
            Files.createDirectories(dst.getParent());
            Files.copy(src, dst, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new StorageException("copy failed for src=" + srcKey + " dst=" + dstKey, e);
        }
        return stat(dstKey).orElseThrow(() -> new StorageException("Copy failed for: " + dstKey));
    }

    @Override
    public StorageEntry.File copy(String srcKey, Storage dst, String dstKey, CopyOptions options) {
        requireOpen();
        if (dst instanceof FileStorage other) {
            return copyToOtherFileStorage(srcKey, other, dstKey, options);
        }
        throw new UnsupportedCapabilityException(
                "cross-backend copy from FileStorage to " + dst.getClass().getSimpleName());
    }

    private StorageEntry.File copyToOtherFileStorage(
            String srcKey, FileStorage dst, String dstKey, CopyOptions options) {
        Path src = resolve(srcKey);
        Path target = dst.resolve(dstKey);
        if (!Files.exists(src)) {
            throw new NotFoundException("Source not found: " + srcKey);
        }
        if (options.ifNotExistsAtDestination() && Files.exists(target)) {
            throw new PreconditionFailedException("Destination already exists: " + dstKey);
        }
        try {
            Files.createDirectories(target.getParent());
            Files.copy(src, target, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new StorageException("copy failed for src=" + srcKey + " dst=" + dstKey, e);
        }
        return dst.stat(dstKey).orElseThrow(() -> new StorageException("Copy failed for: " + dstKey));
    }

    @Override
    public StorageEntry.File move(String srcKey, String dstKey, CopyOptions options) {
        requireOpen();
        Path src = resolve(srcKey);
        Path dst = resolve(dstKey);
        if (!Files.exists(src)) {
            throw new NotFoundException("Source not found: " + srcKey);
        }
        if (options.ifNotExistsAtDestination() && Files.exists(dst)) {
            throw new PreconditionFailedException("Destination already exists: " + dstKey);
        }
        try {
            Files.createDirectories(dst.getParent());
            try {
                Files.move(src, dst, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                // Cross-volume rename: fall back to copy + delete.
                Files.copy(src, dst, StandardCopyOption.REPLACE_EXISTING);
                Files.delete(src);
            }
        } catch (IOException e) {
            throw new StorageException("move failed for src=" + srcKey + " dst=" + dstKey, e);
        }
        return stat(dstKey).orElseThrow(() -> new StorageException("Move failed for: " + dstKey));
    }

    @Override
    public URI presignGet(String key, Duration ttl) {
        throw new UnsupportedCapabilityException("presignGet");
    }

    @Override
    public URI presignPut(String key, Duration ttl, PresignWriteOptions options) {
        throw new UnsupportedCapabilityException("presignPut");
    }

    private Path createTempFile(Path target) throws IOException {
        return Files.createTempFile(target.getParent(), ".tmp-", ".part");
    }
}
