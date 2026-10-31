/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
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
import io.tileverse.storage.StorageOutputStream;
import io.tileverse.storage.StoragePattern;
import io.tileverse.storage.UnsupportedCapabilityException;
import io.tileverse.storage.WriteOptions;
import java.io.File;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Local filesystem implementation of {@link Storage}.
 *
 * <p><b>Atomic writes</b>: {@link #put(String, byte[], WriteOptions)} and {@link #openOutputStream} write to a sibling
 * tempfile in the same directory and atomically rename to the destination on completion. The target object is not
 * visible until the rename succeeds.
 *
 * <p><b>Conditions</b>: a local file has no ETag of its own, and this Storage builds one from the attributes of the
 * file. Within one JVM, a write checks its conditions and renames its file onto the key as one step: of two writes
 * holding the ETag of one version, one fails. Other processes are not bound by this, and another process replacing the
 * file between the check and the rename goes unnoticed. Reads and copies check their conditions before the operation,
 * not atomically with it.
 *
 * <p><b>Interrupts</b>: the streaming reads and writes of this Storage are not interruptible.
 * {@link Files#newInputStream} and {@link Files#newOutputStream} mark their channel uninterruptible, and
 * {@link Thread#interrupt()} on a thread blocked in one of them has no effect on the I/O. Only the range reader's own
 * {@code FileChannel} is exposed to interrupts; {@code FileRangeReader} documents how a read on an interrupted thread
 * fails and how the other readers of the file recover.
 */
final class FileStorage implements Storage {

    /** Shared by the instances of this JVM, because two of them may write one file. Each lock serves several files. */
    private static final Lock[] REPLACE_LOCKS = newLocks(64);

    private static final boolean WINDOWS =
            System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("windows");

    /** Retries of a rename refused by Windows. The pauses grow by 20 ms each, up to 0.9 s in total. */
    private static final int RENAME_RETRIES = 9;

    private static final Duration RENAME_RETRY_PAUSE = Duration.ofMillis(20);

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

    private static Lock[] newLocks(int count) {
        Lock[] locks = new Lock[count];
        for (int i = 0; i < count; i++) {
            locks[i] = new ReentrantLock();
        }
        return locks;
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
                    Optional.of(etagOf(attrs)),
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
        if (!spelledAsOnDisk(parsed.prefix())) {
            // An object store matches a prefix exactly. A case-insensitive filesystem resolves a prefix spelled
            // differently, and listing under it would accept what every other backend rejects.
            return Stream.empty();
        }
        Predicate<String> matcher = parsed.matcher().orElse(k -> true);
        if (Files.isRegularFile(base)) {
            if (parsed.prefix().endsWith("/")) {
                // A prefix ending at a separator asks for a directory's children, and a file has none.
                return Stream.empty();
            }
            // The pattern names this one file. Object stores answer such a pattern by listing that key. Testing
            // the entry against the pattern keeps a root-named file from answering a glob.
            StorageEntry entry = toEntry(base, root, false);
            if (entry == null || !matcher.test(entry.key())) {
                return Stream.empty();
            }
            return Stream.of(entry);
        }
        Stream<Path> paths = walk(base, parsed.walkDescendants(), pattern);
        return paths.filter(p -> !p.equals(base))
                .map(path -> toEntry(path, root, parsed.walkDescendants()))
                .filter(Objects::nonNull)
                .filter(entry -> matcher.test(entry.key()));
    }

    /**
     * Whether every component of {@code prefix} is spelled the way the filesystem holds it. Each component is compared
     * against the names its parent directory reports, which is what a directory read returns and is therefore exact on
     * a case-insensitive filesystem as well. Nothing is resolved: a symbolic link matches under its own name, and a
     * component the caller spelled differently matches nothing.
     *
     * <p>Verifying the prefix is what lets every listing key be built from {@link #root} directly. The prefix is known
     * to be spelled correctly and the names below it come from the directory read. Every key therefore names a file the
     * way it exists on disk.
     */
    private boolean spelledAsOnDisk(String prefix) {
        Path parent = root;
        for (String component : prefix.split("/")) {
            if (component.isEmpty()) {
                continue;
            }
            if (!directoryHoldsName(parent, component)) {
                return false;
            }
            parent = parent.resolve(component);
        }
        return true;
    }

    /**
     * Whether {@code directory} reports an entry named exactly {@code name}. The scan stops at the first match, and it
     * costs one directory read, which the listing that follows was going to pay anyway.
     */
    private static boolean directoryHoldsName(Path directory, String name) {
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(directory)) {
            for (Path entry : entries) {
                if (entry.getFileName().toString().equals(name)) {
                    return true;
                }
            }
            return false;
        } catch (NotDirectoryException e) {
            return false;
        } catch (IOException e) {
            throw new StorageException("Could not read " + directory, e);
        }
    }

    private static Stream<Path> walk(Path base, boolean recursive, String pattern) {
        try {
            return recursive ? Files.walk(base) : Files.list(base);
        } catch (IOException e) {
            throw new StorageException("list failed for pattern: " + pattern, e);
        }
    }

    private static StorageEntry toEntry(Path path, Path root, boolean recursive) {
        String relativeKey = root.relativize(path).toString().replace(File.separatorChar, '/');
        BasicFileAttributes attrs;
        try {
            attrs = Files.readAttributes(path, BasicFileAttributes.class);
        } catch (NoSuchFileException e) {
            // A link to a missing file, or a file removed after the directory read: an object store omits such a
            // key from its listing instead of failing it.
            return null;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (attrs.isDirectory()) {
            // Recursive walks (a glob containing **) emit File entries only; non-recursive walks
            // emit intermediate directories as Directory entries so callers can navigate.
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
                Optional.of(etagOf(attrs)),
                Optional.empty(),
                Optional.empty(),
                Map.of());
    }

    /**
     * The ETag of a local file, built from its size, its last-modified time and, where the filesystem provides one,
     * its identity. A write renames a new file onto the key, and the new file has an identity of its own: the ETag
     * changes even when the clock of the filesystem has not moved since the previous write.
     */
    private static String etagOf(BasicFileAttributes attrs) {
        long lastModified = attrs.lastModifiedTime().to(TimeUnit.NANOSECONDS);
        String sizeAndTime = Long.toHexString(attrs.size()) + "-" + Long.toHexString(lastModified);
        Object identity = attrs.fileKey();
        if (identity == null) {
            return sizeAndTime;
        }
        return sizeAndTime + "-" + Integer.toHexString(identity.hashCode());
    }

    @Override
    public RangeReader openRangeReader(String key) {
        requireOpen();
        Path p = resolve(key);
        try {
            return new FileRangeReader(p, idleTimeout);
        } catch (NoSuchFileException e) {
            throw new NotFoundException("File not found: " + key, e);
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
            requireReadConditions(key, attrs, options);
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
                    Optional.of(etagOf(attrs)),
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

    private static void requireReadConditions(String key, BasicFileAttributes attrs, ReadOptions options) {
        Optional<String> expectedEtag = options.ifMatchEtag();
        if (expectedEtag.isPresent() && !expectedEtag.get().equals(etagOf(attrs))) {
            throw new PreconditionFailedException("ETag of " + key + " does not match " + expectedEtag.get());
        }
        Optional<Instant> since = options.ifModifiedSince();
        Instant lastModified = attrs.lastModifiedTime().toInstant();
        if (since.isPresent() && !lastModified.isAfter(since.get())) {
            throw new PreconditionFailedException(key + " was not modified since " + since.get());
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
            if (remaining <= 0) {
                return -1;
            }
            int b = super.read();
            if (b >= 0) {
                remaining--;
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (remaining <= 0) {
                return -1;
            }
            int n = super.read(b, off, (int) Math.min(len, remaining));
            if (n > 0) {
                remaining -= n;
            }
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
        requireWriteConditions(key, target, options);
        try {
            Files.createDirectories(target.getParent());
            Path tmp = createTempFile(target);
            try {
                Files.write(tmp, data, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
                replace(key, tmp, target, options);
            } catch (IOException | RuntimeException e) {
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
        requireWriteConditions(key, target, options);
        try {
            Files.createDirectories(target.getParent());
            Path tmp = createTempFile(target);
            try {
                Files.copy(source, tmp, StandardCopyOption.REPLACE_EXISTING);
                replace(key, tmp, target, options);
            } catch (IOException | RuntimeException e) {
                Files.deleteIfExists(tmp);
                throw e;
            }
        } catch (IOException e) {
            throw new StorageException("put failed for key: " + key, e);
        }
        return stat(key).orElseThrow(() -> new StorageException("Wrote key but stat failed: " + key));
    }

    @Override
    public StorageOutputStream openOutputStream(String key, WriteOptions options) {
        requireOpen();
        Path target = resolve(key);
        requireWriteConditions(key, target, options);
        try {
            Files.createDirectories(target.getParent());
            Path spool = createTempFile(target);
            return StorageOutputStream.spooling(spool, spooled -> replace(key, spooled, target, options));
        } catch (IOException e) {
            throw new StorageException("openOutputStream failed for key: " + key, e);
        }
    }

    /**
     * Checks the conditions of a write again and renames the staged file onto the target, holding the lock of the
     * target: two writes of this JVM cannot both pass a check against one version of the file. The first check, before
     * the staging, fails early; another write may have replaced the file since.
     */
    private static void replace(String key, Path staged, Path target, WriteOptions options) throws IOException {
        Lock lock = replaceLockOf(target);
        lock.lock();
        try {
            requireWriteConditions(key, target, options);
            moveReplacing(staged, target);
        } finally {
            lock.unlock();
        }
    }

    private static Lock replaceLockOf(Path target) {
        int index = Math.floorMod(target.hashCode(), REPLACE_LOCKS.length);
        return REPLACE_LOCKS[index];
    }

    /**
     * Renames the source onto the target, replacing it. Windows refuses this rename while a handle on the target is
     * open, even a short-lived one opened by another thread to read the attributes of the file. On Windows, a refused
     * rename is tried again for up to about a second.
     */
    private static void moveReplacing(Path source, Path target) throws IOException {
        int retries = 0;
        while (true) {
            try {
                Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                return;
            } catch (AccessDeniedException e) {
                retries++;
                if (!WINDOWS || retries > RENAME_RETRIES) {
                    throw e;
                }
                pauseBeforeRetry(retries, e);
            }
        }
    }

    private static void pauseBeforeRetry(int retries, AccessDeniedException refusal) throws AccessDeniedException {
        try {
            Thread.sleep(RENAME_RETRY_PAUSE.toMillis() * retries);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw refusal;
        }
    }

    private static void requireWriteConditions(String key, Path target, WriteOptions options) {
        if (options.ifNotExists() && Files.exists(target)) {
            throw new PreconditionFailedException("Key already exists: " + key);
        }
        requireEtag(key, target, options.ifMatchEtag());
    }

    /** A missing file has no ETag and matches none. */
    private static void requireEtag(String key, Path file, Optional<String> expected) {
        if (expected.isEmpty()) {
            return;
        }
        Optional<String> actual = currentEtag(key, file);
        if (!actual.equals(expected)) {
            throw new PreconditionFailedException("ETag of " + key + " does not match " + expected.get());
        }
    }

    /** Empty for a missing file or a directory, like {@link #stat(String)}. */
    private static Optional<String> currentEtag(String key, Path file) {
        try {
            BasicFileAttributes attrs = Files.readAttributes(file, BasicFileAttributes.class);
            if (attrs.isDirectory()) {
                return Optional.empty();
            }
            return Optional.of(etagOf(attrs));
        } catch (NoSuchFileException e) {
            return Optional.empty();
        } catch (IOException e) {
            throw new StorageException("stat failed for key: " + key, e);
        }
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
        requireEtag(srcKey, src, options.ifMatchSourceEtag());
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
        requireEtag(srcKey, src, options.ifMatchSourceEtag());
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
        requireEtag(srcKey, src, options.ifMatchSourceEtag());
        if (options.ifNotExistsAtDestination() && Files.exists(dst)) {
            throw new PreconditionFailedException("Destination already exists: " + dstKey);
        }
        try {
            Files.createDirectories(dst.getParent());
            try {
                moveReplacing(src, dst);
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
