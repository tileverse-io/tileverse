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
package io.tileverse.storage;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.EnumSet;
import java.util.Set;

/**
 * The stream returned by {@link Storage#openOutputStream(String, WriteOptions)}. Bytes written to it become the object
 * at the key only when {@link #close()} completes; until then the store holds nothing visible at that key.
 *
 * <p>{@link #abort()} gives up on the write: it releases whatever the stream holds locally, removes any spool file, and
 * leaves the key as it was before the stream was opened. It is idempotent, does nothing once a close has committed the
 * object, and a close after an abort commits nothing.
 *
 * <p>Writes and flushes go to the underlying stream given at construction; a backend decides what close and abort do.
 */
public abstract class StorageOutputStream extends OutputStream {

    /** Turns a completed spool file into the object at the key. */
    @FunctionalInterface
    public interface Commit {
        void apply(Path spoolFile) throws IOException;
    }

    /** The stream receiving the bytes: a spool file, an upload session. */
    protected final OutputStream out;

    protected StorageOutputStream(OutputStream out) {
        this.out = out;
    }

    /**
     * A stream that accumulates the object in a local spool file and hands the completed file to {@code commit} on
     * close; abort deletes the spool file without committing. Backends whose delivery wants the whole object at once
     * build on it: a rename onto the target for a local file, a single upload for an object store.
     *
     * @param spoolFile an existing local file, truncated on open, that accumulates the object
     * @param commit the step that turns the completed spool file into the object at the key
     */
    public static StorageOutputStream spooling(Path spoolFile, Commit commit) throws IOException {
        return new SpoolingOutputStream(spoolFile, commit);
    }

    /**
     * Like {@link #spooling(Path, Commit)} with a fresh spool file in the system temporary directory, for a backend
     * that uploads the whole object from a local file. The spool file has an unpredictable name and, on a filesystem
     * with POSIX permissions, is readable and writable by its owner only.
     *
     * @param spoolFilePrefix the spool file's name prefix, naming the backend in the temporary directory
     * @throws StorageException when the spool file cannot be created
     */
    // java:S5443 asks for care with the shared temporary directory: the spool file has an unpredictable name and
    // owner-only permissions, and nothing else reads it.
    @SuppressWarnings("java:S5443")
    public static StorageOutputStream spoolingInTempDir(String spoolFilePrefix, Commit commit) {
        try {
            Path spoolFile = Files.createTempFile(spoolFilePrefix, ".part", ownerOnlyPermissions());
            return spooling(spoolFile, commit);
        } catch (IOException e) {
            throw new StorageException("Could not create the spool file for a streaming write", e);
        }
    }

    private static FileAttribute<?>[] ownerOnlyPermissions() {
        if (!FileSystems.getDefault().supportedFileAttributeViews().contains("posix")) {
            return new FileAttribute<?>[0];
        }
        Set<PosixFilePermission> ownerReadWrite =
                EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE);
        return new FileAttribute<?>[] {PosixFilePermissions.asFileAttribute(ownerReadWrite)};
    }

    @Override
    public void write(int b) throws IOException {
        out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        out.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    /** Commits the write: the object becomes visible at the key when this returns. */
    @Override
    public abstract void close() throws IOException;

    /**
     * Discards the write, leaving nothing at the key and no spool file behind. Best effort: a failure to release a
     * local resource is swallowed, since the caller is already unwinding from an error.
     */
    public abstract void abort();
}
