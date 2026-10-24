/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** The stream behind {@link StorageOutputStream#spooling(Path, Commit)}. */
final class SpoolingOutputStream extends StorageOutputStream {

    private final Path spoolFile;
    private final Commit commit;
    private boolean closed;

    SpoolingOutputStream(Path spoolFile, Commit commit) throws IOException {
        super(Files.newOutputStream(spoolFile, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING));
        this.spoolFile = spoolFile;
        this.commit = commit;
    }

    /**
     * Completes the spool file, commits it, and deletes it whether or not the commit succeeded. A commit that fails
     * with a {@link StorageException} reports it as the close's I/O failure.
     */
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        try {
            out.close();
            commit.apply(spoolFile);
        } catch (StorageException e) {
            throw new IOException(e);
        } finally {
            Files.deleteIfExists(spoolFile);
        }
    }

    @Override
    public void abort() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            out.close();
        } catch (IOException ignored) {
            // best effort: the spool file is deleted next regardless
        }
        try {
            Files.deleteIfExists(spoolFile);
        } catch (IOException ignored) {
            // best effort
        }
    }
}
