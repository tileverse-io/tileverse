/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.file;

import io.tileverse.storage.Storage;
import io.tileverse.storage.tck.StorageTCK;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.io.TempDir;

class FileStorageIT extends StorageTCK {

    @TempDir
    Path tmp;

    @Override
    protected Storage openStorage() throws IOException {
        Path root = Files.createDirectory(tmp.resolve("storage-" + System.nanoTime()));
        return new FileStorage(root);
    }
}
