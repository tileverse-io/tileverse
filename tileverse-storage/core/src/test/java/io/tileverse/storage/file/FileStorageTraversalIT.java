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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;
import io.tileverse.storage.tck.AbstractStorageTraversalTck;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * TCK-driven traversal-rejection tests for {@link FileStorage}, plus filesystem-specific defenses that the universal
 * lexical guard cannot see: the {@code Path.startsWith(root)} bounds check after resolve handles Windows backslash
 * separators (which {@code key.split("/")} doesn't see) and lexically-resolved escape attempts.
 */
class FileStorageTraversalIT extends AbstractStorageTraversalTck {

    @TempDir
    static Path tempRoot;

    private static Storage storage;

    @Override
    protected Storage storage() {
        return StorageFactory.open(tempRoot.toUri());
    }

    /**
     * Windows-style backslash separators: on Windows, {@code Path.resolve} splits on {@code "\\"} and {@code ".."}
     * segments traverse - the {@code startsWith(root)} bounds check catches it. On POSIX, {@code "\\"} is a literal
     * character in a filename and the path stays inside root, so the bounds check correctly does NOT reject.
     */
    @Test
    void windowsStyleBackslashEscapeIsRejectedOnWindows() {
        assumeTrue(isWindows(), "Windows-specific traversal vector");
        assertThatThrownBy(() -> storage.openRangeReader("..\\etc\\passwd"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static boolean isWindows() {
        return System.getProperty("os.name", "").toLowerCase().contains("windows");
    }
}
