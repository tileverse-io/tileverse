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
package io.tileverse.storage.file;

import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageConfig;
import io.tileverse.storage.StorageParameter;
import io.tileverse.storage.spi.AbstractStorageProvider;
import io.tileverse.storage.spi.StorageProvider;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;

/**
 * {@link StorageProvider} implementation for the local filesystem. Selects on {@code file:} URIs (or filesystem paths
 * convertible to one) and produces a {@link FileStorage} rooted at an existing directory. The opened Storage supports
 * the full read/write surface (stat, list, openRangeReader, read, put, delete, copy/move) backed by NIO; presigned URLs
 * are not applicable.
 *
 * <p>The URI must point to an existing directory. URIs that resolve to a regular file are rejected (use the parent
 * directory and address the file via {@code openRangeReader(key)} or {@code openRangeReader(URI)}). URIs that resolve
 * to a non-existent path are rejected (the provider does not auto-create the directory).
 */
public class FileStorageProvider extends AbstractStorageProvider {

    /**
     * Key used as environment variable name to disable this range reader provider
     *
     * <pre>
     * {@code export IO_TILEVERSE_STORAGE_FILE=false}
     * </pre>
     */
    public static final String ENABLED_KEY = "IO_TILEVERSE_STORAGE_FILE";
    /** This range reader implementation's {@link #getId() unique identifier} */
    public static final String ID = "file";

    /**
     * Idle timeout after which a {@code FileRangeReader} closes its underlying {@code FileChannel} to release file
     * descriptors. The channel is reopened on demand. ISO-8601 duration format (e.g. {@code PT60S} for 60 seconds).
     * Default {@code PT60S}.
     *
     * <p><b>Key:</b> {@code storage.file.idle-timeout}
     */
    public static final StorageParameter<Duration> FILE_IDLE_TIMEOUT = StorageParameter.builder()
            .key("storage.file.idle-timeout")
            .title("File channel idle timeout")
            .description("""
                    Idle timeout after which a FileRangeReader closes its underlying FileChannel to release file descriptors. \
                    The channel is reopened on demand on the next read. ISO-8601 duration format (e.g. PT60S for 60 seconds, PT0S to disable).
                    """)
            .type(Duration.class)
            .group(ID)
            .defaultValue(Duration.ofSeconds(60))
            .build();

    static final List<StorageParameter<?>> PARAMS = List.of(FILE_IDLE_TIMEOUT);

    /**
     * Create a new FileStorageProvider without registering caching parameters (filesystem reads don't benefit from the
     * standard cache decorators).
     */
    public FileStorageProvider() {
        super(false); // don't add caching parameters
    }

    @Override
    protected List<StorageParameter<?>> buildParameters() {
        return PARAMS;
    }

    @Override
    public String getId() {
        return ID;
    }

    @Override
    public boolean isAvailable() {
        return StorageProvider.isEnabled(ENABLED_KEY);
    }

    @Override
    public String getDescription() {
        return "Local file system provider.";
    }

    @Override
    public boolean canProcess(StorageConfig config) {
        return matches(config, "file", null);
    }

    @Override
    public Storage createStorage(StorageConfig config) {
        URI uri = config.baseUri();
        if (uri == null) {
            throw new IllegalArgumentException("StorageConfig.baseUri() is required for FileStorage");
        }
        Path root = Paths.get(uri);
        if (!Files.exists(root)) {
            throw new IllegalArgumentException(
                    "Storage URI must point to an existing directory: " + root + " (provider does not auto-create)");
        }
        if (!Files.isDirectory(root)) {
            throw new IllegalArgumentException("Storage URI must be a directory, got file: " + root
                    + " (use the parent directory and address the file via openRangeReader(key))");
        }
        Duration idleTimeout = config.getParameter(FILE_IDLE_TIMEOUT)
                .orElseGet(() -> FILE_IDLE_TIMEOUT.defaultValue().orElseThrow());
        return new FileStorage(root, idleTimeout);
    }
}
