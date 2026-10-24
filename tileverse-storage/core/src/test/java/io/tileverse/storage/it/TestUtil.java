/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.it;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import org.apache.commons.io.FileUtils;

/** Utilities for creating test files and validating results. */
public class TestUtil {

    private TestUtil() {
        // Utility class, prevent instantiation
    }

    /**
     * Creates a mock test file for testing range readers.
     *
     * <p>This creates a binary file with a simple header followed by random data, which is suitable for testing range
     * reading operations.
     *
     * @param path The path to create the file at
     * @param size The size of the file in bytes
     * @return The path to the created file
     * @throws IOException If an I/O error occurs
     */
    public static Path createMockTestFile(Path path, int size) throws IOException {
        // Create a random data pattern that we can verify later
        Random random = new Random(42); // Fixed seed for reproducibility
        byte[] data = new byte[size];
        random.nextBytes(data);

        // Create a simple header (first 127 bytes)
        ByteBuffer header = ByteBuffer.allocate(127);
        // Magic bytes "TstFile" (7 bytes to match the original PMTiles length)
        header.put("TstFile".getBytes(StandardCharsets.UTF_8));
        // Version 3
        header.put((byte) 3);
        // Rest of header as random bytes
        byte[] headerRest = new byte[127 - 7 - 1];
        random.nextBytes(headerRest);
        header.put(headerRest);

        // Replace the first 127 bytes of data with our header
        System.arraycopy(header.array(), 0, data, 0, 127);

        // Write the file
        Files.write(path, data);
        return path;
    }

    /**
     * Creates a temporary test file.
     *
     * @param size The size of the file in bytes
     * @return The path to the created file
     * @throws IOException If an I/O error occurs
     */
    public static Path createTempTestFile(int size) throws IOException {
        Path tempFile = Files.createTempFile("test", ".bin");
        tempFile.toFile().deleteOnExit();
        return createMockTestFile(tempFile, size);
    }

    /**
     * Copies a file to a specified path.
     *
     * @param source The source file
     * @param destination The destination path
     * @throws IOException If an I/O error occurs
     */
    public static void copyFile(Path source, Path destination) throws IOException {
        FileUtils.copyFile(source.toFile(), destination.toFile());
    }
}
