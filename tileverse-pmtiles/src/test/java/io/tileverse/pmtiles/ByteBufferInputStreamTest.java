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
package io.tileverse.pmtiles;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

/**
 * Tests for ByteBufferInputStream functionality.
 */
class ByteBufferInputStreamTest {

    private static final byte[] TEST_DATA = "Hello, World! This is test data for ByteBufferInputStream.".getBytes();

    @Test
    void testBasicConstruction() {
        ByteBuffer buffer = ByteBuffer.wrap(TEST_DATA);
        try (ByteBufferInputStream stream = new ByteBufferInputStream(TEST_DATA.length, buffer)) {
            assertTrue(stream.isOpen());
            assertEquals(TEST_DATA.length, stream.available());
            assertEquals(0, stream.position());
            assertTrue(stream.hasRemaining());
        }
    }

    @Test
    void testSimplifiedConstruction() {
        ByteBuffer buffer = ByteBuffer.wrap(TEST_DATA);
        try (ByteBufferInputStream stream = new ByteBufferInputStream(buffer)) {
            assertTrue(stream.isOpen());
            assertEquals(TEST_DATA.length, stream.available());
            assertEquals(0, stream.position());
            assertTrue(stream.hasRemaining());
        }
    }

    @Test
    void testConstructorValidation() {
        ByteBuffer buffer = ByteBuffer.wrap(TEST_DATA);

        // Test negative size
        assertThrows(IllegalArgumentException.class, () -> new ByteBufferInputStream(-1, buffer));

        // Test null buffer
        assertThrows(IllegalArgumentException.class, () -> new ByteBufferInputStream(10, null));

        // Test null buffer with simplified constructor
        assertThrows(IllegalArgumentException.class, () -> new ByteBufferInputStream(null));
    }

    @Test
    void testSingleByteRead() throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(TEST_DATA);
        try (ByteBufferInputStream stream = new ByteBufferInputStream(buffer)) {
            int firstByte = stream.read();
            assertEquals(TEST_DATA[0] & 0xFF, firstByte);
            assertEquals(TEST_DATA.length - 1, stream.available());
            assertEquals(1, stream.position());
        }
    }

    @Test
    void testReadUntilEOF() throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(TEST_DATA);
        try (ByteBufferInputStream stream = new ByteBufferInputStream(buffer)) {
            int bytesRead = 0;
            int b;
            while ((b = stream.read()) != -1) {
                assertEquals(TEST_DATA[bytesRead] & 0xFF, b);
                bytesRead++;
            }
            assertEquals(TEST_DATA.length, bytesRead);
            assertEquals(-1, stream.read()); // Should still return -1
        }
    }

    @Test
    void testArrayRead() throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(TEST_DATA);
        try (ByteBufferInputStream stream = new ByteBufferInputStream(buffer)) {
            byte[] readBuffer = new byte[10];
            int bytesRead = stream.read(readBuffer, 0, 10);

            assertEquals(10, bytesRead);
            for (int i = 0; i < 10; i++) {
                assertEquals(TEST_DATA[i], readBuffer[i]);
            }
            assertEquals(TEST_DATA.length - 10, stream.available());
        }
    }

    @Test
    void testArrayReadAtEOF() throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(TEST_DATA);
        try (ByteBufferInputStream stream = new ByteBufferInputStream(buffer)) {
            // Read all data
            byte[] allData = new byte[TEST_DATA.length];
            int bytesRead = stream.read(allData);
            assertEquals(TEST_DATA.length, bytesRead);

            // Try to read more - should return -1
            byte[] extra = new byte[10];
            assertEquals(-1, stream.read(extra));
        }
    }

    @Test
    void testArrayReadValidation() throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(TEST_DATA);
        try (ByteBufferInputStream stream = new ByteBufferInputStream(buffer)) {
            byte[] readBuffer = new byte[10];

            // Test negative length
            assertThrows(IllegalArgumentException.class, () -> stream.read(readBuffer, 0, -1));

            // Test null buffer
            assertThrows(IllegalArgumentException.class, () -> stream.read(null, 0, 5));

            // Test negative offset
            assertThrows(IndexOutOfBoundsException.class, () -> stream.read(readBuffer, -1, 5));

            // Test offset beyond buffer
            assertThrows(IndexOutOfBoundsException.class, () -> stream.read(readBuffer, 15, 5));

            // Test length too large for buffer
            assertThrows(IndexOutOfBoundsException.class, () -> stream.read(readBuffer, 5, 10));
        }
    }

    @Test
    void testSkip() throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(TEST_DATA);
        try (ByteBufferInputStream stream = new ByteBufferInputStream(buffer)) {
            assertEquals(0, stream.position());

            long skipped = stream.skip(5);
            assertEquals(5, skipped);
            assertEquals(5, stream.position());

            // Read next byte to verify position
            int nextByte = stream.read();
            assertEquals(TEST_DATA[5] & 0xFF, nextByte);
        }
    }

    @Test
    void testSkipValidation() throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(TEST_DATA);
        try (ByteBufferInputStream stream = new ByteBufferInputStream(buffer)) {

            // Test negative skip
            assertThrows(EOFException.class, () -> stream.skip(-1));

            // Test skip beyond buffer
            assertThrows(EOFException.class, () -> stream.skip(TEST_DATA.length + 1));
        }
    }

    @Test
    void testMarkAndReset() throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(TEST_DATA);
        try (ByteBufferInputStream stream = new ByteBufferInputStream(buffer)) {
            assertTrue(stream.markSupported());

            // Read some data
            stream.read();
            stream.read();
            assertEquals(2, stream.position());

            // Mark current position
            stream.mark(100);
            int markedPosition = stream.position();

            // Read more data
            stream.read();
            stream.read();
            assertEquals(4, stream.position());

            // Reset to mark
            stream.reset();
            assertEquals(markedPosition, stream.position());

            // Verify we can read the same data again
            int nextByte = stream.read();
            assertEquals(TEST_DATA[2] & 0xFF, nextByte);
        }
    }

    @Test
    void testResetWithoutMark() throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(TEST_DATA);
        try (ByteBufferInputStream stream = new ByteBufferInputStream(buffer)) {
            // Try to reset without setting a mark - should throw IOException
            assertThrows(IOException.class, stream::reset);
        }
    }

    @Test
    void testClose() throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(TEST_DATA);
        ByteBufferInputStream stream = new ByteBufferInputStream(buffer);

        assertTrue(stream.isOpen());
        stream.close();
        assertFalse(stream.isOpen());

        // Operations after close should throw exceptions
        assertThrows(IllegalStateException.class, stream::read);
        assertThrows(IOException.class, () -> stream.read(new byte[10]));
        assertThrows(IOException.class, () -> stream.skip(5));
        assertThrows(IllegalStateException.class, stream::available);
        assertThrows(IllegalStateException.class, stream::position);
        assertThrows(IllegalStateException.class, stream::hasRemaining);
        assertThrows(IllegalStateException.class, () -> stream.mark(10));
        assertThrows(IllegalStateException.class, stream::reset);
    }

    @Test
    void testPartialBufferRead() throws IOException {
        // Create a buffer with more capacity than data
        ByteBuffer buffer = ByteBuffer.allocate(100);
        buffer.put(TEST_DATA);
        buffer.flip(); // Prepare for reading

        try (ByteBufferInputStream stream = new ByteBufferInputStream(buffer)) {
            assertEquals(TEST_DATA.length, stream.available());

            byte[] readData = new byte[TEST_DATA.length];
            int bytesRead = stream.read(readData);

            assertEquals(TEST_DATA.length, bytesRead);
            assertArrayEquals(TEST_DATA, readData);
            assertEquals(0, stream.available());
        }
    }

    @Test
    void testToString() {
        ByteBuffer buffer = ByteBuffer.wrap(TEST_DATA);
        try (ByteBufferInputStream stream = new ByteBufferInputStream(buffer)) {
            String str = stream.toString();
            assertTrue(str.contains("ByteBufferInputStream"));
            assertTrue(str.contains("size=" + TEST_DATA.length));
            assertTrue(str.contains("available=" + TEST_DATA.length));
        }
    }

    @Test
    void testToStringAfterClose() {
        ByteBuffer buffer = ByteBuffer.wrap(TEST_DATA);
        ByteBufferInputStream stream = new ByteBufferInputStream(buffer);
        stream.close();

        String str = stream.toString();
        assertTrue(str.contains("ByteBufferInputStream"));
        assertTrue(str.contains("size=" + TEST_DATA.length));
        assertFalse(str.contains("available="));
    }

    @Test
    void testThreadSafety() throws InterruptedException {
        ByteBuffer buffer = ByteBuffer.wrap(TEST_DATA);
        ByteBufferInputStream stream = new ByteBufferInputStream(buffer);

        // Test concurrent access
        Thread[] threads = new Thread[5];
        Exception[] exceptions = new Exception[5];

        for (int i = 0; i < threads.length; i++) {
            final int threadIndex = i;
            threads[i] = new Thread(() -> {
                try {
                    // Each thread tries various operations
                    stream.available();
                    stream.position();
                    stream.hasRemaining();
                    stream.mark(10);
                } catch (Exception e) {
                    exceptions[threadIndex] = e;
                }
            });
        }

        // Start all threads
        for (Thread thread : threads) {
            thread.start();
        }

        // Wait for all threads
        for (Thread thread : threads) {
            thread.join();
        }

        // Check for exceptions
        for (Exception e : exceptions) {
            if (e != null) {
                fail("Thread safety test failed", e);
            }
        }

        stream.close();
    }
}
