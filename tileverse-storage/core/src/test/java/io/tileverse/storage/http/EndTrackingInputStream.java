/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tileverse.storage.http;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;

/**
 * An in-memory input stream that records whether it was closed and whether a reader observed its end before closing it.
 * The JDK HTTP client cancels an HTTP/2 stream whose body is closed before its final frame arrived; the end-tracking
 * flags detect such a close.
 */
final class EndTrackingInputStream extends FilterInputStream {

    private boolean endObserved;
    private boolean closedBeforeEnd;
    private boolean closed;

    EndTrackingInputStream(byte[] content) {
        super(new ByteArrayInputStream(content));
    }

    /** Whether {@link #close()} ran. */
    boolean closed() {
        return closed;
    }

    /** Whether a read returned end of stream. */
    boolean endObserved() {
        return endObserved;
    }

    /** Whether {@link #close()} ran before any read returned end of stream. */
    boolean closedBeforeEnd() {
        return closedBeforeEnd;
    }

    @Override
    public int read() throws IOException {
        int value = super.read();
        if (value == -1) {
            endObserved = true;
        }
        return value;
    }

    @Override
    public int read(byte[] into, int offset, int length) throws IOException {
        int count = super.read(into, offset, length);
        if (count == -1) {
            endObserved = true;
        }
        return count;
    }

    @Override
    public void close() throws IOException {
        if (!endObserved) {
            closedBeforeEnd = true;
        }
        closed = true;
        super.close();
    }
}
