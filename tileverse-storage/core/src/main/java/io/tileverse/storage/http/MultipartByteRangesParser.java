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
package io.tileverse.storage.http;

import static java.util.Objects.requireNonNull;

import io.tileverse.storage.ContentRange;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

/**
 * Streaming parser for {@code multipart/byteranges} response bodies (RFC 9110 section 14.6).
 *
 * <p>{@link #nextPart()} advances to the next part and returns its parsed {@code Content-Range}; the caller then
 * consumes the part body through {@link #readBody} and {@link #skipBody}. Boundaries are recognized only between
 * bodies, and every body length comes from its part's {@code Content-Range}: body bytes are never scanned for boundary
 * strings. Unconsumed body bytes are skipped by the next {@link #nextPart()} call.
 *
 * <p>{@link #singlePart} adapts a plain single-range body to the same interface, letting one routing loop serve both
 * response shapes.
 *
 * <p>Single-use and not thread-safe; the caller owns and closes the underlying stream.
 */
final class MultipartByteRangesParser {

    private static final int MAX_LINE_LENGTH = 8 * 1024;

    private final InputStream in;
    private final ReadableByteChannel channel;
    private final String delimiter;
    private ContentRange.Bytes preloadedPart;
    private long remainingInPart;
    private boolean finished;

    private MultipartByteRangesParser(InputStream in, String delimiter, ContentRange.Bytes preloadedPart) {
        this.in = requireNonNull(in, "in cannot be null");
        this.channel = Channels.newChannel(in);
        this.delimiter = delimiter;
        this.preloadedPart = preloadedPart;
    }

    /**
     * Creates a parser over a {@code multipart/byteranges} body.
     *
     * @param in the response body stream, positioned at its first byte
     * @param boundary the boundary token from the {@code Content-Type} header, without the leading dashes
     * @return the parser
     */
    static MultipartByteRangesParser multipart(InputStream in, String boundary) {
        return new MultipartByteRangesParser(in, "--" + requireNonNull(boundary, "boundary cannot be null"), null);
    }

    /**
     * Adapts a plain single-range body: one part described by {@code contentRange}, then end of parts.
     *
     * @param in the response body stream, positioned at the range's first byte
     * @param contentRange the response's parsed {@code Content-Range}
     * @return the parser
     */
    static MultipartByteRangesParser singlePart(InputStream in, ContentRange.Bytes contentRange) {
        return new MultipartByteRangesParser(in, null, requireNonNull(contentRange, "contentRange cannot be null"));
    }

    /**
     * Advances to the next part, skipping whatever the caller left of the current body.
     *
     * @return the next part's {@code Content-Range}, or null after the closing boundary or the end of the stream
     * @throws IOException on stream failure or a part without a usable {@code Content-Range}
     */
    ContentRange.Bytes nextPart() throws IOException {
        if (finished) {
            return null;
        }
        if (delimiter == null) {
            return nextSinglePart();
        }
        skipBody(remainingInPart);
        String line;
        while ((line = readLine()) != null) {
            if (line.equals(delimiter + "--")) {
                break;
            }
            if (line.equals(delimiter)) {
                return readPartHeaders();
            }
        }
        finished = true;
        return null;
    }

    private ContentRange.Bytes nextSinglePart() {
        ContentRange.Bytes part = preloadedPart;
        if (part == null) {
            finished = true;
            return null;
        }
        preloadedPart = null;
        remainingInPart = part.length();
        return part;
    }

    private ContentRange.Bytes readPartHeaders() throws IOException {
        ContentRange.Bytes contentRange = null;
        String line;
        while ((line = readLine()) != null && !line.isEmpty()) {
            int colon = line.indexOf(':');
            if (colon > 0 && line.substring(0, colon).trim().equalsIgnoreCase("Content-Range")) {
                contentRange =
                        ContentRange.bytesOf(line.substring(colon + 1).trim()).orElse(null);
            }
        }
        if (contentRange == null) {
            throw new IOException("multipart/byteranges part without a usable Content-Range header");
        }
        remainingInPart = contentRange.length();
        return contentRange;
    }

    /**
     * Skips body bytes of the current part, clamped to what the part still holds.
     *
     * @param bytes how many bytes to skip
     * @throws IOException on stream failure or a stream ending inside the declared body
     */
    void skipBody(long bytes) throws IOException {
        long toSkip = Math.min(bytes, remainingInPart);
        if (toSkip > 0) {
            in.skipNBytes(toSkip);
            remainingInPart -= toSkip;
        }
    }

    /**
     * Reads up to {@code length} body bytes of the current part into {@code target} at its position, clamped to what
     * the part still holds; a truncated stream yields a short count.
     *
     * @param target the buffer the bytes land in
     * @param length how many bytes to read
     * @return the number of bytes read
     * @throws IOException on stream failure
     */
    int readBody(ByteBuffer target, int length) throws IOException {
        int toRead = (int) Math.min(length, remainingInPart);
        if (toRead <= 0) {
            return 0;
        }
        int oldLimit = target.limit();
        target.limit(target.position() + toRead);
        int filled = 0;
        try {
            while (filled < toRead) {
                int read = channel.read(target);
                if (read == -1) {
                    break;
                }
                filled += read;
            }
        } finally {
            target.limit(oldLimit);
        }
        remainingInPart -= filled;
        return filled;
    }

    /** Reads one header or boundary line up to CRLF (bare LF tolerated), without the terminator; null at stream end. */
    private String readLine() throws IOException {
        StringBuilder line = new StringBuilder();
        int c;
        while ((c = in.read()) != -1) {
            if (c == '\n') {
                int last = line.length() - 1;
                if (last >= 0 && line.charAt(last) == '\r') {
                    line.setLength(last);
                }
                return line.toString();
            }
            line.append((char) c);
            if (line.length() > MAX_LINE_LENGTH) {
                throw new IOException("multipart header line exceeds " + MAX_LINE_LENGTH + " characters");
            }
        }
        return line.isEmpty() ? null : line.toString();
    }
}
