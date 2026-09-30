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

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Optional;

/**
 * The content of a range read answered in part, as allowed by RFC 9110 section 15.3.7: when the body of an answer ends
 * before the end of the range, the stream reads on from a GET of the rest. It ends at the end of the range, when the
 * object ends before the rest, or after an answer standing for the whole rest. The bytes of every answer come from the
 * version of the object named by the first answer: {@link RestOfRange#open} fails a GET of the rest answered from
 * another.
 */
final class RangeCompletingInputStream extends InputStream {

    /** Sends the GET of the rest of the range. */
    @FunctionalInterface
    interface RestOfRange {
        /**
         * Returns the answer to a GET of the range from {@code offset} on, or empty when the object ends before
         * {@code offset}. An answer from another version of the object than the first answer fails with an IOException.
         */
        Optional<RestAnswer> open(long offset) throws IOException;
    }

    /**
     * The answer to a GET of the rest of the range. A body not standing for the whole rest holds at least one byte or
     * fails its read: each GET of the rest moves the range forward.
     *
     * @param body the body of the answer
     * @param wholeRest whether the body stands for the whole rest: when it ends early, the object ends before the end
     *     of the range
     */
    record RestAnswer(InputStream body, boolean wholeRest) {}

    private final long end;
    private final RestOfRange restOfRange;
    private final byte[] singleByte = new byte[1];
    private InputStream answer;
    private boolean lastAnswer;
    private long position;
    private boolean ended;
    private boolean closed;

    /**
     * @param firstAnswer the body of the first answer, starting at {@code offset}
     * @param offset the first byte of the range
     * @param end the offset past the last byte to read: the end of the range, or of the object when it ends first
     * @param restOfRange sends the GET of the rest of the range
     */
    RangeCompletingInputStream(InputStream firstAnswer, long offset, long end, RestOfRange restOfRange) {
        this.answer = Objects.requireNonNull(firstAnswer);
        this.position = offset;
        this.end = end;
        this.restOfRange = Objects.requireNonNull(restOfRange);
    }

    @Override
    public int read() throws IOException {
        int read = read(singleByte, 0, 1);
        if (read == 1) {
            return Byte.toUnsignedInt(singleByte[0]);
        }
        return -1;
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
        Objects.checkFromIndexSize(offset, length, buffer.length);
        if (closed) {
            throw new IOException("Stream closed");
        }
        if (length == 0) {
            return 0;
        }
        while (!ended) {
            int read = answer.read(buffer, offset, length);
            if (read != -1) {
                position += read;
                return read;
            }
            openNextAnswer();
        }
        return -1;
    }

    @Override
    public void close() throws IOException {
        closed = true;
        ended = true;
        answer.close();
    }

    /** Closes the answer read through its end and opens the answer to a GET of the rest, if the range goes on. */
    private void openNextAnswer() throws IOException {
        answer.close();
        if (lastAnswer || position >= end) {
            ended = true;
            return;
        }
        Optional<RestAnswer> rest = restOfRange.open(position);
        if (rest.isEmpty()) {
            ended = true;
            return;
        }
        RestAnswer next = rest.get();
        answer = next.body();
        lastAnswer = next.wholeRest();
    }
}
