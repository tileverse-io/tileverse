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
package io.tileverse.storage;

import java.util.Optional;
import java.util.OptionalLong;
import org.jspecify.annotations.Nullable;

/**
 * Parsing helper for the HTTP {@code Content-Range} response header, whose complete-length field tells the total object
 * size ("bytes 0-9/1234" or, on a 416, "bytes &#42;/1234").
 */
public final class ContentRange {

    private ContentRange() {}

    /**
     * Extracts the complete-length from a {@code Content-Range} header value.
     *
     * @param contentRangeHeader the raw header value, may be null
     * @return the total size after the slash, or empty when absent, unknown ("*"), or unparseable
     */
    public static OptionalLong totalOf(@Nullable String contentRangeHeader) {
        if (contentRangeHeader == null) {
            return OptionalLong.empty();
        }
        int slash = contentRangeHeader.lastIndexOf('/');
        if (slash < 0 || slash == contentRangeHeader.length() - 1) {
            return OptionalLong.empty();
        }
        String total = contentRangeHeader.substring(slash + 1).trim();
        if ("*".equals(total)) {
            return OptionalLong.empty();
        }
        try {
            return OptionalLong.of(Long.parseLong(total));
        } catch (NumberFormatException notANumber) {
            return OptionalLong.empty();
        }
    }

    /**
     * A parsed satisfiable {@code Content-Range} value ({@code bytes first-last/total}).
     *
     * @param firstPos absolute offset of the first byte of the response or part
     * @param lastPos absolute offset of the last byte, inclusive
     * @param total the complete object size, empty when the server sent {@code *}
     */
    public record Bytes(long firstPos, long lastPos, OptionalLong total) {

        /**
         * Returns the number of bytes the range describes.
         *
         * @return {@code lastPos - firstPos + 1}
         */
        public long length() {
            return lastPos - firstPos + 1;
        }
    }

    /**
     * Parses a satisfiable {@code Content-Range} header value ({@code "bytes 0-99/1234"} or {@code "bytes 0-99/*"}).
     *
     * @param contentRangeHeader the raw header value, may be null
     * @return the parsed positions, or empty for null, unsatisfied ({@code "bytes *&#47;1234"}), or malformed input
     */
    public static Optional<Bytes> bytesOf(@Nullable String contentRangeHeader) {
        if (contentRangeHeader == null) {
            return Optional.empty();
        }
        String value = contentRangeHeader.trim();
        if (!value.regionMatches(true, 0, "bytes", 0, 5)) {
            return Optional.empty();
        }
        String spec = value.substring(5).trim();
        int slash = spec.lastIndexOf('/');
        if (slash < 0) {
            return Optional.empty();
        }
        String rangePart = spec.substring(0, slash).trim();
        int dash = rangePart.indexOf('-');
        if (dash <= 0) {
            return Optional.empty();
        }
        try {
            long first = Long.parseLong(rangePart.substring(0, dash).trim());
            long last = Long.parseLong(rangePart.substring(dash + 1).trim());
            if (first < 0 || last < first) {
                return Optional.empty();
            }
            return Optional.of(new Bytes(first, last, totalOf(contentRangeHeader)));
        } catch (NumberFormatException notANumber) {
            return Optional.empty();
        }
    }
}
