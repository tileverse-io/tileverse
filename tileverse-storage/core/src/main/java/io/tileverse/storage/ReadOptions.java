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

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Options for {@link Storage#read(String, ReadOptions)}.
 *
 * @param offset byte offset to start reading from (0 = start of object)
 * @param length number of bytes to read; empty means read to EOF
 * @param ifMatchEtag conditional read: only return content when the object's ETag matches
 * @param versionId read a specific prior object version; backend errors if versioning is disabled. Empty means latest
 *     version
 * @param ifModifiedSince conditional read: only return content modified since this instant
 * @param timeout hard time bound for the operation; honored by backends that respect it (notably Azure sync SDK does
 *     not respect Thread.interrupt - use timeout for hard limits)
 */
public record ReadOptions(
        long offset,
        OptionalLong length,
        Optional<String> ifMatchEtag,
        Optional<String> versionId,
        Optional<Instant> ifModifiedSince,
        Optional<Duration> timeout) {

    public ReadOptions {
        if (offset < 0L) {
            throw new IllegalArgumentException("offset must be >= 0");
        }
    }

    public static ReadOptions defaults() {
        return new ReadOptions(
                0L, OptionalLong.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    public static ReadOptions fromOffset(long offset) {
        return new ReadOptions(
                offset, OptionalLong.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    public static ReadOptions range(long offset, long length) {
        if (length < 0L) {
            throw new IllegalArgumentException("length must be >= 0");
        }
        return new ReadOptions(
                offset,
                OptionalLong.of(length),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }
}
