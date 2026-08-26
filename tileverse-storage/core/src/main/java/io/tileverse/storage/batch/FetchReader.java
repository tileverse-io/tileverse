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
package io.tileverse.storage.batch;

import io.tileverse.io.ByteRange;
import java.nio.ByteBuffer;

/**
 * Reads one planned fetch into a buffer with the exact {@link io.tileverse.storage.RangeReader#readRange(long, int,
 * ByteBuffer)} semantics: 0 at or past EOF, a short count when the range straddles EOF, the target position advanced by
 * the count. Normally a {@code readRange} method reference.
 */
@FunctionalInterface
public interface FetchReader {

    /**
     * Reads {@code range} into {@code target} at its current position.
     *
     * @param range the byte range to fetch
     * @param target the buffer the bytes land in
     * @return the number of bytes read
     */
    int read(ByteRange range, ByteBuffer target);
}
