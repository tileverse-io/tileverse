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
package io.tileverse.storage.cache;

import io.tileverse.io.ByteRange;
import io.tileverse.storage.BatchReadResult;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import org.jspecify.annotations.NonNull;

/**
 * The outcome of one batch read by {@link BatchRangeLoader}.
 *
 * @param values the cached value of each range, read-only and holding exactly the bytes read
 * @param fetched the ranges fetched by this read, as opposed to found cached or taken from another read
 * @param cost the fetches and bytes transferred by this read
 */
record BatchLoadOutcome(Map<ByteRange, ByteBuffer> values, Set<ByteRange> fetched, BatchReadResult cost) {

    @NonNull
    ByteBuffer cachedValue(ByteRange range) {
        return values().get(range);
    }

    public boolean fetched(ByteRange range) {
        return fetched().contains(range);
    }
}
