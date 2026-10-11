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

import static org.assertj.core.api.Assertions.assertThat;

import io.tileverse.cache.CacheManager;
import io.tileverse.io.ByteRange;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

/** A cache letting a test hold its next quiet lookup, to place another read inside the gap. */
final class PausingRangeReaderCache extends ByteRangeCache {

    private final AtomicReference<Pause> pause = new AtomicReference<>();

    PausingRangeReaderCache(CacheManager cacheManager, String sourceIdentifier) {
        super(cacheManager, sourceIdentifier);
    }

    /** Holds the next quiet lookup of {@code range}, regardless of the calling thread. */
    Pause pauseNextLookupOf(ByteRange range) {
        return pauseNextLookup(lookedUp -> lookedUp.equals(range));
    }

    /** Holds the next quiet lookup made by {@code thread}. */
    Pause pauseNextLookupBy(Thread thread) {
        return pauseNextLookup(lookedUp -> Thread.currentThread() == thread);
    }

    private Pause pauseNextLookup(Predicate<ByteRange> lookup) {
        Pause next = new Pause(lookup);
        pause.set(next);
        return next;
    }

    @Override
    public ByteBuffer getIfPresentQuietly(ByteRange range) {
        Pause current = pause.get();
        // taken by one read: another reaching the lookup with it passes, and a pause armed meanwhile stays armed
        if (current != null && current.lookup.test(range) && pause.compareAndSet(current, null)) {
            current.hold();
        }
        return super.getIfPresentQuietly(range);
    }

    /** One held lookup. */
    static final class Pause {
        private final Predicate<ByteRange> lookup;
        private final CountDownLatch reached = new CountDownLatch(1);
        private final CountDownLatch resumed = new CountDownLatch(1);

        private Pause(Predicate<ByteRange> lookup) {
            this.lookup = lookup;
        }

        private void hold() {
            reached.countDown();
            try {
                resumed.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }

        /** Returns once a read is held at the lookup. */
        void awaitReached() throws InterruptedException {
            assertThat(reached.await(10, TimeUnit.SECONDS)).isTrue();
        }

        void resume() {
            resumed.countDown();
        }
    }
}
