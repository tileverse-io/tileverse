/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.cache;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CaffeineCacheTest {

    @Test
    void quietLookupRecordsNothing() {
        CaffeineCache<String, String> cache =
                CaffeineCache.<String, String>newBuilder().recordStats().build();
        cache.get("cached", key -> "value");
        CacheStats before = cache.stats();

        String found = cache.getIfPresentQuietly("cached");
        String missing = cache.getIfPresentQuietly("absent");

        assertThat(found).isEqualTo("value");
        assertThat(missing).isNull();
        CacheStats after = cache.stats();
        assertThat(after.hitCount()).isEqualTo(before.hitCount());
        assertThat(after.missCount()).isEqualTo(before.missCount());
    }

    @Test
    void quietLookupStaysInPartition() {
        CaffeineCache<String, String> shared =
                CaffeineCache.<String, String>newBuilder().recordStats().build();
        Cache<String, String> first = shared.subCache("first");
        Cache<String, String> second = shared.subCache("second");
        first.get("cached", key -> "value");
        CacheStats before = shared.stats();

        String inItsPartition = first.getIfPresentQuietly("cached");
        String inAnotherPartition = second.getIfPresentQuietly("cached");

        assertThat(inItsPartition).isEqualTo("value");
        assertThat(inAnotherPartition).isNull();
        CacheStats after = shared.stats();
        assertThat(after.hitCount()).isEqualTo(before.hitCount());
        assertThat(after.missCount()).isEqualTo(before.missCount());
    }

    /**
     * Two entries of a cache expiring one minute after access are stored together. 40 seconds later one is looked up
     * quietly, the other with a recorded lookup. 40 seconds later again, the first is gone, 80 seconds after its store.
     * The second stays, 40 seconds after its lookup.
     */
    @ParameterizedTest
    @MethodSource("rootCacheAndPartition")
    void quietLookupIsNotAnAccess(UnaryOperator<Cache<String, String>> rootOrPartition) {
        AtomicLong nanos = new AtomicLong();
        Duration fortySeconds = Duration.ofSeconds(40);
        CaffeineCache<String, String> shared = expiringAfterAccess(Duration.ofMinutes(1), nanos::get);
        Cache<String, String> cache = rootOrPartition.apply(shared);
        cache.get("looked up quietly", key -> "value");
        cache.get("looked up and recorded", key -> "value");

        nanos.addAndGet(fortySeconds.toNanos());
        String foundQuietly = cache.getIfPresentQuietly("looked up quietly");
        String foundAndRecorded = cache.getIfPresent("looked up and recorded");
        nanos.addAndGet(fortySeconds.toNanos());

        assertThat(foundQuietly).isEqualTo("value");
        assertThat(foundAndRecorded).isEqualTo("value");
        assertThat(cache.getIfPresent("looked up quietly")).isNull();
        assertThat(cache.getIfPresent("looked up and recorded")).isEqualTo("value");
    }

    static Stream<Arguments> rootCacheAndPartition() {
        UnaryOperator<Cache<String, String>> root = UnaryOperator.identity();
        UnaryOperator<Cache<String, String>> partition = shared -> shared.subCache("partition");
        return Stream.of(
                Arguments.argumentSet("the root cache", root), Arguments.argumentSet("a partition", partition));
    }

    private static CaffeineCache<String, String> expiringAfterAccess(Duration window, Ticker clock) {
        Caffeine<Object, Object> expiring =
                Caffeine.newBuilder().expireAfterAccess(window).ticker(clock);
        return new CaffeineCache<>(expiring.build());
    }
}
