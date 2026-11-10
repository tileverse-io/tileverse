/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.cache;

import static java.util.Objects.requireNonNull;

import com.github.benmanes.caffeine.cache.Weigher;
import java.time.Duration;
import org.jspecify.annotations.NullMarked;

/**
 * How a partition of a {@link PoolCache} weighs and expires its entries.
 *
 * <p>Registering a partition name again requires an equal spec, and weighers are compared by identity. Keep the spec in
 * a constant.
 *
 * @param <K> the type of the keys of the partition
 * @param <V> the type of the values of the partition
 * @param weigher computes the weight of an entry, in bytes of the pool's budget
 * @param expireAfterAccess how long an entry stays in the pool after its last access
 */
@NullMarked
public record PartitionSpec<K, V>(Weigher<K, V> weigher, Duration expireAfterAccess) {

    /** @throws IllegalArgumentException if {@code expireAfterAccess} is zero or negative */
    public PartitionSpec {
        requireNonNull(weigher, "weigher");
        requireNonNull(expireAfterAccess, "expireAfterAccess");
        if (expireAfterAccess.isZero() || expireAfterAccess.isNegative()) {
            throw new IllegalArgumentException("expireAfterAccess must be positive: " + expireAfterAccess);
        }
    }
}
