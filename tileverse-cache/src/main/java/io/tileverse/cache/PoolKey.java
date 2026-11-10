/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.cache;

import static java.util.Objects.requireNonNull;

import org.jspecify.annotations.NullMarked;

/**
 * The key of an entry in a {@link PoolCache}. It wraps the key of a partition's consumer with the ids of the partition
 * and of its sub-partition. Two partitions can use the same consumer keys without seeing each other's entries.
 *
 * @param partitionId the id of the partition, computed from its name
 * @param subPartitionId the id of the sub-partition, or {@link #NO_SUB_PARTITION} for an entry of the partition itself
 * @param key the key of the consumer
 */
@NullMarked
record PoolKey(long partitionId, long subPartitionId, Object key) {

    /** The sub-partition id of an entry stored in the partition itself. */
    static final long NO_SUB_PARTITION = 0L;

    PoolKey {
        requireNonNull(key, "key");
    }
}
