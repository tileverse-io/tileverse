/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.io.ffm;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

import io.tileverse.io.ffm.DefaultSegmentPool.Backing;

/**
 * Idle backings under one lock and one byte cap. A pool keeps its large buffers in one, and each stripe of its small
 * buffers is one. The lock is taken after a short spin, because a holder is done within a microsecond and parking would
 * hand the lock over through the scheduler.
 */
final class LockedFreeLists {

    /** Tries of the lock before parking on it: about a microsecond, the time a holder needs to finish. */
    private static final int SPINS_BEFORE_PARKING = 64;

    private final ReentrantLock lock = new ReentrantLock();
    private final FreeLists lists = new FreeLists();
    private final long maxRetainedBytes;
    private long retainedBytes;
    private int count;

    LockedFreeLists(long maxRetainedBytes) {
        this.maxRetainedBytes = maxRetainedBytes;
    }

    /** Removes and returns the smallest idle backing that fits {@code capacity}, or null when none. */
    Backing take(long capacity) {
        acquireLock();
        try {
            return takeLocked(capacity);
        } finally {
            lock.unlock();
        }
    }

    /** Like {@link #take}, but gives up at once when another thread holds the lock. */
    Backing tryTake(long capacity) {
        if (!lock.tryLock()) {
            return null;
        }
        try {
            return takeLocked(capacity);
        } finally {
            lock.unlock();
        }
    }

    /** Retains {@code backing} when it fits under the cap. */
    boolean put(Backing backing) {
        acquireLock();
        try {
            return putLocked(backing);
        } finally {
            lock.unlock();
        }
    }

    /** Like {@link #put}, but gives up at once when another thread holds the lock. */
    boolean tryPut(Backing backing) {
        if (!lock.tryLock()) {
            return false;
        }
        try {
            return putLocked(backing);
        } finally {
            lock.unlock();
        }
    }

    List<Backing> reapStale(long nowNanos, long thresholdNanos) {
        lock.lock();
        try {
            return reapStaleLocked(nowNanos, thresholdNanos);
        } finally {
            lock.unlock();
        }
    }

    /** Like {@link #reapStale}, but reaps nothing when another thread holds the lock. */
    List<Backing> tryReapStale(long nowNanos, long thresholdNanos) {
        if (!lock.tryLock()) {
            return List.of();
        }
        try {
            return reapStaleLocked(nowNanos, thresholdNanos);
        } finally {
            lock.unlock();
        }
    }

    List<Backing> drain() {
        lock.lock();
        try {
            List<Backing> drained = lists.drain();
            retainedBytes = 0;
            count = 0;
            return drained;
        } finally {
            lock.unlock();
        }
    }

    long retainedBytes() {
        lock.lock();
        try {
            return retainedBytes;
        } finally {
            lock.unlock();
        }
    }

    int count() {
        lock.lock();
        try {
            return count;
        } finally {
            lock.unlock();
        }
    }

    /** The number of distinct capacities held, for the tests. */
    int groupCount() {
        lock.lock();
        try {
            return lists.groupCount();
        } finally {
            lock.unlock();
        }
    }

    private Backing takeLocked(long capacity) {
        Backing taken = lists.takeFit(capacity);
        if (taken != null) {
            retainedBytes -= taken.byteSize();
            count--;
        }
        return taken;
    }

    /** The stamp is taken under the lock: a group's stamps then never decrease from tail to head. */
    private boolean putLocked(Backing backing) {
        long size = backing.byteSize();
        if (retainedBytes + size > maxRetainedBytes) {
            return false;
        }
        lists.offer(backing, System.nanoTime());
        retainedBytes += size;
        count++;
        return true;
    }

    private List<Backing> reapStaleLocked(long nowNanos, long thresholdNanos) {
        List<Backing> stale = lists.reapStale(nowNanos, thresholdNanos);
        for (Backing backing : stale) {
            retainedBytes -= backing.byteSize();
        }
        count -= stale.size();
        return stale;
    }

    private void acquireLock() {
        for (int spin = 0; spin < SPINS_BEFORE_PARKING; spin++) {
            if (!lock.isLocked() && lock.tryLock()) {
                return;
            }
            Thread.onSpinWait();
        }
        lock.lock();
    }

    /**
     * Idle backings grouped by capacity, each stamped with the time it was returned for idle reaping. Not thread-safe;
     * the owner serializes all calls through its lock.
     *
     * <p>The groups sit in a map sorted by capacity: a borrow takes the head of the smallest capacity that fits and a
     * return pushes onto its group, a lookup in the map of distinct capacities either way, whatever the idle count.
     * Newest first keeps reuse on the recently returned backings. The oldest stay untouched at the tail of their group,
     * and the idle sweep reaps them there. An emptied group leaves the map: the large class has no upper capacity, and
     * the map would otherwise keep one group per distinct capacity ever retained.
     */
    private static final class FreeLists {

        private final TreeMap<Long, ArrayDeque<Retained>> groups = new TreeMap<>();

        Backing takeFit(long capacity) {
            Map.Entry<Long, ArrayDeque<Retained>> fit = groups.ceilingEntry(capacity);
            if (fit == null) {
                return null;
            }
            ArrayDeque<Retained> group = fit.getValue();
            Retained taken = group.pollFirst();
            if (group.isEmpty()) {
                groups.remove(fit.getKey());
            }
            return taken.backing();
        }

        /**
         * Retains {@code backing}. {@code nowNanos} must not precede the stamp of an earlier return: a group is reaped
         * from its tail on the assumption that its stamps never decrease from tail to head.
         */
        void offer(Backing backing, long nowNanos) {
            groups.computeIfAbsent(backing.byteSize(), _ -> new ArrayDeque<>())
                    .addFirst(new Retained(backing, nowNanos));
        }

        /** Removes and returns the backings idle for at least {@code thresholdNanos} as of {@code nowNanos}. */
        List<Backing> reapStale(long nowNanos, long thresholdNanos) {
            List<Backing> stale = new ArrayList<>();
            Iterator<ArrayDeque<Retained>> groupIterator = groups.values().iterator();
            while (groupIterator.hasNext()) {
                ArrayDeque<Retained> group = groupIterator.next();
                while (!group.isEmpty() && isStale(group.peekLast(), nowNanos, thresholdNanos)) {
                    stale.add(group.pollLast().backing());
                }
                if (group.isEmpty()) {
                    groupIterator.remove();
                }
            }
            return stale;
        }

        private static boolean isStale(Retained retained, long nowNanos, long thresholdNanos) {
            return nowNanos - retained.retainedAtNanos() >= thresholdNanos;
        }

        List<Backing> drain() {
            List<Backing> drained = new ArrayList<>();
            for (ArrayDeque<Retained> group : groups.values()) {
                for (Retained retained : group) {
                    drained.add(retained.backing());
                }
            }
            groups.clear();
            return drained;
        }

        int groupCount() {
            return groups.size();
        }

        /** A retained backing paired with the time it entered the free list, for idle reaping. */
        private record Retained(Backing backing, long retainedAtNanos) {}
    }
}
