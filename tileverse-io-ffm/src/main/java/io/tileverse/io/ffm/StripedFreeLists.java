/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.io.ffm;

import java.util.ArrayList;
import java.util.List;

import io.tileverse.io.ffm.DefaultSegmentPool.Backing;

/**
 * The small buffers of a pool, split over stripes. Each stripe is a {@link LockedFreeLists} with its share of the cap.
 * A thread borrows from and returns to its own stripe, chosen by its id, keeping buffers with the threads that use
 * them. It turns to another stripe only when its own is empty or full, and skips a busy one. A loaded server makes
 * about a million small borrows and returns a second, too many for one lock.
 */
final class StripedFreeLists {

    /** The most stripes a pool keeps; past this the spread gains little and the share of each stripe gets small. */
    private static final int MAX_STRIPES = 64;

    private final LockedFreeLists[] stripes;
    private final int stripeMask;

    /** Splits {@code maxRetainedBytes} evenly over {@code stripes}, a power of two. */
    StripedFreeLists(long maxRetainedBytes, int stripes) {
        if (stripes <= 0 || Integer.bitCount(stripes) != 1) {
            throw new IllegalArgumentException("stripes must be a power of two, got " + stripes);
        }
        this.stripes = new LockedFreeLists[stripes];
        long share = maxRetainedBytes / stripes;
        for (int i = 0; i < stripes; i++) {
            this.stripes[i] = new LockedFreeLists(share);
        }
        this.stripeMask = stripes - 1;
    }

    /**
     * The stripes of a pool with {@code maxPooledBytes} of small retention: {@link #defaultStripeCount} halved until
     * each stripe's share holds two buffers of the largest small size, {@code largeBufferThreshold}. A share below that
     * would refuse the common buffers, and a small cap is better served by fewer stripes than by none retaining.
     */
    static int stripeCount(int availableProcessors, long maxPooledBytes, long largeBufferThreshold) {
        int stripes = defaultStripeCount(availableProcessors);
        long minimumShare = 2 * largeBufferThreshold;
        while (stripes > 1 && maxPooledBytes / stripes < minimumShare) {
            stripes >>= 1;
        }
        return stripes;
    }

    /**
     * The stripes for a machine with {@code availableProcessors}: the next power of two, at most {@link #MAX_STRIPES}.
     */
    static int defaultStripeCount(int availableProcessors) {
        int clamped = Math.clamp(availableProcessors, 1, MAX_STRIPES);
        if (clamped == 1) {
            return 1;
        }
        return Integer.highestOneBit(clamped - 1) << 1;
    }

    /**
     * Takes an idle backing of at least {@code capacity} from the calling thread's stripe, else from the first other
     * stripe that has one and is not busy. Null when no stripe has one.
     */
    Backing take(long capacity) {
        int home = homeIndex();
        Backing taken = stripes[home].take(capacity);
        for (int i = 1; taken == null && i < stripes.length; i++) {
            taken = stripes[(home + i) & stripeMask].tryTake(capacity);
        }
        return taken;
    }

    /**
     * Retains {@code backing} in the calling thread's stripe, else in the first other stripe with room under its share
     * that is not busy. False when none has room.
     */
    boolean put(Backing backing) {
        int home = homeIndex();
        boolean retained = stripes[home].put(backing);
        for (int i = 1; !retained && i < stripes.length; i++) {
            retained = stripes[(home + i) & stripeMask].tryPut(backing);
        }
        return retained;
    }

    /** Removes and returns the backings idle for at least {@code thresholdNanos} as of {@code nowNanos}. */
    List<Backing> reapStale(long nowNanos, long thresholdNanos) {
        List<Backing> stale = new ArrayList<>();
        for (LockedFreeLists stripe : stripes) {
            stale.addAll(stripe.reapStale(nowNanos, thresholdNanos));
        }
        return stale;
    }

    /** Like {@link #reapStale}, but skips the busy stripes. */
    List<Backing> tryReapStale(long nowNanos, long thresholdNanos) {
        List<Backing> stale = new ArrayList<>();
        for (LockedFreeLists stripe : stripes) {
            stale.addAll(stripe.tryReapStale(nowNanos, thresholdNanos));
        }
        return stale;
    }

    List<Backing> drain() {
        List<Backing> drained = new ArrayList<>();
        for (LockedFreeLists stripe : stripes) {
            drained.addAll(stripe.drain());
        }
        return drained;
    }

    long retainedBytes() {
        long retainedBytes = 0;
        for (LockedFreeLists stripe : stripes) {
            retainedBytes += stripe.retainedBytes();
        }
        return retainedBytes;
    }

    int count() {
        int count = 0;
        for (LockedFreeLists stripe : stripes) {
            count += stripe.count();
        }
        return count;
    }

    /** The number of stripes. */
    int stripes() {
        return stripes.length;
    }

    /**
     * The calling thread's stripe, from its id. Thread ids are handed out in sequence, hence the threads of a pool
     * created together spread evenly over the stripes; a thread keeps its stripe for its life.
     */
    private int homeIndex() {
        return (int) (Thread.currentThread().threadId() & stripeMask);
    }
}
