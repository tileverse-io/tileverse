/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.cache;

import io.tileverse.io.ByteRange;
import io.tileverse.storage.AbstractRangeReader;
import io.tileverse.storage.BatchReadResult;
import io.tileverse.storage.ByteArrayRangeReader;
import io.tileverse.storage.RangeRequest;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;

/**
 * A reader over a byte array letting a test hold and fail its calls by their order of arrival, the first call being
 * number 1. A call both held and failed throws once released.
 */
final class ScriptedRangeReader extends AbstractRangeReader {

    private final ByteArrayRangeReader bytes;
    private final List<List<ByteRange>> calls = new ArrayList<>();
    private final Map<Integer, CountDownLatch> holds = new ConcurrentHashMap<>();
    private final Map<Integer, Throwable> failures = new ConcurrentHashMap<>();

    ScriptedRangeReader(byte[] data) {
        this.bytes = new ByteArrayRangeReader(data);
    }

    void hold(int callNumber) {
        holds.put(callNumber, new CountDownLatch(1));
    }

    void release(int callNumber) {
        holds.get(callNumber).countDown();
    }

    /** Makes the call of that number throw {@code failure}, an unchecked exception or an error. */
    void fail(int callNumber, Throwable failure) {
        requireUnchecked(failure);
        failures.put(callNumber, failure);
    }

    private static void requireUnchecked(Throwable failure) {
        boolean unchecked = failure instanceof RuntimeException || failure instanceof Error;
        if (!unchecked) {
            throw new IllegalArgumentException("A read throws no checked exception: " + failure);
        }
    }

    /** Returns once the call of that number has arrived. */
    void awaitCall(int callNumber) {
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> calls().size() >= callNumber);
    }

    /** The ranges of each call, in their order of arrival. */
    List<List<ByteRange>> calls() {
        synchronized (calls) {
            return List.copyOf(calls);
        }
    }

    @Override
    protected int readRangeNoFlip(long offset, int actualLength, ByteBuffer target) {
        ByteRange range = new ByteRange(offset, actualLength);
        arrive(List.of(range));
        return bytes.readRange(offset, actualLength, target);
    }

    @Override
    public BatchReadResult readRanges(List<RangeRequest> requests) {
        List<ByteRange> ranges = requests.stream().map(RangeRequest::range).toList();
        arrive(ranges);
        return bytes.readRanges(requests);
    }

    private void arrive(List<ByteRange> ranges) {
        int callNumber = recordCall(ranges);
        awaitRelease(holds.get(callNumber));
        throwIfScripted(failures.get(callNumber));
    }

    private int recordCall(List<ByteRange> ranges) {
        synchronized (calls) {
            calls.add(ranges);
            return calls.size();
        }
    }

    private static void awaitRelease(CountDownLatch hold) {
        if (hold == null) {
            return;
        }
        try {
            hold.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static void throwIfScripted(Throwable failure) {
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure instanceof RuntimeException unchecked) {
            throw unchecked;
        }
    }

    @Override
    public OptionalLong size() {
        return bytes.size();
    }

    @Override
    public String getSourceIdentifier() {
        return bytes.getSourceIdentifier();
    }

    @Override
    public void close() {
        // nothing to release
    }
}
