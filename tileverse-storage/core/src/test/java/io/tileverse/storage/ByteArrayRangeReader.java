/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage;

import io.tileverse.io.ByteRange;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * In-memory {@link RangeReader} over a byte array with real EOF semantics, recording every read for call-shape
 * assertions. Thread-safe.
 */
public class ByteArrayRangeReader extends AbstractRangeReader {

    private static final AtomicInteger IDS = new AtomicInteger();

    private final byte[] data;
    private final String identifier;
    private final List<ByteRange> singleReads = Collections.synchronizedList(new ArrayList<>());
    private final List<List<ByteRange>> batchReads = Collections.synchronizedList(new ArrayList<>());

    public ByteArrayRangeReader(byte[] data) {
        this.data = data.clone();
        this.identifier = "memory://test-" + IDS.incrementAndGet();
    }

    @Override
    protected int readRangeNoFlip(long offset, int actualLength, ByteBuffer target) {
        singleReads.add(new ByteRange(offset, actualLength));
        int available = (int) Math.min(actualLength, data.length - offset);
        if (available <= 0) {
            return 0;
        }
        target.put(data, (int) offset, available);
        return available;
    }

    @Override
    public BatchReadResult readRanges(List<RangeRequest> requests) {
        RangeRequest.validate(requests);
        batchReads.add(requests.stream().map(RangeRequest::range).toList());
        int[] read = new int[requests.size()];
        for (int i = 0; i < requests.size(); i++) {
            RangeRequest request = requests.get(i);
            read[i] = readRange(request.range(), request.target());
        }
        return BatchReadResult.perRange(requests, read);
    }

    @Override
    public OptionalLong size() {
        return OptionalLong.of(data.length);
    }

    @Override
    public String getSourceIdentifier() {
        return identifier;
    }

    @Override
    public void close() {
        // nothing to release
    }

    public List<ByteRange> singleReads() {
        return List.copyOf(singleReads);
    }

    public List<List<ByteRange>> batchReads() {
        return List.copyOf(batchReads);
    }

    public void clearRecordings() {
        singleReads.clear();
        batchReads.clear();
    }
}
