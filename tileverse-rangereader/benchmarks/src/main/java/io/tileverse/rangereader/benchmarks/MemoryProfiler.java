/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.rangereader.benchmarks;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.Arrays;
import java.util.Collection;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.InternalProfiler;
import org.openjdk.jmh.results.AggregationPolicy;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.ScalarResult;

/**
 * Custom JMH profiler for measuring memory usage during benchmarks.
 * <p>
 * This profiler collects heap and non-heap memory usage stats before and after each
 * benchmark iteration, providing detailed information about memory consumption.
 */
public class MemoryProfiler implements InternalProfiler {

    /**
     * Memory MXBean for accessing memory usage information.
     */
    private final MemoryMXBean memoryMXBean;

    /**
     * Memory usage at the start of the iteration.
     */
    private MemoryUsage heapBefore;

    private MemoryUsage nonHeapBefore;

    /**
     * Maximum memory usage during the iteration.
     */
    private long maxHeapUsed;

    private long maxNonHeapUsed;

    /**
     * Constructor initializes the Memory MXBean.
     */
    public MemoryProfiler() {
        this.memoryMXBean = ManagementFactory.getMemoryMXBean();
    }

    @Override
    public String getDescription() {
        return "Collects detailed memory usage statistics during benchmark execution";
    }

    @Override
    public void beforeIteration(BenchmarkParams benchmarkParams, IterationParams iterationParams) {
        // Force garbage collection before measuring memory
        System.gc();
        System.gc();

        // Capture initial memory usage
        heapBefore = memoryMXBean.getHeapMemoryUsage();
        nonHeapBefore = memoryMXBean.getNonHeapMemoryUsage();

        // Initialize max usage trackers
        maxHeapUsed = heapBefore.getUsed();
        maxNonHeapUsed = nonHeapBefore.getUsed();
    }

    @Override
    public Collection<? extends Result<?>> afterIteration(
            BenchmarkParams benchmarkParams, IterationParams iterationParams, IterationResult iterationResult) {
        // Force garbage collection after the benchmark
        System.gc();
        System.gc();

        // Get final memory usage
        MemoryUsage heapAfter = memoryMXBean.getHeapMemoryUsage();
        MemoryUsage nonHeapAfter = memoryMXBean.getNonHeapMemoryUsage();

        // Calculate memory metrics
        long heapUsed = heapAfter.getUsed() - heapBefore.getUsed();
        long nonHeapUsed = nonHeapAfter.getUsed() - nonHeapBefore.getUsed();

        // Update max usage if necessary
        maxHeapUsed = Math.max(maxHeapUsed, heapAfter.getUsed());
        maxNonHeapUsed = Math.max(maxNonHeapUsed, nonHeapAfter.getUsed());

        // Calculate retained memory (memory that wasn't freed by GC)
        long retainedHeap = Math.max(0, heapAfter.getUsed() - heapBefore.getUsed());
        long retainedNonHeap = Math.max(0, nonHeapAfter.getUsed() - nonHeapBefore.getUsed());

        // Return memory usage results
        return Arrays.asList(
                new ScalarResult("heap.used", heapUsed / 1024.0, "KB", AggregationPolicy.AVG),
                new ScalarResult("heap.max", maxHeapUsed / 1024.0, "KB", AggregationPolicy.MAX),
                new ScalarResult("heap.retained", retainedHeap / 1024.0, "KB", AggregationPolicy.AVG),
                new ScalarResult("nonheap.used", nonHeapUsed / 1024.0, "KB", AggregationPolicy.AVG),
                new ScalarResult("nonheap.max", maxNonHeapUsed / 1024.0, "KB", AggregationPolicy.MAX),
                new ScalarResult("nonheap.retained", retainedNonHeap / 1024.0, "KB", AggregationPolicy.AVG),
                new ScalarResult("total.used", (heapUsed + nonHeapUsed) / 1024.0, "KB", AggregationPolicy.AVG),
                new ScalarResult("total.max", (maxHeapUsed + maxNonHeapUsed) / 1024.0, "KB", AggregationPolicy.MAX),
                new ScalarResult(
                        "total.retained", (retainedHeap + retainedNonHeap) / 1024.0, "KB", AggregationPolicy.AVG));
    }
}
