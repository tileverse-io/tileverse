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

import io.tileverse.rangereader.RangeReader;
import io.tileverse.rangereader.block.BlockAlignedRangeReader;
import io.tileverse.rangereader.cache.CachingRangeReader;
import io.tileverse.rangereader.cache.DiskCachingRangeReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Base class for all RangeReader benchmarks.
 * <p>
 * This class provides common functionality for setting up test data files,
 * parameters for different access patterns, and benchmark methods for all
 * RangeReader implementations.
 */
@BenchmarkMode({Mode.Throughput})
@OutputTimeUnit(TimeUnit.SECONDS)
// @Warmup(iterations = 1, time = 1)
// @Measurement(iterations = 3, time = 5)
// @Threads(4)
// @Fork(0)
@State(Scope.Benchmark)
public abstract class AbstractRangeReaderBenchmark {
    /**
     * Access pattern types for benchmarking.
     */
    public enum AccessPattern {
        SEQUENTIAL, // Sequential access (reading consecutive blocks)
        RANDOM, // Random access (reading randomly distributed blocks)
        MIXED // Mix of sequential and random access
    }

    /**
     * Enumeration of different reader configurations to benchmark.
     */
    public enum ReaderConfig {
        PLAIN, // Plain range reader without decorators
        MEMORY_CACHE, // With memory caching
        DISK_CACHE, // With disk caching
        //        CACHE_THEN_BLOCK_ALIGN, // With memory cache followed by block alignment (cache is not block-aligned)
        BLOCK_ALIGN_THEN_CACHE, // With block alignment followed by memory cache (cache is block-aligned)
        //        DISK_CACHE_THEN_BLOCK_ALIGN, // With disk cache followed by block alignment (disk cache is not
        // block-aligned)
        BLOCK_ALIGN_THEN_DISK_CACHE, // With block alignment followed by disk cache (disk cache is block-aligned)
        MEMORY_AND_DISK_CACHE_ALIGNED // enable both memory and disk cache with block alignment
    }

    /**
     * Reader configuration to use.
     */
    @Param
    public ReaderConfig readerConfig;

    /**
     * Size of the test file to create.
     */
    @Param({"209715200"}) // 200MB (bigger than cache)
    public int fileSize;

    /**
     * Size of each read operation.
     */
    @Param({"16384", "262144"}) // 16KB (smaller than block size), 256KB (larger than block size)
    public int readSize;

    /**
     * Access pattern for the benchmark.
     */
    @Param({"SEQUENTIAL", "RANDOM", "MIXED"})
    public String accessPattern;

    /**
     * Number of reads to perform during the benchmark.
     */
    @Param({"1000"})
    public int numberOfReads;

    /**
     * Disk cache max size
     */
    @Param({"104857600"}) // 100MB, smaller than file size
    public int diskCacheSize;

    /**
     * Block size for block-aligned disk readers.
     */
    @Param({"65536"}) // 64K (default), 1MB (large)
    public int diskBlockSize;

    /**
     * Memory Cache max size
     */
    @Param({"16777216"}) // 16MB
    public int memoryCacheSize;

    /**
     * Block size for block-aligned memory cache readers.
     */
    @Param({"65536"}) // 64KB (default)
    public int memoryBlockSize;

    /**
     * Temporary directory for test files, created at setup and deleted at teardown.
     */
    protected Path tempDir;

    /**
     * Test data file path.
     */
    protected Path testFilePath;

    /**
     * The RangeReader implementation to benchmark.
     */
    protected RangeReader rangeReader;

    /**
     * Random number generator for random access patterns.
     */
    protected Random random;

    /**
     * Pre-generated offsets for the benchmark, created based on the access pattern.
     */
    protected long[] offsets;

    /**
     * Setup method that creates the temporary directory and test file.
     * This runs once per entire benchmark.
     */
    @Setup(Level.Trial)
    public void setupTrial() throws IOException {
        // Create temporary directory
        tempDir = Files.createTempDirectory("rangereader-benchmark");

        // Create test file
        testFilePath = tempDir.resolve("test.dat");
        createTestFile(testFilePath, fileSize);

        // Initialize random and offsets
        random = new Random(42); // Fixed seed for reproducibility
        offsets = generateOffsets();
    }

    /**
     * Teardown method that cleans up resources after the benchmark.
     */
    @TearDown(Level.Trial)
    public void teardownTrial() throws IOException {
        // Delete temporary files and directory
        if (testFilePath != null && Files.exists(testFilePath)) {
            Files.delete(testFilePath);
        }

        if (tempDir != null && Files.exists(tempDir)) {
            FileUtils.deleteDirectory(tempDir.toFile());
        }
    }

    @Setup(Level.Iteration)
    public void setupRangeReader() throws IOException {
        // Initialize the RangeReader
        rangeReader = createRangeReader();
    }

    @TearDown(Level.Iteration)
    public void tearDownRangeReader() throws IOException {
        if (rangeReader != null) {
            rangeReader.close();
        }
    }

    protected abstract RangeReader createBaseReader() throws IOException;

    protected abstract String getSourceIndentifier();

    protected RangeReader createRangeReader() throws IOException {
        RangeReader baseReader = createBaseReader();

        // Decorate the base reader according to configuration
        return switch (readerConfig) {
            case PLAIN -> baseReader;

            case MEMORY_CACHE -> caching(baseReader);

            case DISK_CACHE -> diskCaching(baseReader);

            // Memory cache followed by block alignment (cache is not block-aligned)
            //            case CACHE_THEN_BLOCK_ALIGN -> caching(blockAligned(memoryBlockSize, baseReader));

            // Block alignment followed by memory cache (cache is block-aligned)
            case BLOCK_ALIGN_THEN_CACHE -> blockAligned(memoryBlockSize, caching(baseReader));

            // Disk cache followed by block alignment (disk cache is not aligned, has overlapping ranges, but requests
            // to baseRader as block aligned, requesting more than required)
            //            case DISK_CACHE_THEN_BLOCK_ALIGN -> diskCaching(blockAligned(diskBlockSize, baseReader));

            // Block alignment followed by disk cache (disk cache is block aligned, no overlaps)
            case BLOCK_ALIGN_THEN_DISK_CACHE -> blockAligned(diskBlockSize, diskCaching(baseReader));

            // Should be the best option, both disk and memory cache with block alignment
            case MEMORY_AND_DISK_CACHE_ALIGNED ->
                blockAligned(memoryBlockSize, caching(blockAligned(diskBlockSize, diskCaching(baseReader))));

            default -> throw new IllegalStateException("Unknown reader configuration: " + readerConfig);
        };
    }

    private DiskCachingRangeReader diskCaching(RangeReader baseReader) throws IOException {
        return DiskCachingRangeReader.builder(baseReader)
                .cacheDirectory(tempDir.resolve("disk-cache"))
                .maxCacheSizeBytes(diskCacheSize)
                .build();
    }

    private RangeReader caching(RangeReader baseReader) {
        return CachingRangeReader.builder(baseReader).blockSize(memoryBlockSize).build();
    }

    private BlockAlignedRangeReader blockAligned(int blockSize, RangeReader baseReader) {
        return new BlockAlignedRangeReader(baseReader, blockSize);
    }

    /**
     * Creates a test file with random data of the specified size.
     */
    protected void createTestFile(Path path, int size) throws IOException {
        byte[] data = new byte[size];
        new Random(42).nextBytes(data); // Fixed seed for reproducibility
        Files.write(path, data);
    }

    /**
     * Generates an array of offsets based on the selected access pattern.
     */
    protected long[] generateOffsets() throws IOException {
        long[] result = new long[numberOfReads];
        long fileLength = Files.size(testFilePath);
        long maxOffset = fileLength - readSize;

        AccessPattern pattern = AccessPattern.valueOf(accessPattern);

        switch (pattern) {
            case SEQUENTIAL:
                for (int i = 0; i < numberOfReads; i++) {
                    // For sequential, we read consecutive blocks as much as possible
                    long offset = (i * readSize) % maxOffset;
                    result[i] = offset;
                }
                break;

            case RANDOM:
                for (int i = 0; i < numberOfReads; i++) {
                    // For random, we read from random positions
                    long offset = Math.abs(random.nextLong()) % maxOffset;
                    result[i] = offset;
                }
                break;

            case MIXED:
                // For mixed, we do 50% sequential and 50% random
                for (int i = 0; i < numberOfReads; i++) {
                    if (i % 2 == 0) {
                        // Sequential part
                        long offset = ((i / 2) * readSize) % maxOffset;
                        result[i] = offset;
                    } else {
                        // Random part
                        long offset = Math.abs(random.nextLong()) % maxOffset;
                        result[i] = offset;
                    }
                }
                break;
        }

        return result;
    }

    /**
     * Benchmark method for measuring read performance.
     * This performs reads according to the pre-generated offset pattern.
     */
    @Benchmark
    public Integer[] benchmarkRead() throws IOException {
        Integer[] results = new Integer[numberOfReads];

        ByteBuffer buffer = ByteBuffer.allocate(readSize);

        for (int i = 0; i < numberOfReads; i++) {
            results[i] = rangeReader.readRange(offsets[i], readSize, buffer);
            buffer.clear();
        }
        return results;
    }

    /**
     * Main method to run the benchmark from command line.
     * Subclasses can use this to run their specific benchmarks.
     */
    public static void runBenchmark(Class<? extends AbstractRangeReaderBenchmark> benchmarkClass)
            throws RunnerException {
        Options options = new OptionsBuilder()
                .include(benchmarkClass.getSimpleName())
                .addProfiler(GCProfiler.class)
                .build();

        new Runner(options).run();
    }
}
