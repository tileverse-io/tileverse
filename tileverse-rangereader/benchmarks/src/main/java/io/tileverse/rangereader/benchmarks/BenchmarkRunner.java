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

import io.tileverse.rangereader.benchmarks.AbstractRangeReaderBenchmark.ReaderConfig;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.openjdk.jmh.runner.options.WarmupMode;

/**
 * Main benchmark runner utility for RangeReader benchmarks.
 * <p>
 * This class provides a unified interface for running all RangeReader benchmark types
 * with various configurations. It includes options for customizing the benchmarks,
 * output formats, and profiling.
 */
public class BenchmarkRunner {

    /**
     * Available benchmark types.
     */
    public enum BenchmarkType {
        FILE(FileRangeReaderBenchmark.class), // FileRangeReader
        HTTP(HttpRangeReaderBenchmark.class), // HttpRangeReader
        S3(S3RangeReaderBenchmark.class), // S3RangeReader
        AZURE(AzureBlobRangeReaderBenchmark.class), // AzureBlobRangeReader
        ALL(
                FileRangeReaderBenchmark.class,
                HttpRangeReaderBenchmark.class,
                S3RangeReaderBenchmark.class,
                AzureBlobRangeReaderBenchmark.class);

        private List<Class<?>> benchmarkClasses;

        private BenchmarkType(Class<?>... classes) {
            this.benchmarkClasses = List.of(classes);
        }
    }

    /**
     * Main method for running the benchmarks from command line.
     */
    public static void main(String[] args) throws RunnerException, IOException {
        System.out.println("Starting RangeReader Benchmarks..."); // Added for feedback
        // Parse command line arguments
        BenchmarkConfig config = parseArgs(args);

        // Run the benchmarks
        Collection<RunResult> results = runBenchmarks(config);

        // Generate report
        if (results != null && !results.isEmpty()) {
            generateReport(results, config);
            System.out.println("Benchmarks completed successfully!");
        } else {
            System.out.println("Benchmarks did not produce results or were not run.");
        }
    }

    /**
     * Parse command line arguments into a benchmark configuration.
     */
    private static BenchmarkConfig parseArgs(String[] args) {
        BenchmarkConfig config = new BenchmarkConfig();

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            String nextArg = (i + 1 < args.length) ? args[i + 1] : null;

            try {
                switch (arg) {
                    case "--type":
                        if (nextArg != null) {
                            config.type = BenchmarkType.valueOf(args[++i].toUpperCase());
                        } else {
                            throw new IllegalArgumentException("Missing value for " + arg);
                        }
                        break;
                    case "--forks":
                        if (nextArg != null) config.forks = Integer.parseInt(args[++i]);
                        else throw new IllegalArgumentException("Missing value for " + arg);
                        break;
                    case "--warmup-iterations":
                        if (nextArg != null) config.warmupIterations = Integer.parseInt(args[++i]);
                        else throw new IllegalArgumentException("Missing value for " + arg);
                        break;
                    case "--measurement-iterations":
                        if (nextArg != null) config.measurementIterations = Integer.parseInt(args[++i]);
                        else throw new IllegalArgumentException("Missing value for " + arg);
                        break;
                    case "--warmup-time":
                        if (nextArg != null) config.warmupTime = Integer.parseInt(args[++i]);
                        else throw new IllegalArgumentException("Missing value for " + arg);
                        break;
                    case "--measurement-time":
                        if (nextArg != null) config.measurementTime = Integer.parseInt(args[++i]);
                        else throw new IllegalArgumentException("Missing value for " + arg);
                        break;
                    case "--output-format":
                        if (nextArg != null) {
                            String format = args[++i].toUpperCase();
                            config.resultFormat = ResultFormatType.valueOf(format);
                        } else {
                            throw new IllegalArgumentException("Missing value for " + arg);
                        }
                        break;
                    case "--output-file":
                        if (nextArg != null) config.outputFile = args[++i];
                        else throw new IllegalArgumentException("Missing value for " + arg);
                        break;
                    case "--profiler":
                        config.enableProfiler = true;
                        break;

                    // New parameters
                    case "--reader-config":
                        if (nextArg != null) config.readerConfig = args[++i];
                        else throw new IllegalArgumentException("Missing value for " + arg);
                        break;
                    case "--file-size":
                        if (nextArg != null) config.fileSize = Integer.parseInt(args[++i]);
                        else throw new IllegalArgumentException("Missing value for " + arg);
                        break;
                    case "--read-size":
                        if (nextArg != null) config.readSize = Integer.parseInt(args[++i]);
                        else throw new IllegalArgumentException("Missing value for " + arg);
                        break;
                    case "--access-pattern":
                        if (nextArg != null) config.accessPattern = args[++i];
                        else throw new IllegalArgumentException("Missing value for " + arg);
                        break;
                    case "--number-of-reads":
                        if (nextArg != null) config.numberOfReads = Integer.parseInt(args[++i]);
                        else throw new IllegalArgumentException("Missing value for " + arg);
                        break;
                    case "--disk-cache-size":
                        if (nextArg != null) config.diskCacheSize = Integer.parseInt(args[++i]);
                        else throw new IllegalArgumentException("Missing value for " + arg);
                        break;
                    case "--disk-block-size":
                        if (nextArg != null) config.diskBlockSize = Integer.parseInt(args[++i]);
                        else throw new IllegalArgumentException("Missing value for " + arg);
                        break;
                    case "--memory-cache-size":
                        if (nextArg != null) config.memoryCacheSize = Integer.parseInt(args[++i]);
                        else throw new IllegalArgumentException("Missing value for " + arg);
                        break;
                    case "--memory-block-size":
                        if (nextArg != null) config.memoryBlockSize = Integer.parseInt(args[++i]);
                        else throw new IllegalArgumentException("Missing value for " + arg);
                        break;

                    case "--help":
                        printUsage();
                        System.exit(0);
                        break;
                    default:
                        System.err.println("Unknown option: " + arg);
                        printUsage();
                        System.exit(1);
                }
            } catch (IllegalArgumentException
                    | NullPointerException
                    | SecurityException e) { // Added more specific exceptions
                System.err.println("Error parsing argument '" + arg + "': " + e.getMessage());
                printUsage();
                System.exit(1);
            }
        }
        return config;
    }

    /**
     * Print usage instructions for the benchmark runner.
     */
    private static void printUsage() {
        System.out.println("RangeReader Benchmark Runner");
        System.out.println("----------------------------");
        System.out.println("Usage: java -jar <your-benchmark-jar>.jar [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --type <type>                 Benchmark type (FILE, HTTP, S3, AZURE, ALL). Default: ALL");
        System.out.println("  --forks <n>                   Number of forks to use. Default: 1");
        System.out.println("  --warmup-iterations <n>       Number of warmup iterations. Default: 3");
        System.out.println("  --measurement-iterations <n>  Number of measurement iterations. Default: 5");
        System.out.println("  --warmup-time <seconds>       Warmup time per iteration in seconds. Default: 10");
        System.out.println("  --measurement-time <seconds>  Measurement time per iteration in seconds. Default: 10");
        System.out.println("  --output-format <format>      Output format (CSV, JSON, SCSV, TEXT). Default: TEXT");
        System.out.println(
                "  --output-file <file>          Output file for results. Default: benchmark-results-{timestamp}.{ext}");
        System.out.println("  --profiler                    Enable memory and GC profiling. Default: false");
        System.out.println();
        System.out.println("  Benchmark Parameters (override @Param defaults):");

        Stream<ReaderConfig> configs = Stream.of(AbstractRangeReaderBenchmark.ReaderConfig.values());
        String readerConfigs = configs.map(ReaderConfig::toString).collect(Collectors.joining(", "));

        System.out.println("  --reader-config <config>      ReaderConfig (e.g., %s)".formatted(readerConfigs));
        System.out.println("  --file-size <bytes>           Size of the test file");
        System.out.println("  --read-size <bytes>           Size of each read operation");
        System.out.println("  --access-pattern <pattern>    Access pattern (SEQUENTIAL, RANDOM, MIXED)");
        System.out.println("  --number-of-reads <n>         Number of reads to perform");
        System.out.println("  --disk-cache-size <bytes>     Disk cache max size");
        System.out.println("  --disk-block-size <bytes>     Block size for disk-aligned readers");
        System.out.println("  --memory-cache-size <bytes>   Memory cache max size");
        System.out.println("  --memory-block-size <bytes>   Block size for memory-aligned readers");
        System.out.println();
        System.out.println("  --help                        Print this help message");
    }

    /**
     * Run the benchmarks with the specified configuration.
     */
    private static Collection<RunResult> runBenchmarks(BenchmarkConfig config) throws RunnerException {
        List<Class<?>> benchmarkClasses = new ArrayList<>();
        benchmarkClasses.addAll(config.type.benchmarkClasses);

        // Build the benchmark options
        ChainedOptionsBuilder optionsBuilder = new OptionsBuilder()
                .include(buildIncludePattern(benchmarkClasses))
                .warmupMode(WarmupMode.BULK) // This is a specific choice
                .warmupIterations(config.warmupIterations)
                .warmupTime(TimeValue.seconds(config.warmupTime))
                .measurementIterations(config.measurementIterations)
                .measurementTime(TimeValue.seconds(config.measurementTime))
                .forks(config.forks)
                .shouldFailOnError(true)
                .shouldDoGC(true);

        // Add profiler if enabled
        if (config.enableProfiler) {
            optionsBuilder.addProfiler(MemoryProfiler.class);
            // optionsBuilder.addProfiler(org.openjdk.jmh.profile.GCProfiler.class);
        }

        // Set result file format if specified
        if (config.outputFile != null) {
            optionsBuilder.resultFormat(config.resultFormat);
            optionsBuilder.result(config.outputFile);
        }

        // Apply parameters from CLI if they were set
        if (config.readerConfig != null) {
            optionsBuilder.param("readerConfig", config.readerConfig);
        }
        if (config.fileSize != null) {
            optionsBuilder.param("fileSize", config.fileSize.toString());
        }
        if (config.readSize != null) {
            optionsBuilder.param("readSize", config.readSize.toString());
        }
        if (config.accessPattern != null) {
            optionsBuilder.param("accessPattern", config.accessPattern);
        }
        if (config.numberOfReads != null) {
            optionsBuilder.param("numberOfReads", config.numberOfReads.toString());
        }
        if (config.diskCacheSize != null) {
            optionsBuilder.param("diskCacheSize", config.diskCacheSize.toString());
        }
        if (config.diskBlockSize != null) {
            optionsBuilder.param("diskBlockSize", config.diskBlockSize.toString());
        }
        if (config.memoryCacheSize != null) {
            optionsBuilder.param("memoryCacheSize", config.memoryCacheSize.toString());
        }
        if (config.memoryBlockSize != null) {
            optionsBuilder.param("memoryBlockSize", config.memoryBlockSize.toString());
        }

        // Run the benchmarks
        Options options = optionsBuilder.build();
        return new Runner(options).run();
    }

    /**
     * Build the include pattern for the specified benchmark classes.
     */
    private static String buildIncludePattern(List<Class<?>> benchmarkClasses) {
        StringBuilder pattern = new StringBuilder();
        for (int i = 0; i < benchmarkClasses.size(); i++) {
            if (i > 0) {
                pattern.append("|");
            }
            // Match the simple name of the class to ensure the correct benchmarks are included.
            // Using ".*" might be too broad if you have similarly named test methods in other classes
            // that aren't benchmarks. Using the fully qualified name or a more specific regex is safer
            // if SimpleName causes issues. For now, SimpleName is common.
            pattern.append(benchmarkClasses.get(i).getSimpleName());
        }
        return pattern.length() > 0 ? pattern.toString() : ".*"; // Fallback if list is empty
    }

    /**
     * Generate a report from the benchmark results.
     * (Consider updating this to reflect which parameters were CLI overridden)
     */
    private static void generateReport(Collection<RunResult> results, BenchmarkConfig config) throws IOException {
        if (config.outputFile == null) {
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
            String extension = getFileExtension(config.resultFormat);
            config.outputFile = "benchmark-results-" + timestamp + "." + extension;
        }

        File outputFile = new File(config.outputFile);
        File parentDir = outputFile.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs();
        }

        try (PrintWriter writer = new PrintWriter(new FileWriter(outputFile))) {
            writer.println("RangeReader Benchmark Results");
            writer.println("-----------------------------");
            writer.println("Timestamp: " + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            writer.println("Configuration (CLI Overrides):"); // Indicate these are CLI values or defaults
            writer.println("  Type: " + config.type);
            writer.println("  Forks: " + config.forks);
            writer.println("  Warmup Iterations: " + config.warmupIterations);
            writer.println("  Measurement Iterations: " + config.measurementIterations);
            writer.println("  Warmup Time: " + config.warmupTime + "s");
            writer.println("  Measurement Time: " + config.measurementTime + "s");
            writer.println("  Memory Profiling: " + (config.enableProfiler ? "Enabled" : "Disabled"));

            // Print CLI-set benchmark parameters
            if (config.readerConfig != null) writer.println("  CLI readerConfig: " + config.readerConfig);
            if (config.fileSize != null) writer.println("  CLI fileSize: " + config.fileSize);
            if (config.readSize != null) writer.println("  CLI readSize: " + config.readSize);
            if (config.accessPattern != null) writer.println("  CLI accessPattern: " + config.accessPattern);
            if (config.numberOfReads != null) writer.println("  CLI numberOfReads: " + config.numberOfReads);
            if (config.diskCacheSize != null) writer.println("  CLI diskCacheSize: " + config.diskCacheSize);
            if (config.diskBlockSize != null) writer.println("  CLI diskBlockSize: " + config.diskBlockSize);
            if (config.memoryCacheSize != null) writer.println("  CLI memoryCacheSize: " + config.memoryCacheSize);
            if (config.memoryBlockSize != null) writer.println("  CLI memoryBlockSize: " + config.memoryBlockSize);
            writer.println();

            // Process and write the results (Original logic, may need adjustment based on ResultFormatType)
            if (config.resultFormat
                    == ResultFormatType.TEXT) { // Only print detailed results for TEXT format in this custom report
                for (RunResult result : results) {
                    writer.println("Benchmark: " + result.getParams().getBenchmark());
                    // Print all benchmark parameters (including those set by @Param)
                    result.getParams()
                            .getParamsKeys()
                            .forEach(key -> writer.println("  Param: " + key + " = "
                                    + result.getParams().getParam(key)));
                    writer.println(
                            "  Primary Score: " + result.getPrimaryResult().getScore() + " "
                                    + result.getPrimaryResult().getScoreUnit());
                    writer.println("  Error: " + result.getPrimaryResult().getScoreError() + " ("
                            + result.getPrimaryResult().getScoreConfidence()[0] + ", "
                            + result.getPrimaryResult().getScoreConfidence()[1] + ")");
                    writer.println();

                    if (config.enableProfiler && result.getSecondaryResults() != null) {
                        result.getSecondaryResults().forEach((label, res) -> {
                            writer.println("  " + label + ": " + res.getScore() + " " + res.getScoreUnit());
                        });
                        writer.println();
                    }
                }
            } else {
                writer.println(
                        "Results are in " + config.resultFormat + " format. See JMH output if not writing to file.");
                // If outputting to a file in JSON/CSV, JMH handles the formatting.
                // This custom text report section is redundant if resultFormat is not TEXT and outputFile is set.
            }
        }
        System.out.println(
                "Results information (potentially custom format) written to: " + outputFile.getAbsolutePath());
    }

    /**
     * Get the file extension for the specified result format.
     */
    private static String getFileExtension(ResultFormatType formatType) {
        // JMH's ResultFormat handles extensions, but for custom naming:
        if (formatType == null) return "txt";
        switch (formatType) {
            case CSV:
                return "csv";
            case SCSV:
                return "scsv"; // JMH uses .csv for SCSV too in its ResultPersister
            case JSON:
                return "json";
            case TEXT:
            default:
                return "txt";
        }
    }

    /**
     * Configuration for the benchmark run.
     */
    static class BenchmarkConfig {
        BenchmarkType type = BenchmarkType.ALL;
        int forks = 1;
        int warmupIterations = 3;
        int measurementIterations = 5;
        int warmupTime = 10; // seconds
        int measurementTime = 10; // seconds
        ResultFormatType resultFormat = ResultFormatType.TEXT;
        String outputFile = null;
        boolean enableProfiler = false;

        // New fields for JMH @Params
        String readerConfig = null;
        Integer fileSize = null;
        Integer readSize = null;
        String accessPattern = null;
        Integer numberOfReads = null;
        Integer diskCacheSize = null;
        Integer diskBlockSize = null;
        Integer memoryCacheSize = null;
        Integer memoryBlockSize = null;
    }
}
