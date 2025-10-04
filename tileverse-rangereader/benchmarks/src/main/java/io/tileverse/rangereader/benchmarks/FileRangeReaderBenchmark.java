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
import io.tileverse.rangereader.file.FileRangeReader;
import java.io.IOException;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.RunnerException;

/**
 * JMH benchmark for FileRangeReader with various configurations.
 * <p>
 * This benchmark measures the performance of FileRangeReader with different
 * combinations of caching and block alignment.
 */
@State(Scope.Benchmark)
public class FileRangeReaderBenchmark extends AbstractRangeReaderBenchmark {

    @Override
    protected RangeReader createBaseReader() throws IOException {
        return FileRangeReader.of(testFilePath);
    }

    @Override
    protected String getSourceIndentifier() {
        return testFilePath.toString();
    }
    /**
     * Main method to run this benchmark.
     */
    public static void main(String[] args) throws RunnerException {
        runBenchmark(FileRangeReaderBenchmark.class);
    }
}
