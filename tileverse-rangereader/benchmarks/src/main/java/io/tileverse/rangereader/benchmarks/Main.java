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

import java.io.IOException;
import org.openjdk.jmh.runner.RunnerException;

/**
 * Main entry point for running RangeReader benchmarks.
 * <p>
 * This class delegates to the BenchmarkRunner. It exists as a simple
 * entry point that can be specified in the JAR manifest.
 */
public class Main {

    /**
     * Main method for running benchmarks from command line.
     *
     * @param args command line arguments to pass to the BenchmarkRunner
     * @throws RunnerException if an error occurs during benchmark execution
     * @throws IOException if an I/O error occurs
     */
    public static void main(String[] args) throws RunnerException, IOException {
        System.out.println("Starting RangeReader Benchmarks...");
        BenchmarkRunner.main(args);
    }
}
