# RangeReader Benchmarks

This module contains JMH benchmarks for measuring the performance of RangeReader implementations
with different caching and block-alignment strategies.

Currently, only the FileRangeReaderBenchmark is enabled.

The S3, Azure, and HTTP benchmarks require TestContainers and have been disabled to simplify
initial setup.