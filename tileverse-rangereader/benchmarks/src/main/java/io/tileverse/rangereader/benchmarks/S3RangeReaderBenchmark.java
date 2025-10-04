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

import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

import io.tileverse.rangereader.RangeReader;
import io.tileverse.rangereader.s3.S3RangeReader;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.RunnerException;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * JMH benchmark for S3RangeReader with various configurations.
 * <p>
 * This benchmark measures the performance of S3RangeReader with different
 * combinations of caching and block alignment. It uses Testcontainers with
 * LocalStack to simulate an S3 service.
 */
@State(Scope.Benchmark)
public class S3RangeReaderBenchmark extends AbstractRangeReaderBenchmark {
    /**
     * LocalStack container for S3 simulation.
     */
    private LocalStackContainer localstack;

    /**
     * S3 client for interacting with the LocalStack container.
     */
    private S3Client s3Client;

    /**
     * Bucket name for test files.
     */
    private static final String BUCKET_NAME = "benchmark-bucket";

    /**
     * Key for the test file in S3.
     */
    private static final String TEST_FILE_KEY = "test.dat";

    /**
     * Setup method that creates a LocalStack container and configures S3.
     * This runs once per entire benchmark.
     */
    @Setup(Level.Trial)
    @Override
    public void setupTrial() throws IOException {
        // Call the parent setup to create the test file
        super.setupTrial();

        // Start LocalStack
        localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.3.0")).withServices(S3);
        localstack.start();

        // Create S3 client
        s3Client = S3Client.builder()
                .endpointOverride(localstack.getEndpointOverride(S3))
                .region(Region.of(localstack.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())))
                .build();

        // Create bucket
        s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET_NAME).build());

        // Upload test file to S3
        byte[] fileContent = Files.readAllBytes(testFilePath);
        s3Client.putObject(
                PutObjectRequest.builder()
                        .bucket(BUCKET_NAME)
                        .key(TEST_FILE_KEY)
                        .build(),
                RequestBody.fromBytes(fileContent));
    }

    /**
     * Teardown method that stops the LocalStack container.
     */
    @TearDown(Level.Trial)
    @Override
    public void teardownTrial() throws IOException {
        // Call the parent teardown to clean up the test file
        super.teardownTrial();

        // Close S3 client and stop LocalStack
        if (s3Client != null) {
            s3Client.close();
        }

        if (localstack != null) {
            localstack.stop();
        }
    }

    @Override
    protected RangeReader createBaseReader() throws IOException {
        // Create S3 URI
        URI s3Uri = URI.create(getSourceIndentifier());

        // Start with basic builder
        AwsBasicCredentials credentials =
                AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey());

        return S3RangeReader.builder()
                .uri(s3Uri)
                .endpoint(localstack.getEndpointOverride(S3))
                .region(Region.of(localstack.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                .forcePathStyle()
                .build();
    }

    @Override
    protected String getSourceIndentifier() {
        return "s3://" + BUCKET_NAME + "/" + TEST_FILE_KEY;
    }

    /**
     * Main method to run this benchmark.
     */
    public static void main(String[] args) throws RunnerException {
        runBenchmark(S3RangeReaderBenchmark.class);
    }
}
