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

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import io.tileverse.rangereader.RangeReader;
import io.tileverse.rangereader.azure.AzureBlobRangeReader;
import java.io.IOException;
import java.nio.file.Files;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.RunnerException;
import org.testcontainers.azure.AzuriteContainer;

/**
 * JMH benchmark for AzureBlobRangeReader with various configurations.
 * <p>
 * This benchmark measures the performance of AzureBlobRangeReader with
 * different combinations of caching and block alignment. It uses
 * AzuriteContainer with Testcontainers to simulate Azure Blob Storage.
 */
@State(Scope.Benchmark)
public class AzureBlobRangeReaderBenchmark extends AbstractRangeReaderBenchmark {

    /**
     * Azurite container for Azure Blob Storage simulation.
     */
    private AzuriteContainer azurite;

    /**
     * Azure Blob Service client for interacting with the Azurite container.
     */
    private BlobServiceClient blobServiceClient;

    /**
     * Container name for test files.
     */
    private static final String CONTAINER_NAME = "benchmark-container";

    /**
     * Blob name for the test file.
     */
    private static final String TEST_BLOB_NAME = "test.dat";

    /**
     * Setup method that creates an Azurite container and configures Azure Blob
     * Storage. This runs once per entire benchmark.
     */
    @Setup(Level.Trial)
    @Override
    public void setupTrial() throws IOException {
        // Call the parent setup to create the test file
        super.setupTrial();

        // Start Azurite container
        azurite = new AzuriteContainer("mcr.microsoft.com/azure-storage/azurite")
                .withEnv("AZURITE_BLOB_LOOSE", "true")
                .withCommand("azurite-blob --skipApiVersionCheck --loose --blobHost 0.0.0.0");
        azurite.start();

        // Create Azure Blob Service client
        String connectionString = azurite.getConnectionString();
        blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(connectionString)
                .buildClient();

        // Create container
        BlobContainerClient containerClient = blobServiceClient.createBlobContainerIfNotExists(CONTAINER_NAME);

        // Upload test file to Azure Blob Storage
        byte[] fileContent = Files.readAllBytes(testFilePath);
        BlobClient blobClient = containerClient.getBlobClient(TEST_BLOB_NAME);
        blobClient.upload(BinaryData.fromBytes(fileContent), true);
    }

    /**
     * Teardown method that stops the Azurite container.
     */
    @TearDown(Level.Trial)
    @Override
    public void teardownTrial() throws IOException {
        // Call the parent teardown to clean up the test file
        super.teardownTrial();

        // Close Azure clients and stop Azurite container
        if (azurite != null) {
            azurite.stop();
        }
    }

    @Override
    protected String getSourceIndentifier() {
        return "azure-" + CONTAINER_NAME + "-" + TEST_BLOB_NAME;
    }

    @Override
    protected RangeReader createBaseReader() throws IOException {
        // Get connection string from Azurite container
        String connectionString = azurite.getConnectionString();
        return AzureBlobRangeReader.builder()
                .connectionString(connectionString)
                .containerName(CONTAINER_NAME)
                .blobName(TEST_BLOB_NAME)
                .build();
    }

    /**
     * Main method to run this benchmark.
     */
    public static void main(String[] args) throws RunnerException {
        runBenchmark(AzureBlobRangeReaderBenchmark.class);
    }
}
