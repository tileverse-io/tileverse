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
package io.tileverse.rangereader.it;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import io.tileverse.rangereader.RangeReader;
import io.tileverse.rangereader.azure.AzureBlobRangeReader;
import io.tileverse.rangereader.cache.CachingRangeReader;
import io.tileverse.rangereader.s3.S3RangeReader;
import java.io.IOException;
import java.net.URI;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Examples of using different authentication methods with cloud storage RangeReaders.
 * <p>
 * This class demonstrates various ways to authenticate with AWS S3 and Azure Blob Storage
 * when creating RangeReader instances.
 * <p>
 * Note: These examples are for illustration purposes only and not meant to be run directly.
 * In a real application, you would use your actual credentials and endpoints.
 */
class CloudAuthenticationExample {

    /**
     * Examples of AWS S3 authentication methods.
     */
    public static void s3AuthenticationExamples() throws IOException {
        // Example S3 URI
        URI s3Uri = URI.create("s3://example-bucket/path/to/file.pmtiles");

        // 1. Default credentials (checks env vars, AWS profile, EC2 instance profile, etc.)
        RangeReader defaultReader = S3RangeReader.builder()
                .uri(s3Uri)
                .credentialsProvider(DefaultCredentialsProvider.builder().build())
                .build();

        // 2. Specific AWS profile
        RangeReader profileReader = S3RangeReader.builder()
                .uri(s3Uri)
                .credentialsProvider(
                        ProfileCredentialsProvider.builder().profileName("dev").build())
                .build();

        // 3. Static credentials (not recommended for production, use for testing only)
        RangeReader staticReader = S3RangeReader.builder()
                .uri(s3Uri)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("access-key-id", "secret-access-key")))
                .build();

        // 4. With specific region
        RangeReader regionReader = S3RangeReader.builder()
                .uri(s3Uri)
                .credentialsProvider(DefaultCredentialsProvider.builder().build())
                .region(Region.US_WEST_2)
                .build();

        // 5. Create with explicit bucket and key
        S3Client s3Client = S3Client.builder()
                .credentialsProvider(DefaultCredentialsProvider.builder().build())
                .region(Region.US_EAST_1)
                .build();
        RangeReader explicitReader = S3RangeReader.builder()
                .s3Client(s3Client)
                .bucket("example-bucket")
                .key("path/to/file.pmtiles")
                .build();

        // Clean up resources
        defaultReader.close();
        profileReader.close();
        staticReader.close();
        regionReader.close();
        explicitReader.close();
    }

    /**
     * Examples of Azure Blob Storage authentication methods.
     */
    public static void azureAuthenticationExamples() throws IOException {
        // Example Azure URI
        URI azureUri = URI.create("https//mystorageaccount.blob.core.windows.net/container/file.pmtiles");

        // 1. Default credentials (checks env vars, managed identity, Visual Studio, Azure CLI, etc.)
        TokenCredential defaultCredential = new DefaultAzureCredentialBuilder().build();
        RangeReader defaultReader = AzureBlobRangeReader.builder()
                .endpoint(azureUri)
                .tokenCredential(defaultCredential)
                .build();

        // 2. Managed Identity (useful in Azure hosted services like App Service, Functions, VMs)
        TokenCredential managedIdentityCredential = new ManagedIdentityCredentialBuilder()
                .clientId("client-id") // Optional, if you have multiple identities
                .build();
        RangeReader managedIdentityReader = AzureBlobRangeReader.builder()
                .endpoint(azureUri)
                .tokenCredential(managedIdentityCredential)
                .build();

        // 3. Service Principal with client secret
        TokenCredential servicePrincipalCredential = new ClientSecretCredentialBuilder()
                .tenantId("tenant-id")
                .clientId("client-id")
                .clientSecret("client-secret")
                .build();
        RangeReader servicePrincipalReader = AzureBlobRangeReader.builder()
                .endpoint(azureUri)
                .tokenCredential(servicePrincipalCredential)
                .build();

        // 4. Storage account shared key (account key authentication)
        RangeReader sharedKeyReader = AzureBlobRangeReader.builder()
                .accountName("mystorageaccount")
                .accountCredentials("mystorageaccount", "accountkey==") // Replace with actual key
                .containerName("container")
                .blobName("file.pmtiles")
                .build();

        // 5. Connection string
        String connectionString =
                "DefaultEndpointsProtocol=https;AccountName=mystorageaccount;AccountKey=accountkey==;EndpointSuffix=core.windows.net";
        RangeReader connectionStringReader = AzureBlobRangeReader.builder()
                .connectionString(connectionString)
                .containerName("container")
                .blobName("file.pmtiles")
                .build();

        // 6. SAS token (Shared Access Signature)
        // SAS tokens can be included directly in the URI's query string
        URI sasUri = URI.create(
                "https://mystorageaccount.blob.core.windows.net/container/file.pmtiles?sp=r&st=2023-01-01&se=2024-01-01&spr=https&sv=2022-11-02&sig=signature");
        RangeReader sasReader = AzureBlobRangeReader.builder().endpoint(sasUri).build();

        // Clean up resources
        defaultReader.close();
        managedIdentityReader.close();
        servicePrincipalReader.close();
        sharedKeyReader.close();
        connectionStringReader.close();
        sasReader.close();
    }

    /**
     * Example of using a RangeReader with caching and block alignment for better performance.
     */
    public static void performanceOptimizationExample() throws IOException {
        // Example URI
        URI uri = URI.create("s3://example-bucket/path/to/file.pmtiles");

        // Create a basic reader
        RangeReader basicReader = S3RangeReader.builder().uri(uri).build();

        // Add caching for better performance
        RangeReader cachedReader =
                CachingRangeReader.builder(basicReader).blockSize(32 * 1024).build();

        // Clean up resources
        basicReader.close();
        cachedReader.close();
    }

    /**
     * Main method for demonstration purposes.
     * <p>
     * Note: This is not meant to be run directly as it contains placeholder credentials.
     */
    public static void main(String[] args) {
        try {
            System.out.println("These examples are for illustration only and not meant to be run directly.");
            System.out.println("In a real application, you would use your actual credentials and endpoints.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
