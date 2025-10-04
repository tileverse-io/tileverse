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
package io.tileverse.rangereader.azure;

import static java.util.Objects.requireNonNull;

import com.azure.core.credential.TokenCredential;
import com.azure.core.http.rest.Response;
import com.azure.core.util.logging.ClientLogger;
import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobClientBuilder;
import com.azure.storage.blob.BlobUrlParts;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.DownloadRetryOptions;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.common.implementation.connectionstring.StorageConnectionString;
import io.tileverse.rangereader.AbstractRangeReader;
import io.tileverse.rangereader.RangeReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.OptionalLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A RangeReader implementation that reads from an Azure Blob Storage container.
 * <p>
 * This class enables reading data stored in Azure Blob Storage using the
 * Azure Storage Blob client library for Java.
 */
public class AzureBlobRangeReader extends AbstractRangeReader implements RangeReader {
    private static final Logger LOGGER = Logger.getLogger(AzureBlobRangeReader.class.getName());

    private final BlobClient blobClient;
    private long contentLength = -1;

    /**
     * Creates a new AzureBlobRangeReader for the specified blob.
     *
     * @param blobClient The Azure Blob client to read from
     * @throws IOException If an I/O error occurs
     */
    AzureBlobRangeReader(BlobClient blobClient) throws IOException {
        this.blobClient = requireNonNull(blobClient, "BlobClient cannot be null");

        // Check if the blob exists and get its content length
        try {
            if (!blobClient.exists()) {
                throw new IOException("Blob does not exist: " + blobClient.getBlobUrl());
            }

            this.contentLength = blobClient.getProperties().getBlobSize();
        } catch (BlobStorageException e) {
            BlobErrorCode errorCode = e.getErrorCode();
            int statusCode = e.getStatusCode();
            throw new IOException(
                    "%s (%d): failure to access %s".formatted(errorCode, statusCode, blobClient.getBlobUrl()), e);
        } catch (RuntimeException e) {
            throw new IOException("failure to access %s: %s".formatted(blobClient.getBlobUrl(), e.getMessage()), e);
        }
    }

    @Override
    protected int readRangeNoFlip(long offset, int actualLength, ByteBuffer target) throws IOException {

        try {
            final long start = System.nanoTime();
            // Download the specified range
            BlobRange range = new BlobRange(offset, (long) actualLength);
            DownloadRetryOptions options = new DownloadRetryOptions().setMaxRetryRequests(3);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(actualLength);

            // API requires Duration and Context parameters
            Response<Void> response = blobClient.downloadStreamWithResponse(
                    outputStream,
                    range,
                    options,
                    new BlobRequestConditions(),
                    false,
                    java.time.Duration.ofSeconds(60), // Timeout
                    com.azure.core.util.Context.NONE); // Context

            if (LOGGER.isLoggable(Level.FINE)) {
                final long end = System.nanoTime();
                final long millis = Duration.ofNanos(end - start).toMillis();
                LOGGER.fine("range:[%,d +%,d], time: %,dms]".formatted(offset, actualLength, millis));
            }

            // Verify the response is successful
            if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
                throw new IOException("Failed to download blob range, status code: " + response.getStatusCode());
            }

            // Copy the bytes directly into the target buffer
            byte[] data = outputStream.toByteArray();
            target.put(data);
            // Return the number of bytes read
            return data.length;
        } catch (Exception e) {
            throw new IOException("Failed to read range from blob: " + e.getMessage(), e);
        }
    }

    @Override
    public OptionalLong size() throws IOException {
        if (contentLength < 0) {
            try {
                contentLength = blobClient.getProperties().getBlobSize();
            } catch (Exception e) {
                throw new IOException("Failed to get blob size: " + e.getMessage(), e);
            }
        }
        return OptionalLong.of(contentLength);
    }

    @Override
    public String getSourceIdentifier() {
        return blobClient.getBlobUrl();
    }

    @Override
    public void close() {
        // Azure BlobClient doesn't require explicit closing
    }

    /**
     * Creates a new AzureBlobRangeReader for a pre-configured BlobClient.
     *
     * <p>This is the simplest way to create an AzureBlobRangeReader when you already
     * have a configured BlobClient instance with authentication, connection settings, etc.
     *
     * @param blobClient the pre-configured Azure BlobClient
     * @return a new AzureBlobRangeReader instance
     * @throws IOException if the blob doesn't exist or cannot be accessed
     * @throws NullPointerException if blobClient is null
     */
    public static AzureBlobRangeReader of(BlobClient blobClient) throws IOException {
        return new AzureBlobRangeReader(blobClient);
    }

    /**
     * Creates a new builder for AzureBlobRangeReader configuration-based construction.
     *
     * <p>Use the builder when you need to construct the BlobClient from connection parameters.
     * The builder supports several authentication patterns:
     *
     * <h4>Connection String Authentication</h4>
     * <pre>{@code
     * AzureBlobRangeReader reader = AzureBlobRangeReader.builder()
     *     .connectionString("DefaultEndpointsProtocol=https;AccountName=...")
     *     .containerName("mycontainer")
     *     .blobName("path/to/file.pmtiles")
     *     .build();
     * }</pre>
     *
     * <h4>Account Name + Key Authentication</h4>
     * <pre>{@code
     * AzureBlobRangeReader reader = AzureBlobRangeReader.builder()
     *     .accountName("mystorageaccount")
     *     .accountKey("base64-encoded-key")
     *     .containerName("mycontainer")
     *     .blobName("path/to/file.pmtiles")
     *     .build();
     * }</pre>
     *
     * <h4>SAS Token Authentication</h4>
     * <pre>{@code
     * AzureBlobRangeReader reader = AzureBlobRangeReader.builder()
     *     .accountName("mystorageaccount")
     *     .sasToken("?sv=2020-08-04&ss=bfqt...")
     *     .containerName("mycontainer")
     *     .blobName("path/to/file.pmtiles")
     *     .build();
     * }</pre>
     *
     * <h4>Azure AD Token Credential Authentication</h4>
     * <pre>{@code
     * TokenCredential credential = new DefaultAzureCredentialBuilder().build();
     * AzureBlobRangeReader reader = AzureBlobRangeReader.builder()
     *     .accountName("mystorageaccount")
     *     .tokenCredential(credential)
     *     .containerName("mycontainer")
     *     .blobName("path/to/file.pmtiles")
     *     .build();
     * }</pre>
     *
     * @return a new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for AzureBlobRangeReader.
     */
    public static class Builder {
        private TokenCredential tokenCredential;
        private String accountName;
        private String accountKey;
        private String connectionString;
        private String containerName;
        private String blobName;
        private String sasToken;
        /**
         * If provided, superseeds {@link #accountName}, {@link #containerName}, and {@link #blobName(String)} if provided in the URI
         * @see BlobClientBuilder#endpoint(String)
         */
        private URI endpoint;

        private Builder() {}

        /**
         * Sets the Azure token credential.
         *
         * @param tokenCredential the token credential
         * @return this builder
         */
        public Builder tokenCredential(TokenCredential tokenCredential) {
            this.tokenCredential = requireNonNull(tokenCredential, "Token credential cannot be null");
            return this;
        }

        /**
         * Sets the Azure Storage account name.
         *
         * @param accountName the account name
         * @return this builder
         */
        public Builder accountName(String accountName) {
            this.accountName = requireNonNull(accountName, "Account name cannot be null");
            return this;
        }

        /**
         * Sets the Azure Storage account name.
         *
         * @param accountKey the account name
         * @return this builder
         */
        public Builder accountKey(String accountKey) {
            this.accountKey = requireNonNull(accountKey, "Account key cannot be null");
            return this;
        }

        /**
         * Sets the Azure Storage account name and key.
         *
         * @param accountName the account name
         * @param accountKey the account key
         * @return this builder
         */
        public Builder accountCredentials(String accountName, String accountKey) {
            accountName(accountName);
            accountKey(accountKey);
            return this;
        }

        /**
         * Sets the Azure Storage connection string.
         *
         * @param connectionString the connection string
         * @return this builder
         */
        public Builder connectionString(String connectionString) {
            this.connectionString = requireNonNull(connectionString, "Connection string cannot be null");
            return this;
        }

        /**
         * Sets the container name.
         *
         * @param containerName the container name
         * @return this builder
         */
        public Builder containerName(String containerName) {
            this.containerName = requireNonNull(containerName, "Container name cannot be null");
            return this;
        }

        /**
         * Sets the blob path.
         *
         * @param blobName the blob path
         * @return this builder
         */
        public Builder blobName(String blobName) {
            this.blobName = requireNonNull(blobName, "Blob path cannot be null");
            return this;
        }

        /**
         * Sets the SAS token.
         *
         * @param sasToken the SAS token
         * @return this builder
         */
        public Builder sasToken(String sasToken) {
            this.sasToken = requireNonNull(sasToken, "SAS token cannot be null");
            return this;
        }

        /**
         * Sets the blob information from an Azure URI.
         *
         * @param uri the Azure URI (https://account.blob.core.windows.net/container/blob or https://...)
         * @return this builder
         * @see BlobClientBuilder#endpoint(String)
         */
        public Builder endpoint(URI uri) {
            requireNonNull(uri, "URI cannot be null");
            String scheme = uri.getScheme().toLowerCase();
            if (!("https".equals(scheme) || "http".equals(scheme))) {
                throw new IllegalArgumentException("URI must have azure, https, or blob scheme: " + uri);
            }
            this.endpoint = uri;
            return this;
        }

        /**
         * Builds the AzureBlobRangeReader.
         *
         * @return a new AzureBlobRangeReader instance
         * @throws IOException if an error occurs during construction
         */
        public AzureBlobRangeReader build() throws IOException {

            BlobClientBuilder blobClientBuilder = new BlobClientBuilder();

            String accountName = this.accountName;
            String accountKey = this.accountKey;
            String endpointUrl;

            if (endpoint != null) {
                endpointUrl = endpoint.toString();
                // sets accountName, endpoint, containerName, and blobName from the URI
                // also extracts the sasToken if present in the query string
                blobClientBuilder.endpoint(endpointUrl);
                if (accountName == null) {
                    // but does not provide accessors, parse accountName
                    BlobUrlParts parts = BlobUrlParts.parse(endpointUrl);
                    accountName = parts.getAccountName();
                }
            } else if (connectionString != null) {
                if (containerName == null || blobName == null) {
                    throw new IllegalStateException("Container name and blob path are required with connection string");
                }
                blobClientBuilder
                        .connectionString(connectionString)
                        .containerName(containerName)
                        .blobName(blobName);
                StorageConnectionString storageConnectionString =
                        StorageConnectionString.create(connectionString, new ClientLogger(AzureBlobRangeReader.class));
                accountName = storageConnectionString.getAccountName();
                endpointUrl = storageConnectionString.getBlobEndpoint().getPrimaryUri();
            } else {
                throw new IllegalStateException(
                        "Either provide the endpoint URI or connectionString, containerName, and blobName");
            }

            // authentication
            if (accountName != null && accountKey != null) {

                StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);
                blobClientBuilder.credential(credential);

            } else if (accountName != null && sasToken != null) {

                String sasTokenWithQuestion = sasToken.startsWith("?") ? sasToken : "?" + sasToken;
                blobClientBuilder.sasToken(sasTokenWithQuestion);

            } else if (tokenCredential != null) {

                blobClientBuilder.credential(tokenCredential);
            } else if (endpointUrl.startsWith("https://%s.blob.core.windows.net".formatted(accountName))) {
                // Use default Azure credential. REVISIT: should this be made a flag and AzureBlobRangeReaderProvider
                // parameter instead?
                DefaultAzureCredential defaultAzureCredential = new DefaultAzureCredentialBuilder().build();
                blobClientBuilder.credential(defaultAzureCredential);
            }

            BlobClient client = blobClientBuilder.buildClient();
            return new AzureBlobRangeReader(client);
        }
    }
}
