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
package io.tileverse.rangereader.gcs;

import static java.util.Objects.requireNonNull;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import io.tileverse.rangereader.AbstractRangeReader;
import io.tileverse.rangereader.RangeReader;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.OptionalLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A RangeReader implementation that reads from Google Cloud Storage.
 * <p>
 * This class enables reading data stored in Google Cloud Storage buckets using the
 * Google Cloud Storage client library for Java.
 */
public class GoogleCloudStorageRangeReader extends AbstractRangeReader implements RangeReader {

    private static final Logger LOGGER = Logger.getLogger(GoogleCloudStorageRangeReader.class.getName());

    @SuppressWarnings("unused")
    private final Storage storage;

    private final String bucket;
    private final String objectName;

    private Blob blob;

    /**
     * Creates a new GoogleCloudStorageRangeReader for the specified GCS object.
     *
     * @param storage The GCS Storage client to use
     * @param bucket The GCS bucket name
     * @param objectName The GCS object name
     * @throws IOException If an I/O error occurs
     */
    GoogleCloudStorageRangeReader(Storage storage, String bucket, String objectName) throws IOException {
        this.storage = requireNonNull(storage, "Storage client cannot be null");
        this.bucket = requireNonNull(bucket, "Bucket name cannot be null");
        this.objectName = requireNonNull(objectName, "Object name cannot be null");
        BlobId blobId = BlobId.of(bucket, objectName);
        this.blob = storage.get(blobId);

        if (blob == null || !blob.exists()) {
            throw new IOException("GCS object not found: gs://" + bucket + "/" + objectName);
        }
    }

    @Override
    protected int readRangeNoFlip(final long offset, final int actualLength, ByteBuffer target) throws IOException {
        try {
            final long start = System.nanoTime();
            // Read the specified range from GCS using readChannelWithResponse
            try (ReadChannel reader = blob.reader()) {
                reader.seek(offset);
                reader.limit(offset + actualLength);
                int totalBytesRead = 0;
                while (totalBytesRead < actualLength) {
                    int bytesRead = reader.read(target);
                    if (bytesRead == -1) {
                        // End of file reached
                        break;
                    }
                    totalBytesRead += bytesRead;
                }
                if (LOGGER.isLoggable(Level.FINE)) {
                    final long end = System.nanoTime();
                    final long millis = Duration.ofNanos(end - start).toMillis();
                    LOGGER.fine("range:[%,d +%,d], time: %,dms]".formatted(offset, actualLength, millis));
                }
                return totalBytesRead;
            }
        } catch (StorageException e) {
            throw new IOException("Failed to read range from GCS: " + e.getMessage(), e);
        }
    }

    @Override
    public OptionalLong size() throws IOException {
        if (blob == null || !blob.exists()) {
            throw new IOException("GCS object not found: gs://" + bucket + "/" + objectName);
        }
        Long size = blob.getSize();
        return size == null ? OptionalLong.empty() : OptionalLong.of(size.longValue());
    }

    @Override
    public String getSourceIdentifier() {
        return "gs://" + bucket + "/" + objectName;
    }

    @Override
    public void close() {
        // Google Cloud Storage client is typically managed externally and should be closed by the caller
    }

    /**
     * Creates a new builder for GoogleCloudStorageRangeReader.
     *
     * @return a new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for GoogleCloudStorageRangeReader.
     */
    public static class Builder {
        private Storage storage;
        private String projectId;
        private String bucket;
        private String objectName;
        private String host; // Custom endpoint for emulators or non-standard GCS endpoints

        private Credentials credentials;

        private boolean defaultCredentialsChain;

        private String quotaProjectId;

        private Builder() {}

        /**
         * Sets the Google Cloud Storage client to use.
         *
         * @param storage the Storage client
         * @return this builder
         */
        public Builder storage(Storage storage) {
            this.storage = requireNonNull(storage, "Storage cannot be null");
            return this;
        }

        /**
         * Sets the project ID. If no project ID is set, an attempt to obtain a default project ID from the environment will be made.
         *<p>
         *The default project ID will be obtained by the first available project ID among the
         * following sources:
         *
         * <ol>
         * <li>The project ID specified by the {@code GOOGLE_CLOUD_PROJECT} environment variable
         * <li>The App Engine project ID
         * <li>The project ID specified in the JSON credentials file pointed by the
         * {@code GOOGLE_APPLICATION_CREDENTIALS} environment variable
         * <li>The Google Cloud SDK project ID
         * <li>The Compute Engine project ID
         * </ol>
         *
         * @param projectId the project ID
         * @return this builder
         */
        public Builder projectId(String projectId) {
            this.projectId = requireNonNull(projectId, "Project ID cannot be null");
            return this;
        }

        /**
         * Sets the bucket name.
         * <p>
         * Optionally, use {@link #uri(URI)} to set both bucket and object name
         * @param bucket the bucket name
         * @return this builder
         */
        public Builder bucket(String bucket) {
            this.bucket = requireNonNull(bucket, "Bucket cannot be null");
            return this;
        }

        /**
         * Sets the object name.
         * <p>
         * Optionally, use {@link #uri(URI)} to set both bucket and object name
         *
         * @param objectName the object name
         * @return this builder
         */
        public Builder objectName(String objectName) {
            this.objectName = requireNonNull(objectName, "Object name cannot be null");
            return this;
        }

        /**
         * Sets the quotaProjectId that specifies the project used for quota and billing purposes.
         * <p>
         * The caller must have {@code serviceusage.services.use} permission on the project
         *
         * @param quotaProjectId quota project identifier
         * @return this builder
         * @see <a href="https://cloud.google.com/apis/docs/system-parameters">See system parameter $userProject</a>
         */
        public Builder quotaProjectId(String quotaProjectId) {
            this.quotaProjectId = requireNonNull(quotaProjectId);
            return this;
        }
        /**
         * Whether to use the default application credentials chain.
         * <p>
         * To set up Application Default Credentials for your environment, see https://cloud.google.com/docs/authentication/external/set-up-adc
         * <p>
         * Not doing so will lead to an error saying "Your default credentials were not found"
         *
         * @param defaultCredentialsChain boolean indicating whether to use the default credentials
         * @return this builder
         */
        public Builder defaultCredentialsChain(boolean defaultCredentialsChain) {
            this.defaultCredentialsChain = defaultCredentialsChain;
            return this;
        }

        /**
         * Sets the bucket and object from a GCS URI.
         * <p>
         * A GCS URL, or Google Cloud Storage Uniform Resource Locator, refers to the
         * address used to access resources stored within Google Cloud Storage. There
         * are several forms of GCS URLs, depending on the context and desired access
         * method:
         *
         * <ul>
         * <li>{@code gs://} URI: This is the canonical URI format for referencing
         * objects within Cloud Storage. It is commonly used within Google Cloud
         * services, tools, and libraries for internal referencing. For example:
         * <pre>
         * {@literal gs://your-bucket-name/your-object-name}
         * </pre>
         * <li>Public HTTP/HTTPS URLs: If an object is configured for public access, it
         * can be accessed directly via a standard HTTP or HTTPS URL. These URLs are
         * typically in the format:
         * <pre>
         * {@literal https://storage.googleapis.com/your-bucket-name/your-object-name}
         * </pre>
         * </ul>
         * This method extracts {@link #bucket(String)} and {@link #objectName(String)} from the URI
         *
         * @param uri the GCS URI
         * @return this builder
         */
        public Builder uri(URI uri) {
            requireNonNull(uri, "URI cannot be null");

            String scheme = uri.getScheme();
            if (!("gs".equalsIgnoreCase(scheme)
                    || "http".equalsIgnoreCase(scheme)
                    || "https".equalsIgnoreCase(scheme))) {
                throw new IllegalArgumentException("URI must have gs or http(s) scheme: " + uri);
            }

            if ("gs".equalsIgnoreCase(scheme)) {
                // Handle gs:// URI format: gs://bucket/object
                String bucketName = uri.getAuthority();
                if (bucketName == null || bucketName.isEmpty()) {
                    throw new IllegalArgumentException("GCS URI must have a bucket: " + uri);
                }

                String pathStr = uri.getPath();
                if (pathStr == null || pathStr.isEmpty() || pathStr.equals("/")) {
                    throw new IllegalArgumentException("GCS URI must have an object name: " + uri);
                }

                // Remove leading slash from path to get the object name
                String object = pathStr.startsWith("/") ? pathStr.substring(1) : pathStr;

                this.bucket = bucketName;
                this.objectName = object;
            } else {
                // Handle HTTP/HTTPS URLs
                String pathStr = uri.getPath();
                if (pathStr != null && pathStr.contains("/storage/v1/b/") && pathStr.contains("/o/")) {
                    // GCS REST API URL: /storage/v1/b/{bucket}/o/{object}
                    parseGcsApiUrl(uri);
                    // Extract host endpoint for custom GCS emulators
                    this.host = uri.getScheme() + "://" + uri.getAuthority();
                } else {
                    // Public GCS URL: https://storage.googleapis.com/bucket/object
                    parsePublicGcsUrl(uri);
                    // Only set custom host if it's not the standard GCS endpoint
                    if (!"storage.googleapis.com".equals(uri.getHost())) {
                        this.host = uri.getScheme() + "://" + uri.getAuthority();
                    }
                }
            }
            return this;
        }

        private void parseGcsApiUrl(URI uri) {
            String pathStr = uri.getPath();
            // Pattern: /storage/v1/b/{bucket}/o/{object}
            String bucketMarker = "/storage/v1/b/";
            String objectMarker = "/o/";

            int bucketStart = pathStr.indexOf(bucketMarker);
            if (bucketStart == -1) {
                throw new IllegalArgumentException("Invalid GCS API URL format: " + uri);
            }
            bucketStart += bucketMarker.length();

            int bucketEnd = pathStr.indexOf(objectMarker, bucketStart);
            if (bucketEnd == -1) {
                throw new IllegalArgumentException("Invalid GCS API URL format: " + uri);
            }

            int objectStart = bucketEnd + objectMarker.length();

            this.bucket = pathStr.substring(bucketStart, bucketEnd);

            // Extract object name, removing query parameters if present
            String objectPart = pathStr.substring(objectStart);
            int queryStart = objectPart.indexOf('?');
            if (queryStart != -1) {
                objectPart = objectPart.substring(0, queryStart);
            }

            // URL decode the object name
            try {
                this.objectName = java.net.URLDecoder.decode(objectPart, "UTF-8");
            } catch (java.io.UnsupportedEncodingException e) {
                this.objectName = objectPart;
            }
        }

        private void parsePublicGcsUrl(URI uri) {
            String pathStr = uri.getPath();
            if (pathStr == null || pathStr.isEmpty() || pathStr.equals("/")) {
                throw new IllegalArgumentException("GCS URI must have an object name: " + uri);
            }

            // For public GCS URLs like https://storage.googleapis.com/bucket/object
            // Extract bucket and object from the path
            String pathWithoutLeadingSlash = pathStr.startsWith("/") ? pathStr.substring(1) : pathStr;
            int firstSlash = pathWithoutLeadingSlash.indexOf('/');

            if (firstSlash == -1) {
                // Path contains only bucket name, no object
                throw new IllegalArgumentException("GCS URI must have an object name: " + uri);
            }

            String bucketName = pathWithoutLeadingSlash.substring(0, firstSlash);
            String objectName = pathWithoutLeadingSlash.substring(firstSlash + 1);

            if (bucketName.isEmpty() || objectName.isEmpty()) {
                throw new IllegalArgumentException(
                        "Invalid GCS URI format - bucket and object cannot be empty: " + uri);
            }

            this.bucket = bucketName;
            this.objectName = objectName;
        }

        /**
         * Builds the GoogleCloudStorageRangeReader.
         *
         * @return a new GoogleCloudStorageRangeReader instance
         * @throws IOException if an error occurs during construction
         */
        public GoogleCloudStorageRangeReader build() throws IOException {
            if (bucket == null || objectName == null) {
                throw new IllegalStateException("Bucket and object name must be set");
            }

            Credentials credentials = this.credentials; // GoogleCredentials.getApplicationDefault();
            if (credentials == null && this.defaultCredentialsChain) {
                credentials = GoogleCredentials.getApplicationDefault();
            }

            Storage storageClient = this.storage;
            if (storageClient == null) {
                com.google.cloud.storage.StorageOptions.Builder builder =
                        StorageOptions.getDefaultInstance().toBuilder();
                if (projectId != null) {
                    builder.setProjectId(projectId);
                }
                if (quotaProjectId != null) {
                    builder.setQuotaProjectId(quotaProjectId);
                }
                if (host != null) {
                    // Set custom endpoint for emulators or non-standard GCS endpoints
                    builder.setHost(host);
                }

                if (credentials != null) {
                    // credentials need to be set after projectId and quotaProjectId so its setter will
                    // check whether projectId is null and get it from credentials if its a ServiceAccountCredentials
                    // or quotaProjectId is null and get it from credentials if it's a QuotaProjectIdProvider
                    builder.setCredentials(credentials);
                }
                storageClient = builder.build().getService();
            }

            return new GoogleCloudStorageRangeReader(storageClient, bucket, objectName);
        }
    }
}
