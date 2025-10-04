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
package io.tileverse.rangereader.s3;

import static java.util.Optional.ofNullable;

import io.tileverse.rangereader.AbstractRangeReader;
import io.tileverse.rangereader.RangeReader;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

/**
 * A {@link RangeReader} implementation that reads data from an AWS S3-compatible
 * object storage service.
 * <p>
 * This class enables efficient reading of data from S3 objects by leveraging
 * the {@link software.amazon.awssdk.services.s3.S3Client} from the AWS SDK for Java v2.
 * It is designed to handle both standard AWS S3 and self-hosted S3-compatible
 * services like MinIO.
 * <h2>Authentication and Configuration</h2>
 * The {@link Builder} for this class provides a flexible and robust mechanism for
 * resolving credentials and other client settings. The builder determines which
 * credentials provider to use based on a defined precedence:
 * <ol>
 * <li><b>Explicit Credentials:</b> If an explicit {@link AwsCredentialsProvider}
 * is provided, or if an access key and secret key are set directly, these are used.</li>
 *
 * <li><b>Default Credential Chain:</b> If {@code useDefaultCredentialsProvider} is enabled,
 * the client first attempts to resolve credentials from the AWS default credential chain,
 * which checks environment variables, system properties, and shared credentials files.
 * If a {@code defaultCredentialsProfile} is also specified, the chain is configured
 * to prioritize that profile.</li>
 *
 * <li><b>Forced Profile:</b> If a {@code defaultCredentialsProfile} is set but
 * {@code useDefaultCredentialsProvider} is disabled, the client bypasses the
 * full default chain and uses only the {@link ProfileCredentialsProvider} for
 * the specified profile.</li>
 *
 * <li><b>Anonymous Access:</b> If no credentials are explicitly configured, the
 * client uses {@link AnonymousCredentialsProvider} to make unsigned requests.</li>
 * </ol>
 *
 * <h2>Profile-Based Configuration</h2>
 * When a named profile (e.g., 'minio') is used, the builder also attempts to resolve the
 * AWS region from the corresponding section in the {@code ~/.aws/config} file. This
 * allows for a cleaner separation of credentials and configuration. For S3-compatible
 * services like MinIO, the region is a required parameter for the SDK's signing process,
 * even though the service itself may not use it.
 *
 * <h2>S3-Compatible Endpoints</h2>
 * This builder supports custom S3-compatible endpoints via the {@code endpointOverride}
 * method. For most self-hosted services (e.g., MinIO), it is critical to enable
 * <b>path-style access</b> by setting {@code forcePathStyle(true)} to ensure the
 * request is correctly addressed to the bucket.
 */
public class S3RangeReader extends AbstractRangeReader implements RangeReader {

    private final S3Client s3Client;
    private final S3Reference s3Location;

    private final OptionalLong contentLength;

    /**
     * Creates a new S3RangeReader for the specified S3 object.
     *
     * @param s3Client The S3 client to use
     * @param bucket The S3 bucket name
     * @param key The S3 object key
     * @throws IOException If an I/O error occurs
     */
    S3RangeReader(S3Client s3Client, S3Reference s3Location) throws IOException {
        this.s3Client = Objects.requireNonNull(s3Client, "S3Client cannot be null");
        this.s3Location = Objects.requireNonNull(s3Location, "S3Location cannot be null");

        // Check if the object exists and get its content length
        try {
            HeadObjectRequest headRequest = HeadObjectRequest.builder()
                    .bucket(s3Location.bucket())
                    .key(s3Location.key())
                    .build();

            HeadObjectResponse headResponse = s3Client.headObject(headRequest);
            Long size = headResponse.contentLength();
            this.contentLength = size == null ? OptionalLong.empty() : OptionalLong.of(size);
        } catch (NoSuchKeyException e) {
            throw new IOException("S3 object does not exist: s3://" + s3Location, e);
        } catch (SdkException e) {
            throw new IOException("Failed to access S3 object " + s3Location + ": " + e.getMessage(), e);
        }
    }

    S3RangeReader(S3Client client, String bucketName, String keyName) throws IOException {
        this(client, new S3Reference(null, bucketName, keyName, null));
    }

    @Override
    protected int readRangeNoFlip(final long offset, final int actualLength, ByteBuffer target) throws IOException {
        long rangeEnd = offset + actualLength - 1;

        try {
            // Request the specified range from S3
            GetObjectRequest rangeRequest = GetObjectRequest.builder()
                    .bucket(s3Location.bucket())
                    .key(s3Location.key())
                    .range("bytes=" + offset + "-" + rangeEnd)
                    .build();

            ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(rangeRequest);

            // Validate that we got the expected content length
            if (objectBytes.response().contentLength() != actualLength) {
                throw new IOException("Unexpected content length: got "
                        + objectBytes.response().contentLength()
                        + ", expected "
                        + actualLength);
            }

            // Put the bytes directly into the target buffer
            byte[] data = objectBytes.asByteArray();
            target.put(data);

            // Return the number of bytes read
            return data.length;
        } catch (SdkException e) {
            throw new IOException("Failed to read range from S3: " + e.getMessage(), e);
        }
    }

    @Override
    public OptionalLong size() throws IOException {
        return contentLength;
    }

    @Override
    public String getSourceIdentifier() {
        return s3Location.toString();
    }

    @Override
    public void close() {
        // S3Client is typically managed externally and should be closed by the caller
    }

    /**
     * Creates a new builder for S3RangeReader.
     *
     * @return a new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for S3RangeReader.
     */
    public static class Builder {
        private S3Reference s3Location;
        private S3Client s3Client;

        private Region region;
        private boolean forcePathStyle;

        // authentication related parameters
        private AwsCredentialsProvider credentialsProvider;
        private String awsAccessKeyId;
        private String awsSecretAccessKey;
        private boolean useDefaultCredentialsProvider;
        private String defaultCredentialsProfile;

        private Builder() {
            // Initialize with empty S3Location
            this.s3Location = new S3Reference();
        }

        /**
         * Sets the AWS access key ID.
         *
         * @param awsAccessKeyId the AWS access key ID
         * @return this builder
         */
        public Builder awsAccessKeyId(String awsAccessKeyId) {
            this.awsAccessKeyId = awsAccessKeyId;
            return this;
        }

        /**
         * Sets the AWS secret access key.
         *
         * @param awsSecretAccessKey the AWS secret access key
         * @return this builder
         */
        public Builder awsSecretAccessKey(String awsSecretAccessKey) {
            this.awsSecretAccessKey = awsSecretAccessKey;
            return this;
        }

        /**
         * Controls whether to use the AWS default credentials provider chain for authentication.
         *
         * <p>When set to {@code false} (the default), no credentials provider is configured,
         * allowing access to publicly accessible S3 resources without authentication.
         * This is useful for accessing public datasets like Overture Maps without requiring
         * AWS credentials to be configured.
         *
         * <p>When set to {@code true}, the AWS default credentials provider chain is used,
         * which looks for credentials in the standard AWS locations.
         *
         * <p><strong>Note:</strong> If explicit {@link #awsAccessKeyId(String)} and
         * {@link #awsSecretAccessKey(String)} are provided, they take precedence over this setting.
         *
         * @param useDefaultCredentialsProvider whether to use the default credentials provider chain
         * @return this builder
         */
        public Builder useDefaultCredentialsProvider(boolean useDefaultCredentialsProvider) {
            this.useDefaultCredentialsProvider = useDefaultCredentialsProvider;
            return this;
        }

        /**
         * Sets the AWS credentials profile name to use when the default credentials provider is enabled.
         *
         * <p>This parameter is only effective when {@link #useDefaultCredentialsProvider(boolean)}
         * is set to {@code true}. If not specified, the 'default' profile is used.
         *
         * <p>The profile should exist in the AWS credentials file (typically {@code ~/.aws/credentials})
         * or AWS config file (typically {@code ~/.aws/config}).
         *
         * <p>Example profiles:
         * <pre>{@code
         * // In ~/.aws/credentials
         * [default]
         * aws_access_key_id = AKIAIOSFODNN7EXAMPLE
         * aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
         *
         * [production]
         * aws_access_key_id = AKIAI44QH8DHBEXAMPLE
         * aws_secret_access_key = je7MtGbClwBF/2Zp9Utk/h3yCo8nvbEXAMPLEKEY
         * }</pre>
         *
         * @param defaultCredentialsProfile the AWS credentials profile name
         * @return this builder
         */
        public Builder defaultCredentialsProfile(String defaultCredentialsProfile) {
            this.defaultCredentialsProfile = defaultCredentialsProfile;
            return this;
        }

        /**
         * Sets the S3 client to use.
         *
         * @param s3Client the S3 client
         * @return this builder
         */
        public Builder s3Client(S3Client s3Client) {
            this.s3Client = Objects.requireNonNull(s3Client, "S3Client cannot be null");
            return this;
        }

        /**
         * Sets the AWS credentials provider.
         *
         * @param credentialsProvider the credentials provider
         * @return this builder
         */
        public Builder credentialsProvider(AwsCredentialsProvider credentialsProvider) {
            this.credentialsProvider =
                    Objects.requireNonNull(credentialsProvider, "Credentials provider cannot be null");
            return this;
        }

        /**
         * Sets the AWS region.
         *
         * @param region the AWS region
         * @return this builder
         */
        public Builder region(Region region) {
            this.region = Objects.requireNonNull(region, "Region cannot be null");
            return this;
        }

        /**
         * Sets a custom endpoint URI.
         *
         * @param endpoint the endpoint URI
         * @return this builder
         */
        public Builder endpoint(URI endpoint) {
            Objects.requireNonNull(endpoint, "Endpoint cannot be null");
            this.s3Location = this.s3Location.withEndpoint(endpoint);
            return this;
        }

        /**
         * Enables force path style for S3 client.
         * <p>
         * Note: When using the {@link #uri(URI)} method, path style will be automatically
         * enabled for non-AWS S3-compatible services (such as MinIO, Google Cloud Storage, etc.)
         * that require path-style addressing. This method allows explicit override of that behavior.
         *
         * @return this builder
         */
        public Builder forcePathStyle() {
            return forcePathStyle(true);
        }

        /**
         * Enable or disable S3 path style access. When enabled, requests will use
         * path-style addressing (e.g., {@code https://s3.amazonaws.com/bucket/key}). When disabled, virtual-hosted-style
         * addressing will be used instead (e.g., {@code https://bucket.s3.amazonaws.com/key}). This can be useful for
         * compatibility with S3-compatible storage systems that do not support virtual-hosted-style requests.
         * <p>
         * <strong>Automatic Detection:</strong> When using the {@link #uri(URI)} method, path style is automatically
         * enabled for non-AWS endpoints (detected via {@link S3Reference#requiresPathStyle()}). This method allows
         * explicit override of the automatic detection.
         * <p>
         * <strong>Examples:</strong>
         * <pre>{@code
         * // Automatic path style detection
         * builder.uri("http://localhost:9000/bucket/key")  // Automatically enables path style
         *
         * // Manual override
         * builder.uri("http://localhost:9000/bucket/key")
         *        .forcePathStyle(false)                    // Override automatic detection
         *
         * // AWS endpoints default to virtual hosted-style
         * builder.uri("https://bucket.s3.amazonaws.com/key")  // Path style remains false
         * }</pre>
         *
         * @param forcePathStyle whether to enable (true) or disable path style, defaults to {@code false}
         * @return this builder
         */
        public Builder forcePathStyle(boolean forcePathStyle) {
            this.forcePathStyle = forcePathStyle;
            return this;
        }

        /**
         * Sets the bucket name.
         *
         * @param bucket the bucket name
         * @return this builder
         */
        public Builder bucket(String bucket) {
            Objects.requireNonNull(bucket, "Bucket cannot be null");
            this.s3Location = this.s3Location.withBucket(bucket);
            return this;
        }

        /**
         * Sets the object key.
         *
         * @param key the object key
         * @return this builder
         */
        public Builder key(String key) {
            Objects.requireNonNull(key, "Key cannot be null");
            this.s3Location = this.s3Location.withKey(key);
            return this;
        }

        /**
         * Sets the bucket, key, and endpoint from an S3 URI.
         *
         * <p>Examples:
         * <pre>{@code
         * // AWS S3
         * builder.uri(URI.create("s3://my-bucket/path/file.txt"));
         * builder.uri(URI.create("https://my-bucket.s3.us-west-2.amazonaws.com/path/file.txt"));
         *
         * // MinIO - extracts custom endpoint and enables path-style
         * builder.uri(URI.create("http://localhost:9000/my-bucket/path/file.txt"));
         *
         * // Other S3-compatible services
         * builder.uri(URI.create("https://storage.googleapis.com/my-bucket/path/file.txt"));
         * }</pre>
         *
         * @param uri the S3 URI (s3|http|https://bucket/key)
         * @throws IllegalArgumentException if the uri scheme is not supported, or the bucket name and key can't be extracted from the URI
         * @return this builder
         */
        public Builder uri(URI uri) {
            Objects.requireNonNull(uri, "URI cannot be null");
            this.s3Location = S3CompatibleUrlParser.parseS3Url(uri);
            return this;
        }

        /**
         * Sets the bucket, key, and endpoint from an S3 URL string.
         *
         * @param uri the S3 URL string
         * @return this builder
         * @see #uri(URI)
         */
        public Builder uri(String uri) {
            return uri(URI.create(uri));
        }

        /**
         * Builds the S3RangeReader with the configured parameters.
         *
         * <p>This method creates a new {@link S3RangeReader} instance by:
         * <ol>
         * <li><strong>Validating required parameters:</strong> Ensures bucket and key are set</li>
         * <li><strong>Creating S3 client:</strong> If no explicit client was provided via
         * {@link #s3Client(S3Client)}, builds one using:
         *   <ul>
         *   <li>Credentials resolved via {@link #resolveCredentialsProvider()}</li>
         *   <li>Region resolved via {@link #resolveRegion()}</li>
         *   <li>Endpoint override if specified in the URI</li>
         *   <li>Path-style access when required by the endpoint or explicitly enabled</li>
         *   </ul>
         * </li>
         * <li><strong>Validating S3 object access:</strong> Performs a HEAD request to verify
         * the object exists and retrieve its size</li>
         * </ol>
         *
         * <p><strong>Thread Safety:</strong> The returned {@link S3RangeReader} is thread-safe
         * and can be used concurrently from multiple threads.
         *
         * @return a new S3RangeReader instance configured with the specified parameters
         * @throws IllegalStateException if bucket or key are not set
         * @throws IOException if S3 client creation fails, the S3 object doesn't exist,
         *         or other I/O errors occur during validation
         */
        public S3RangeReader build() throws IOException {
            if (s3Location.bucket() == null || s3Location.key() == null) {
                throw new IllegalStateException("Bucket and key must be set");
            }

            S3Client client = s3Client;
            if (client == null) {
                // Build S3 client
                S3ClientBuilder clientBuilder = S3Client.builder();

                clientBuilder.credentialsProvider(resolveCredentialsProvider());

                resolveRegion().ifPresent(clientBuilder::region);

                s3Location.endpointOverride().ifPresent(clientBuilder::endpointOverride);

                final boolean pathStyle = s3Location.requiresPathStyle() || forcePathStyle;
                clientBuilder.forcePathStyle(pathStyle);

                try {
                    client = clientBuilder.build();
                } catch (SdkException e) {
                    throw new IOException("Failed to create S3 client: " + e.getMessage(), e);
                }
            }

            return new S3RangeReader(client, s3Location);
        }

        /**
         * Resolves the AWS region to use for the S3 client based on the configured parameters.
         *
         * <p>This method determines which AWS region to use based on the following precedence:
         * <ol>
         * <li><strong>Explicit region:</strong> If {@link #region(Region)} was called,
         * that region is returned directly.</li>
         * <li><strong>URL-parsed region:</strong> If no explicit region was set but the S3 URL
         * contains region information (e.g., from virtual hosted-style URLs like
         * {@code https://bucket.s3.us-west-2.amazonaws.com/key}), that region is used.</li>
         * <li><strong>No region (AWS SDK default):</strong> If neither explicit nor URL-parsed
         * region is available, {@code empty()} is returned, allowing the AWS SDK to determine
         * the region using its default chain (environment variables, config files, etc.).</li>
         * </ol>
         *
         * @return the resolved AWS region, or {@code empty()} to use AWS SDK default region resolution
         */
        private Optional<Region> resolveRegion() {
            Region region = this.region;
            if (region == null) {
                if (s3Location.region() != null) {
                    // Use region parsed from URL if no explicit region was set
                    region = Region.of(s3Location.region());
                } else if (defaultCredentialsProfile != null) {
                    // if a profile was specified, we need to...
                    DefaultAwsRegionProviderChain profileProvider = DefaultAwsRegionProviderChain.builder()
                            .profileName(defaultCredentialsProfile)
                            .build();
                    region = profileProvider.getRegion();
                }
            }
            return ofNullable(region);
        }

        /**
         * Resolves the AWS credentials provider based on the configured parameters.
         *
         * <p>This method determines which credentials provider to use based on the following precedence:
         * <ol>
         * <li><strong>Explicit credentials provider:</strong> If {@link #credentialsProvider(AwsCredentialsProvider)}
         * was called, that provider is returned directly.</li>
         * <li><strong>Static credentials:</strong> If both {@link #awsAccessKeyId(String)} and
         * {@link #awsSecretAccessKey(String)} are provided, a {@link StaticCredentialsProvider}
         * is created with those credentials.</li>
         * <li><strong>Default chain with optional profile:</strong> If {@link #useDefaultCredentialsProvider(boolean)}
         * is set to {@code true}, the AWS default credentials provider chain is used. If a profile name
         * is also provided via {@link #defaultCredentialsProfile(String)}, the chain is built to
         * prioritize that specific profile.</li>
         * <li><strong>Forced profile provider:</strong> If {@link #useDefaultCredentialsProvider(boolean)} is
         * {@code false} but a {@link #defaultCredentialsProfile(String)} is specified, the
         * {@link ProfileCredentialsProvider} for that profile is used, bypassing the default chain.</li>
         * <li><strong>No credentials (public access):</strong> If none of the above are configured,
         * {@link AnonymousCredentialsProvider} is returned for unsigned requests to public S3 resources.</li>
         * </ol>
         *
         * @return The resolved {@link AwsCredentialsProvider} instance.
         */
        private AwsCredentialsProvider resolveCredentialsProvider() {

            if (this.credentialsProvider != null) {
                return this.credentialsProvider;
            }

            if (awsAccessKeyId != null && awsSecretAccessKey != null) {
                AwsBasicCredentials credentials = AwsBasicCredentials.create(awsAccessKeyId, awsSecretAccessKey);
                return StaticCredentialsProvider.create(credentials);
            }
            if (this.useDefaultCredentialsProvider) {
                DefaultCredentialsProvider.Builder defaultChainBuilder = DefaultCredentialsProvider.builder()
                        // Caches the last successful provider, avoiding repeated credential chain scanning
                        .reuseLastProviderEnabled(true)
                        // handle credential updates (like IAM role rotation) asynchronously without blocking requests
                        .asyncCredentialUpdateEnabled(true);

                if (defaultCredentialsProfile != null) {
                    defaultChainBuilder.profileName(defaultCredentialsProfile);
                }
                return defaultChainBuilder.build();
            }

            if (defaultCredentialsProfile != null) {
                // don't use the full default credentials chain, just the profile
                return ProfileCredentialsProvider.create(defaultCredentialsProfile);
            }

            return AnonymousCredentialsProvider.create();
        }
    }
}
