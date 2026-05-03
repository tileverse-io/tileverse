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
package io.tileverse.storage.s3;

import io.tileverse.storage.AbstractRangeReader;
import io.tileverse.storage.NotFoundException;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.StorageException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.OptionalLong;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * A {@link RangeReader} implementation that reads data from an AWS S3-compatible object storage service.
 *
 * <p>This class enables efficient reading of data from S3 objects by leveraging the
 * {@link software.amazon.awssdk.services.s3.S3Client} from the AWS SDK for Java v2. It is designed to handle both
 * standard AWS S3 and self-hosted S3-compatible services like MinIO.
 *
 * <h2>Authentication and Configuration</h2>
 *
 * The {@link Builder} for this class provides a flexible and robust mechanism for resolving credentials and other
 * client settings. The builder determines which credentials provider to use based on a defined precedence:
 *
 * <ol>
 *   <li><b>Explicit Credentials:</b> If an explicit {@link AwsCredentialsProvider} is provided, or if an access key and
 *       secret key are set directly, these are used.
 *   <li><b>Default Credential Chain:</b> If {@code useDefaultCredentialsProvider} is enabled, the client first attempts
 *       to resolve credentials from the AWS default credential chain, which checks environment variables, system
 *       properties, and shared credentials files. If a {@code defaultCredentialsProfile} is also specified, the chain
 *       is configured to prioritize that profile.
 *   <li><b>Forced Profile:</b> If a {@code defaultCredentialsProfile} is set but {@code useDefaultCredentialsProvider}
 *       is disabled, the client bypasses the full default chain and uses only the {@link ProfileCredentialsProvider}
 *       for the specified profile.
 *   <li><b>Anonymous Access:</b> If no credentials are explicitly configured, the client uses
 *       {@link AnonymousCredentialsProvider} to make unsigned requests.
 * </ol>
 *
 * <h2>Profile-Based Configuration</h2>
 *
 * When a named profile (e.g., 'minio') is used, the builder also attempts to resolve the AWS region from the
 * corresponding section in the {@code ~/.aws/config} file. This allows for a cleaner separation of credentials and
 * configuration. For S3-compatible services like MinIO, the region is a required parameter for the SDK's signing
 * process, even though the service itself may not use it.
 *
 * <h2>S3-Compatible Endpoints</h2>
 *
 * This builder supports custom S3-compatible endpoints via the {@code endpointOverride} method. For most self-hosted
 * services (e.g., MinIO), it is critical to enable <b>path-style access</b> by setting {@code forcePathStyle(true)} to
 * ensure the request is correctly addressed to the bucket.
 */
final class S3RangeReader extends AbstractRangeReader implements RangeReader {

    private final S3Client s3Client;
    private final S3Reference s3Location;

    private final OptionalLong contentLength;

    /**
     * Creates a new S3RangeReader for the specified S3 object.
     *
     * @param s3Client The S3 client to use
     * @param s3Location The S3 reference (bucket + key)
     * @throws StorageException If a storage error occurs
     */
    S3RangeReader(S3Client s3Client, S3Reference s3Location) {
        this.s3Client = Objects.requireNonNull(s3Client, "S3Client cannot be null");
        this.s3Location = Objects.requireNonNull(s3Location, "S3Location cannot be null");
        // Eager HEAD: surface NotFoundException at construction time so callers don't get
        // mid-stream failures, and cache the content length for subsequent size() calls.
        try {
            HeadObjectRequest headRequest = HeadObjectRequest.builder()
                    .bucket(s3Location.bucket())
                    .key(s3Location.key())
                    .build();
            HeadObjectResponse headResponse = s3Client.headObject(headRequest);
            Long size = headResponse.contentLength();
            this.contentLength = size == null ? OptionalLong.empty() : OptionalLong.of(size);
        } catch (NoSuchKeyException e) {
            throw new NotFoundException("S3 object does not exist: s3://" + s3Location, e);
        } catch (S3Exception e) {
            throw S3ExceptionMapper.map(e, s3Location.key());
        } catch (SdkException e) {
            throw new StorageException("Failed to access S3 object " + s3Location + ": " + e.getMessage(), e);
        }
    }

    @Override
    protected int readRangeNoFlip(final long offset, final int actualLength, ByteBuffer target) {
        long rangeEnd = offset + actualLength - 1;
        try {
            GetObjectRequest rangeRequest = GetObjectRequest.builder()
                    .bucket(s3Location.bucket())
                    .key(s3Location.key())
                    .range("bytes=" + offset + "-" + rangeEnd)
                    .build();
            ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(rangeRequest);
            if (objectBytes.response().contentLength() != actualLength) {
                throw new StorageException("Unexpected content length: got "
                        + objectBytes.response().contentLength()
                        + ", expected "
                        + actualLength);
            }
            byte[] data = objectBytes.asByteArray();
            target.put(data);
            return data.length;
        } catch (S3Exception e) {
            throw S3ExceptionMapper.map(e, s3Location.key());
        } catch (SdkException e) {
            throw new StorageException("Failed to read range from S3: " + e.getMessage(), e);
        }
    }

    @Override
    public OptionalLong size() {
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
}
