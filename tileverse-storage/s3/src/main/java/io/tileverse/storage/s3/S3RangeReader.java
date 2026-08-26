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
import io.tileverse.storage.ContentRange;
import io.tileverse.storage.NotFoundException;
import io.tileverse.storage.RangeNotSatisfiableException;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.RangeRequest;
import io.tileverse.storage.StorageException;
import io.tileverse.storage.batch.BatchPlanner;
import io.tileverse.storage.batch.CoalescingPolicy;
import io.tileverse.storage.batch.PlannedFetch;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.Nullable;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.RequestPayer;
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
 *
 * <h2>Batched Reads</h2>
 *
 * {@code readRanges} merges nearby ranges under the object-store coalescing policy and, when the CRT
 * {@link S3AsyncClient} is present, fetches the planned ranges in parallel with one async {@code getObject} each.
 * Without the async client the {@link AbstractRangeReader} template runs the same plan on the shared batch executor.
 */
final class S3RangeReader extends AbstractRangeReader implements RangeReader {

    /** Fetch parallelism when batched reads fall back to the AbstractRangeReader template (no async client). */
    private static final int MAX_CONCURRENT_FETCHES = 8;

    private final S3Client s3Client;

    @Nullable
    private final S3AsyncClient asyncClient;

    private final S3Reference s3Location;
    private final boolean requesterPays;

    private final AtomicReference<OptionalLong> contentLength = new AtomicReference<>();

    /**
     * Creates a reader without an async client; batched reads run through the {@link AbstractRangeReader} template.
     *
     * @param s3Client The S3 client to use
     * @param s3Location The S3 reference (bucket + key)
     * @param requesterPays when {@code true}, every request adds {@code x-amz-request-payer: requester}
     */
    S3RangeReader(S3Client s3Client, S3Reference s3Location, boolean requesterPays) {
        this(s3Client, null, s3Location, requesterPays);
    }

    /**
     * Creates a new S3RangeReader for the specified S3 object.
     *
     * <p>Construction performs no I/O. A missing object is reported by the first {@link #readRange(long, int)} or
     * {@link #size()} call instead of at construction time.
     *
     * @param s3Client The S3 client to use for single reads and metadata
     * @param asyncClient the CRT async client for parallel batched reads, or null to batch through the shared executor
     * @param s3Location The S3 reference (bucket + key)
     * @param requesterPays when {@code true}, every request adds {@code x-amz-request-payer: requester}
     */
    S3RangeReader(
            S3Client s3Client, @Nullable S3AsyncClient asyncClient, S3Reference s3Location, boolean requesterPays) {
        this.s3Client = Objects.requireNonNull(s3Client, "S3Client cannot be null");
        this.asyncClient = asyncClient;
        this.s3Location = Objects.requireNonNull(s3Location, "S3Location cannot be null");
        this.requesterPays = requesterPays;
    }

    private GetObjectRequest buildGetRequest(long offset, int length) {
        long rangeEnd = offset + length - 1;
        GetObjectRequest.Builder request = GetObjectRequest.builder()
                .bucket(s3Location.bucket())
                .key(s3Location.key())
                .range("bytes=" + offset + "-" + rangeEnd);
        if (requesterPays) {
            request.requestPayer(RequestPayer.REQUESTER);
        }
        return request.build();
    }

    @Override
    protected int readRangeNoFlip(final long offset, final int actualLength, ByteBuffer target) {
        try {
            ResponseBytes<GetObjectResponse> objectBytes =
                    s3Client.getObjectAsBytes(buildGetRequest(offset, actualLength));
            captureSizeFrom(objectBytes.response());
            byte[] data = objectBytes.asByteArray();
            if (data.length > actualLength) {
                throw new StorageException(
                        "Server returned more data than requested: got " + data.length + ", requested " + actualLength);
            }
            target.put(data);
            return data.length;
        } catch (NoSuchKeyException e) {
            throw new NotFoundException("S3 object does not exist: s3://" + s3Location, e);
        } catch (S3Exception e) {
            throw S3ExceptionMapper.map(e, s3Location.key());
        } catch (SdkException e) {
            throw new StorageException("Failed to read range from S3: " + e.getMessage(), e);
        }
    }

    @Override
    protected CoalescingPolicy coalescingPolicy() {
        return CoalescingPolicy.objectStoreDefaults();
    }

    @Override
    protected int maxConcurrentFetches() {
        return MAX_CONCURRENT_FETCHES;
    }

    /**
     * Reads a batch with one parallel CRT {@code getObject} per planned fetch when the async client is present; without
     * it, the batched-read template runs the same plan on the shared executor through the sync client.
     *
     * <p>A fetch answered 416 (entirely past EOF) reports 0 bytes for its entries, exactly like {@code readRange}; any
     * other failure aborts the whole call after the in-flight fetches complete. Worst-case amplification: the requested
     * bytes plus the gaps the object-store policy merges, at most {@link CoalescingPolicy#maxFetchBytes()} per fetch.
     *
     * @param requests the ranges to read and the buffers they land in
     * @return the number of bytes read per request, in request order
     */
    @Override
    public int[] readRanges(List<RangeRequest> requests) {
        if (asyncClient == null) {
            return super.readRanges(requests);
        }
        RangeRequest.validate(requests);
        if (requests.isEmpty()) {
            return new int[0];
        }
        List<PlannedFetch> fetches = BatchPlanner.plan(requests, coalescingPolicy());
        int[] counts = new int[requests.size()];
        if (fetches.isEmpty()) {
            return counts;
        }
        CompletableFuture<?>[] outcomes = new CompletableFuture<?>[fetches.size()];
        for (int i = 0; i < outcomes.length; i++) {
            outcomes[i] = fetchAsync(fetches.get(i), requests, counts);
        }
        try {
            CompletableFuture.allOf(outcomes).join();
        } catch (CompletionException failure) {
            throw unwrapBatchFailure(failure);
        }
        return counts;
    }

    /**
     * Issues one async GET for a fetch and scatters the result. A 416 leaves the fetch's entries at 0 and completes
     * normally; every other failure completes the future exceptionally with the mapped storage exception.
     */
    private CompletableFuture<Void> fetchAsync(PlannedFetch fetch, List<RangeRequest> requests, int[] counts) {
        GetObjectRequest request =
                buildGetRequest(fetch.range().offset(), fetch.range().length());
        return asyncClient
                .getObject(request, AsyncResponseTransformer.toBytes())
                .handle((fetched, failure) -> {
                    if (failure == null) {
                        captureSizeFrom(fetched.response());
                        ByteBuffer data = fetched.asByteBuffer();
                        fetch.scatter(data, data.remaining(), requests, counts);
                        return null;
                    }
                    StorageException translated = unwrapBatchFailure(failure);
                    if (translated instanceof RangeNotSatisfiableException) {
                        return null;
                    }
                    throw translated;
                });
    }

    /** Unwraps async completion wrappers and maps SDK failures onto the storage exception hierarchy. */
    private StorageException unwrapBatchFailure(Throwable failure) {
        Throwable cause = failure;
        while ((cause instanceof CompletionException || cause instanceof ExecutionException)
                && cause.getCause() != null) {
            cause = cause.getCause();
        }
        if (cause instanceof StorageException storageFailure) {
            return storageFailure;
        }
        if (cause instanceof NoSuchKeyException noSuchKey) {
            return new NotFoundException("S3 object does not exist: s3://" + s3Location, noSuchKey);
        }
        if (cause instanceof S3Exception s3Failure) {
            return S3ExceptionMapper.map(s3Failure, s3Location.key());
        }
        return new StorageException("Failed to read ranges from S3: " + cause.getMessage(), cause);
    }

    @Override
    public OptionalLong size() {
        OptionalLong known = contentLength.get();
        if (known == null) {
            known = contentLength.updateAndGet(current -> current != null ? current : fetchSize());
        }
        return known;
    }

    private OptionalLong fetchSize() {
        try {
            HeadObjectRequest.Builder headBuilder =
                    HeadObjectRequest.builder().bucket(s3Location.bucket()).key(s3Location.key());
            if (requesterPays) {
                headBuilder.requestPayer(RequestPayer.REQUESTER);
            }
            HeadObjectResponse headResponse = s3Client.headObject(headBuilder.build());
            Long size = headResponse.contentLength();
            return size == null ? OptionalLong.empty() : OptionalLong.of(size);
        } catch (NoSuchKeyException e) {
            throw new NotFoundException("S3 object does not exist: s3://" + s3Location, e);
        } catch (S3Exception e) {
            throw S3ExceptionMapper.map(e, s3Location.key());
        } catch (SdkException e) {
            throw new StorageException("Failed to access S3 object " + s3Location + ": " + e.getMessage(), e);
        }
    }

    /**
     * Captures the total object size from a range response's {@code Content-Range} header, when not already known. Lets
     * a read that happens before any {@link #size()} call populate the memoized value for free, without an extra HEAD
     * request.
     */
    private void captureSizeFrom(GetObjectResponse response) {
        if (contentLength.get() != null) {
            return;
        }
        ContentRange.totalOf(response.contentRange())
                .ifPresent(total -> contentLength.compareAndSet(null, OptionalLong.of(total)));
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
