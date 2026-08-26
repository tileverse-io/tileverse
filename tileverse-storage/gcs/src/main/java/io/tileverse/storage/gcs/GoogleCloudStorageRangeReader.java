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
package io.tileverse.storage.gcs;

import static java.util.Objects.requireNonNull;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobGetOption;
import com.google.cloud.storage.Storage.BlobSourceOption;
import com.google.cloud.storage.StorageException;
import io.tileverse.storage.AbstractRangeReader;
import io.tileverse.storage.NotFoundException;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.batch.CoalescingPolicy;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

/**
 * A RangeReader implementation that reads from Google Cloud Storage.
 *
 * <p>This class enables reading data stored in Google Cloud Storage buckets using the Google Cloud Storage client
 * library for Java.
 *
 * <p>Batched reads merge nearby ranges under the object-store coalescing policy and run up to 8 fetches concurrently on
 * the shared batch executor; worst-case amplification is the requested bytes plus the merged gaps.
 */
@Slf4j
final class GoogleCloudStorageRangeReader extends AbstractRangeReader implements RangeReader {

    /** Fetch parallelism for batched reads on the shared batch executor. */
    private static final int MAX_CONCURRENT_FETCHES = 8;

    private final Storage storage;
    private final String bucket;
    private final String objectName;
    private final Optional<String> userProject;

    private final AtomicReference<OptionalLong> contentLength = new AtomicReference<>();

    /**
     * Creates a new GoogleCloudStorageRangeReader for the specified GCS object.
     *
     * <p>Construction performs no I/O. A missing object is reported by the first {@link #readRange(long, int)} or
     * {@link #size()} call instead of at construction time.
     *
     * @param storage The GCS Storage client to use
     * @param bucket The GCS bucket name
     * @param objectName The GCS object name
     * @param userProject the project to bill for a Requester Pays bucket, if any
     */
    GoogleCloudStorageRangeReader(Storage storage, String bucket, String objectName, Optional<String> userProject) {
        this.storage = requireNonNull(storage, "Storage client cannot be null");
        this.bucket = requireNonNull(bucket, "Bucket name cannot be null");
        this.objectName = requireNonNull(objectName, "Object name cannot be null");
        this.userProject = requireNonNull(userProject, "userProject cannot be null");
    }

    @Override
    protected int readRangeNoFlip(final long offset, final int actualLength, ByteBuffer target) {
        try {
            final long start = System.nanoTime();
            // Read the specified range from GCS, always against the current object generation
            try (ReadChannel reader = storage.reader(BlobId.of(bucket, objectName), readOptions())) {
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
                if (log.isDebugEnabled()) {
                    long end = System.nanoTime();
                    long millis = Duration.ofNanos(end - start).toMillis();
                    log.debug("range:[{} +{}], time: {}ms]", offset, actualLength, millis);
                }
                return totalBytesRead;
            }
        } catch (StorageException e) {
            throw SdkExceptionMapper.map(e, objectName);
        } catch (IOException e) {
            throw new io.tileverse.storage.StorageException("Failed to read range from GCS: " + e.getMessage(), e);
        }
    }

    private BlobSourceOption[] readOptions() {
        return userProject
                .map(p -> new BlobSourceOption[] {BlobSourceOption.userProject(p)})
                .orElseGet(() -> new BlobSourceOption[0]);
    }

    @Override
    protected CoalescingPolicy coalescingPolicy() {
        return CoalescingPolicy.objectStoreDefaults();
    }

    @Override
    protected int maxConcurrentFetches() {
        return MAX_CONCURRENT_FETCHES;
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
        List<BlobGetOption> getOpts = new ArrayList<>();
        getOpts.add(BlobGetOption.fields(Storage.BlobField.SIZE));
        userProject.ifPresent(p -> getOpts.add(BlobGetOption.userProject(p)));
        Blob blob;
        try {
            blob = storage.get(BlobId.of(bucket, objectName), getOpts.toArray(BlobGetOption[]::new));
        } catch (StorageException e) {
            throw SdkExceptionMapper.map(e, objectName);
        }
        if (blob == null) {
            throw new NotFoundException("GCS object not found: gs://" + bucket + "/" + objectName);
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
}
