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
import com.google.cloud.storage.StorageException;
import io.tileverse.storage.AbstractRangeReader;
import io.tileverse.storage.NotFoundException;
import io.tileverse.storage.RangeReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.OptionalLong;
import lombok.extern.slf4j.Slf4j;

/**
 * A RangeReader implementation that reads from Google Cloud Storage.
 *
 * <p>This class enables reading data stored in Google Cloud Storage buckets using the Google Cloud Storage client
 * library for Java.
 */
@Slf4j
final class GoogleCloudStorageRangeReader extends AbstractRangeReader implements RangeReader {

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
     * @throws io.tileverse.storage.StorageException If a storage error occurs
     */
    GoogleCloudStorageRangeReader(Storage storage, String bucket, String objectName) {
        this.storage = requireNonNull(storage, "Storage client cannot be null");
        this.bucket = requireNonNull(bucket, "Bucket name cannot be null");
        this.objectName = requireNonNull(objectName, "Object name cannot be null");
        BlobId blobId = BlobId.of(bucket, objectName);
        try {
            this.blob = storage.get(blobId);
        } catch (StorageException e) {
            throw SdkExceptionMapper.map(e, objectName);
        }

        if (blob == null || !blob.exists()) {
            throw new NotFoundException("GCS object not found: gs://" + bucket + "/" + objectName);
        }
    }

    @Override
    protected int readRangeNoFlip(final long offset, final int actualLength, ByteBuffer target) {
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

    @Override
    public OptionalLong size() {
        if (blob == null || !blob.exists()) {
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
