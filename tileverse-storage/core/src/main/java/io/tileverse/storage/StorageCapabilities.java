/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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
package io.tileverse.storage;

import java.time.Duration;
import java.util.Optional;
import org.jspecify.annotations.NullMarked;

/**
 * Reports what a {@link Storage} backend supports. Callers may interrogate this record before invoking optional
 * methods, or rely on require* helpers that throw {@link UnsupportedCapabilityException} on the first unsupported call.
 *
 * @param rangeReads whether {@link Storage#openRangeReader(String)} is supported. All first-party backends report
 *     {@code true}; {@code false} would mean the backend can only stream sequentially.
 * @param streamingReads whether {@link Storage#read(String, ReadOptions)} is supported. All first-party backends report
 *     {@code true}.
 * @param stat whether {@link Storage#stat(String)} can return file metadata (size, last-modified, ETag, optional
 *     content-type) without fetching the body. The HTTP backend reports {@code true} when the origin honors
 *     {@code HEAD}.
 * @param userMetadata whether the backend preserves and returns user-defined key/value metadata stored alongside an
 *     object (e.g. S3 {@code x-amz-meta-*}, Azure blob metadata, GCS custom metadata). Local FS and HTTP report
 *     {@code false}.
 * @param list whether {@link Storage#list(String)} (any overload) is supported. HTTP reports {@code false}; all object
 *     stores and local FS report {@code true}.
 * @param hierarchicalList whether the backend can list with a delimiter and return synthetic prefix entries (i.e.
 *     distinguish between "files" and "directory-shaped prefixes" in a single page). Implies {@code list}. S3 GP, Azure
 *     Blob, GCS, and local FS report {@code true}; S3 Express is partial.
 * @param realDirectories whether the backend has true directories with their own existence and metadata - as opposed to
 *     synthetic prefixes derived from object keys. {@code true} for local FS, Azure DataLake Gen2 (HNS), and GCS HNS;
 *     {@code false} for S3 GP, Azure Blob (flat), and GCS flat. Affects {@link StorageEntry.Directory} vs
 *     {@link StorageEntry.Prefix} discrimination during listing.
 * @param writes whether any of {@link Storage#put} / {@code openOutputStream} / {@link Storage#delete} /
 *     {@link Storage#copy} / {@link Storage#move} is supported. HTTP reports {@code false}.
 * @param multipartUpload whether large objects are uploaded in multiple parts transparently (S3 multipart, Azure block
 *     blobs, GCS resumable). When {@code true}, {@code multipartThresholdBytes} indicates the cutover size. Local FS
 *     reports {@code false} (single rename).
 * @param multipartThresholdBytes payload size in bytes at or above which the backend switches from single-shot to
 *     multipart upload. Must be {@code >= 0}; {@code 0} means "not applicable" (the field has no meaning for backends
 *     that report {@code multipartUpload == false}).
 * @param conditionalWrite whether the backend honors conditional headers on writes - specifically {@code If-None-Match:
 *     *} for create-only semantics ({@link io.tileverse.storage.PreconditionFailedException} on collision) and
 *     {@code If-Match: <etag>} for compare-and-set replacement.
 * @param bulkDelete whether the backend supports {@link Storage#deleteAll(java.util.Collection)} as a single
 *     round-trip. When {@code false}, callers should expect {@code N} sequential deletes; when {@code true},
 *     {@code bulkDeleteBatchLimit} caps the per-call batch size.
 * @param bulkDeleteBatchLimit maximum number of keys per bulk-delete request. Must be {@code >= 1}. {@code 1} is used
 *     both for single-only backends and for backends that enforce a batch size of one; callers should chunk larger
 *     requests at this limit. S3 caps at {@code 1000}; Azure Blob batch is around {@code 256}.
 * @param deleteReportsExistence whether {@link Storage#deleteAll(java.util.Collection)} populates
 *     {@link DeleteResult#didNotExist()} accurately. {@code true} for backends whose bulk-delete API distinguishes
 *     "removed" from "did not exist" (local FS, Azure Blob, Azure DataLake, GCS); {@code false} for S3, whose
 *     {@code DeleteObjects} response does not flag missing keys, so all requested keys land in
 *     {@link DeleteResult#deleted()} regardless.
 * @param serverSideCopy whether {@link Storage#copy(String, Storage, String, CopyOptions)} between containers/buckets
 *     on the same backend can be executed by the service without streaming bytes through the client. When
 *     {@code false}, the implementation falls back to download-and-reupload.
 * @param atomicMove whether {@link Storage#move(String, String)} is atomic (single rename) rather than
 *     copy-then-delete. {@code true} for local FS, DataLake Gen2 HNS, and GCS HNS; {@code false} for flat object
 *     stores.
 * @param presignedUrls whether the backend can produce presigned URLs for unauthenticated GET (and PUT, where
 *     supported) via {@link Storage#presignGet(String, Duration)} / {@link Storage#presignPut(String, Duration,
 *     PresignWriteOptions)}.
 * @param maxPresignTtl optional upper bound on the time-to-live of a presigned URL accepted by the backend.
 *     {@link Optional#empty()} means "no provider-imposed cap" (the caller's requested TTL is used as-is). Distinct
 *     from credential session TTL: a presigned URL outlives its TTL only insofar as the signing credentials are still
 *     valid.
 * @param versioning whether the backend keeps prior object versions when keys are overwritten or deleted, addressable
 *     via the {@code versionId} returned in {@link StorageEntry.File}. Reflects the bucket/container configuration, not
 *     just the service capability: a versioning-capable bucket with versioning disabled reports {@code false}.
 * @param strongReadAfterWrite whether a successful write is immediately visible to subsequent reads from the same
 *     {@code Storage}. All first-party object-store backends report {@code true} (S3, Azure Blob, and GCS provide
 *     strong read-after-write); HTTP reports {@code false} (cache-controlled).
 */
@NullMarked
public record StorageCapabilities(
        boolean rangeReads,
        boolean streamingReads,
        boolean stat,
        boolean userMetadata,
        boolean list,
        boolean hierarchicalList,
        boolean realDirectories,
        boolean writes,
        boolean multipartUpload,
        long multipartThresholdBytes,
        boolean conditionalWrite,
        boolean bulkDelete,
        int bulkDeleteBatchLimit,
        boolean deleteReportsExistence,
        boolean serverSideCopy,
        boolean atomicMove,
        boolean presignedUrls,
        Optional<Duration> maxPresignTtl,
        boolean versioning,
        boolean strongReadAfterWrite) {

    public StorageCapabilities {
        if (multipartThresholdBytes < 0) {
            throw new IllegalArgumentException("multipartThresholdBytes must be >= 0");
        }
        if (bulkDeleteBatchLimit < 1) {
            throw new IllegalArgumentException("bulkDeleteBatchLimit must be >= 1");
        }
    }

    /**
     * Default for HTTP-style read-only backends: range and streaming reads with stat support and strong
     * read-after-write; all listing, writing, copy, presign, and versioning capabilities disabled.
     */
    public static StorageCapabilities rangeReadOnly() {
        return builder()
                .rangeReads(true)
                .streamingReads(true)
                .stat(true)
                .strongReadAfterWrite(true)
                .build();
    }

    /** Throws {@link UnsupportedCapabilityException} if {@link #list} is {@code false}. */
    public void requireList() throws UnsupportedCapabilityException {
        if (!list()) {
            throw new UnsupportedCapabilityException("list");
        }
    }

    /** Throws {@link UnsupportedCapabilityException} if {@link #writes} is {@code false}. */
    public void requireWrites() throws UnsupportedCapabilityException {
        if (!writes()) {
            throw new UnsupportedCapabilityException("writes");
        }
    }

    /** Throws {@link UnsupportedCapabilityException} if {@link #serverSideCopy} is {@code false}. */
    public void requireServerSideCopy() throws UnsupportedCapabilityException {
        if (!serverSideCopy()) {
            throw new UnsupportedCapabilityException("serverSideCopy");
        }
    }

    /** Throws {@link UnsupportedCapabilityException} if {@link #presignedUrls} is {@code false}. */
    public void requirePresignedUrls() throws UnsupportedCapabilityException {
        if (!presignedUrls()) {
            throw new UnsupportedCapabilityException("presignedUrls");
        }
    }

    /** Throws {@link UnsupportedCapabilityException} if {@link #bulkDelete} is {@code false}. */
    public void requireBulkDelete() throws UnsupportedCapabilityException {
        if (!bulkDelete()) {
            throw new UnsupportedCapabilityException("bulkDelete");
        }
    }

    /** Returns a fresh builder; all flags default to {@code false}, scalars to their minima. */
    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private boolean rangeReads;
        private boolean streamingReads;
        private boolean stat;
        private boolean userMetadata;
        private boolean list;
        private boolean hierarchicalList;
        private boolean realDirectories;
        private boolean writes;
        private boolean multipartUpload;
        private long multipartThresholdBytes;
        private boolean conditionalWrite;
        private boolean bulkDelete;
        private int bulkDeleteBatchLimit = 1;
        private boolean deleteReportsExistence;
        private boolean serverSideCopy;
        private boolean atomicMove;
        private boolean presignedUrls;
        private Optional<Duration> maxPresignTtl = Optional.empty();
        private boolean versioning;
        private boolean strongReadAfterWrite;

        public Builder rangeReads(boolean v) {
            this.rangeReads = v;
            return this;
        }

        public Builder streamingReads(boolean v) {
            this.streamingReads = v;
            return this;
        }

        public Builder stat(boolean v) {
            this.stat = v;
            return this;
        }

        public Builder userMetadata(boolean v) {
            this.userMetadata = v;
            return this;
        }

        public Builder list(boolean v) {
            this.list = v;
            return this;
        }

        public Builder hierarchicalList(boolean v) {
            this.hierarchicalList = v;
            return this;
        }

        public Builder realDirectories(boolean v) {
            this.realDirectories = v;
            return this;
        }

        public Builder writes(boolean v) {
            this.writes = v;
            return this;
        }

        public Builder multipartUpload(boolean v) {
            this.multipartUpload = v;
            return this;
        }

        public Builder multipartThresholdBytes(long v) {
            this.multipartThresholdBytes = v;
            return this;
        }

        public Builder conditionalWrite(boolean v) {
            this.conditionalWrite = v;
            return this;
        }

        public Builder bulkDelete(boolean v) {
            this.bulkDelete = v;
            return this;
        }

        public Builder bulkDeleteBatchLimit(int v) {
            this.bulkDeleteBatchLimit = v;
            return this;
        }

        public Builder deleteReportsExistence(boolean v) {
            this.deleteReportsExistence = v;
            return this;
        }

        public Builder serverSideCopy(boolean v) {
            this.serverSideCopy = v;
            return this;
        }

        public Builder atomicMove(boolean v) {
            this.atomicMove = v;
            return this;
        }

        public Builder presignedUrls(boolean v) {
            this.presignedUrls = v;
            return this;
        }

        public Builder maxPresignTtl(Optional<Duration> v) {
            this.maxPresignTtl = v;
            return this;
        }

        public Builder versioning(boolean v) {
            this.versioning = v;
            return this;
        }

        public Builder strongReadAfterWrite(boolean v) {
            this.strongReadAfterWrite = v;
            return this;
        }

        public StorageCapabilities build() {
            return new StorageCapabilities(
                    rangeReads,
                    streamingReads,
                    stat,
                    userMetadata,
                    list,
                    hierarchicalList,
                    realDirectories,
                    writes,
                    multipartUpload,
                    multipartThresholdBytes,
                    conditionalWrite,
                    bulkDelete,
                    bulkDeleteBatchLimit,
                    deleteReportsExistence,
                    serverSideCopy,
                    atomicMove,
                    presignedUrls,
                    maxPresignTtl,
                    versioning,
                    strongReadAfterWrite);
        }
    }
}
