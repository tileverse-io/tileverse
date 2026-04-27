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
package io.tileverse.storage.s3;

import java.util.Objects;
import java.util.Optional;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

/**
 * Bundle of caller-supplied AWS SDK objects that {@link S3StorageProvider#open(java.net.URI, S3ClientBundle) accepts}
 * for the SDK-injection escape hatch. {@link S3Storage} draws on up to four SDK objects to cover its full feature
 * surface:
 *
 * <ul>
 *   <li>{@link S3Client} (sync) — required; drives stat, list, range read, single-shot write, copy, delete.
 *   <li>{@link S3AsyncClient} (CRT-based) — optional; required for {@code read} (parallel multi-part GET).
 *   <li>{@link S3TransferManager} — optional; required for multipart upload of larger payloads.
 *   <li>{@link S3Presigner} — optional; required for {@code presignGet}/{@code presignPut}.
 * </ul>
 *
 * <p>Use {@link #syncOnly(S3Client)} when only sync-path operations are needed (range reads, small writes); the missing
 * capabilities surface as {@link io.tileverse.storage.UnsupportedCapabilityException} at the call site. Use
 * {@link #of(S3Client, S3AsyncClient, S3TransferManager, S3Presigner)} for full feature parity with the SPI path.
 *
 * <p>The returned {@code Storage} <b>borrows</b> all four SDK objects; closing the {@code Storage} does NOT close them.
 * The caller retains ownership and lifetime control.
 */
public record S3ClientBundle(
        S3Client sync,
        Optional<S3AsyncClient> async,
        Optional<S3TransferManager> transferManager,
        Optional<S3Presigner> presigner) {

    /** Compact-constructor null check. */
    public S3ClientBundle {
        Objects.requireNonNull(sync, "sync");
        Objects.requireNonNull(async, "async");
        Objects.requireNonNull(transferManager, "transferManager");
        Objects.requireNonNull(presigner, "presigner");
    }

    /**
     * Bundle carrying only the sync {@link S3Client}. The async client, transfer manager, and presigner are absent;
     * {@code read}, multipart upload, and presigned URL operations on the resulting {@code Storage} throw
     * {@link io.tileverse.storage.UnsupportedCapabilityException}.
     *
     * @param sync the sync S3 client
     * @return a new bundle with only the sync client
     */
    public static S3ClientBundle syncOnly(S3Client sync) {
        return new S3ClientBundle(sync, Optional.empty(), Optional.empty(), Optional.empty());
    }

    /**
     * Bundle carrying all four SDK objects. The resulting {@code Storage} has the same capability set as one obtained
     * via the SPI path.
     *
     * @param sync the sync S3 client
     * @param async the CRT-based async S3 client
     * @param transferManager the transfer manager
     * @param presigner the presigner
     * @return a new bundle with all four clients
     */
    public static S3ClientBundle of(
            S3Client sync, S3AsyncClient async, S3TransferManager transferManager, S3Presigner presigner) {
        return new S3ClientBundle(
                sync,
                Optional.of(Objects.requireNonNull(async, "async")),
                Optional.of(Objects.requireNonNull(transferManager, "transferManager")),
                Optional.of(Objects.requireNonNull(presigner, "presigner")));
    }
}
