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
import java.util.function.Function;
import org.jspecify.annotations.Nullable;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

/**
 * {@link S3ClientHandle} over an {@link S3AsyncClient} passed by the caller, who keeps it: {@link #close()} closes the
 * transfer manager built by this handle at the first multipart upload, never the client. A caller-supplied client has
 * no presigner: presigning needs the region, endpoint and path style of a client set.
 */
final class BorrowedS3Handle implements S3ClientHandle {

    private final S3AsyncClient client;
    private final Function<S3AsyncClient, S3TransferManager> transferManagers;

    @Nullable
    private S3TransferManager transferManager; // guarded by this

    private boolean closed; // guarded by this

    BorrowedS3Handle(S3AsyncClient client) {
        this(client, BorrowedS3Handle::newTransferManager);
    }

    /** @param transferManagers builds the transfer manager over the caller's client */
    BorrowedS3Handle(S3AsyncClient client, Function<S3AsyncClient, S3TransferManager> transferManagers) {
        this.client = Objects.requireNonNull(client, "client");
        this.transferManagers = Objects.requireNonNull(transferManagers, "transferManagers");
    }

    private static S3TransferManager newTransferManager(S3AsyncClient client) {
        return S3TransferManager.builder().s3Client(client).build();
    }

    @Override
    public S3AsyncClient client() {
        return client;
    }

    @Override
    public synchronized S3TransferManager transferManager() {
        if (closed) {
            throw new IllegalStateException("S3 Storage is closed");
        }
        if (transferManager == null) {
            transferManager = transferManagers.apply(client);
        }
        return transferManager;
    }

    @Override
    public boolean presigns() {
        return false;
    }

    @Override
    public Optional<S3Presigner> presigner() {
        return Optional.empty();
    }

    @Override
    public synchronized void close() {
        closed = true;
        if (transferManager != null) {
            transferManager.close();
        }
    }
}
