/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

@ExtendWith(MockitoExtension.class)
class BorrowedS3HandleTest {

    @Mock
    private S3AsyncClient client;

    @Mock
    private S3TransferManager transferManager;

    private final AtomicInteger built = new AtomicInteger();

    private BorrowedS3Handle handle() {
        return new BorrowedS3Handle(client, borrowed -> {
            built.incrementAndGet();
            return transferManager;
        });
    }

    @Test
    void theHandleServesTheCallersClient() {
        assertThat(handle().client()).isSameAs(client);
    }

    @Test
    void theTransferManagerIsBuiltOnTheFirstRequestOnly() {
        BorrowedS3Handle handle = handle();
        assertThat(built).hasValue(0);

        S3TransferManager first = handle.transferManager();

        assertThat(handle.transferManager()).isSameAs(first);
        assertThat(built).hasValue(1);
    }

    @Test
    void closingClosesTheTransferManagerAndLeavesTheClientOpen() {
        BorrowedS3Handle handle = handle();
        handle.transferManager();

        handle.close();

        verify(transferManager).close();
        verify(client, never()).close();
    }

    @Test
    void closingWithoutAnUploadBuildsNothing() {
        handle().close();

        assertThat(built).hasValue(0);
        verifyNoInteractions(client);
    }

    @Test
    void aTransferManagerRequestedAfterCloseFails() {
        BorrowedS3Handle handle = handle();
        handle.close();

        assertThatThrownBy(handle::transferManager).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void aCallerSuppliedClientHasNoPresigner() {
        assertThat(handle().presigner()).isEmpty();
    }

    @Test
    void aCallerSuppliedClientDoesNotPresign() {
        assertThat(handle().presigns()).isFalse();
    }
}
