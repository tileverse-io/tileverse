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
