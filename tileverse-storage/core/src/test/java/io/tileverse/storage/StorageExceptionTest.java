/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import org.junit.jupiter.api.Test;

class StorageExceptionTest {

    @Test
    void allSubclassesExtendStorageException() {
        assertThat(StorageException.class)
                .isAssignableFrom(NotFoundException.class)
                .isAssignableFrom(AccessDeniedException.class)
                .isAssignableFrom(ConflictException.class)
                .isAssignableFrom(PreconditionFailedException.class)
                .isAssignableFrom(RangeNotSatisfiableException.class)
                .isAssignableFrom(UnsupportedCapabilityException.class)
                .isAssignableFrom(InvalidKeyException.class)
                .isAssignableFrom(TransientStorageException.class);
        assertThat(RuntimeException.class).isAssignableFrom(StorageException.class);
    }

    @Test
    void preservesMessageAndCause() {
        IOException root = new IOException("root cause");
        NotFoundException ex = new NotFoundException("missing key", root);
        assertThat(ex.getMessage()).isEqualTo("missing key");
        assertThat(ex.getCause()).isSameAs(root);
    }

    @Test
    void unsupportedCapabilityCarriesCapabilityName() {
        UnsupportedCapabilityException ex = new UnsupportedCapabilityException("list");
        assertThat(ex.getMessage()).contains("list");
        assertThatThrownBy(() -> {
                    throw ex;
                })
                .isInstanceOf(StorageException.class);
    }
}
