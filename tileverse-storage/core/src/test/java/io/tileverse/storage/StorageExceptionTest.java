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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import org.junit.jupiter.api.Test;

class StorageExceptionTest {

    @Test
    void allSubclassesExtendStorageException() {
        assertThat(StorageException.class).isAssignableFrom(NotFoundException.class);
        assertThat(StorageException.class).isAssignableFrom(AccessDeniedException.class);
        assertThat(StorageException.class).isAssignableFrom(ConflictException.class);
        assertThat(StorageException.class).isAssignableFrom(PreconditionFailedException.class);
        assertThat(StorageException.class).isAssignableFrom(RangeNotSatisfiableException.class);
        assertThat(StorageException.class).isAssignableFrom(UnsupportedCapabilityException.class);
        assertThat(StorageException.class).isAssignableFrom(InvalidKeyException.class);
        assertThat(StorageException.class).isAssignableFrom(TransientStorageException.class);
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
