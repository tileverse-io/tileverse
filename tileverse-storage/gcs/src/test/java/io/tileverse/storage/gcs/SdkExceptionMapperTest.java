/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.gcs;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.cloud.storage.StorageException;
import io.tileverse.storage.AccessDeniedException;
import io.tileverse.storage.NotFoundException;
import io.tileverse.storage.PreconditionFailedException;
import io.tileverse.storage.RangeNotSatisfiableException;
import io.tileverse.storage.TransientStorageException;
import org.junit.jupiter.api.Test;

class SdkExceptionMapperTest {

    @Test
    void status404MapsToNotFound() {
        assertThat(SdkExceptionMapper.map(new StorageException(404, "not found"), "k"))
                .isInstanceOf(NotFoundException.class);
    }

    @Test
    void status403MapsToAccessDenied() {
        assertThat(SdkExceptionMapper.map(new StorageException(403, "forbidden"), "k"))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void status412MapsToPreconditionFailed() {
        assertThat(SdkExceptionMapper.map(new StorageException(412, "no match"), "k"))
                .isInstanceOf(PreconditionFailedException.class);
    }

    @Test
    void status416MapsToRangeNotSatisfiable() {
        assertThat(SdkExceptionMapper.map(new StorageException(416, "out of range"), "k"))
                .isInstanceOf(RangeNotSatisfiableException.class);
    }

    @Test
    void status500MapsToTransient() {
        assertThat(SdkExceptionMapper.map(new StorageException(500, "oops"), "k"))
                .isInstanceOf(TransientStorageException.class);
    }

    @Test
    void status429MapsToTransient() {
        assertThat(SdkExceptionMapper.map(new StorageException(429, "throttled"), "k"))
                .isInstanceOf(TransientStorageException.class);
    }

    @Test
    void unmappedFallsBack() {
        assertThat(SdkExceptionMapper.map(new StorageException(418, "teapot"), "k"))
                .isInstanceOf(io.tileverse.storage.StorageException.class);
    }
}
