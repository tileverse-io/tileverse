/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.s3;

import static org.assertj.core.api.Assertions.assertThat;

import io.tileverse.storage.AccessDeniedException;
import io.tileverse.storage.NotFoundException;
import io.tileverse.storage.PreconditionFailedException;
import io.tileverse.storage.RangeNotSatisfiableException;
import io.tileverse.storage.StorageException;
import io.tileverse.storage.TransientStorageException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

class S3ExceptionMapperTest {

    @Test
    void noSuchKeyMapsToNotFound() {
        StorageException mapped = S3ExceptionMapper.map(
                NoSuchKeyException.builder().message("not found").build(), "k");
        assertThat(mapped).isInstanceOf(NotFoundException.class);
    }

    @Test
    void noSuchBucketMapsToNotFound() {
        StorageException mapped = S3ExceptionMapper.map(
                NoSuchBucketException.builder().message("nope").build(), "k");
        assertThat(mapped).isInstanceOf(NotFoundException.class);
    }

    @Test
    void status404MapsToNotFound() {
        S3Exception ex = (S3Exception)
                S3Exception.builder().statusCode(404).message("not found").build();
        assertThat(S3ExceptionMapper.map(ex, "k")).isInstanceOf(NotFoundException.class);
    }

    @Test
    void status403MapsToAccessDenied() {
        S3Exception ex = (S3Exception)
                S3Exception.builder().statusCode(403).message("denied").build();
        assertThat(S3ExceptionMapper.map(ex, "k")).isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void status412MapsToPreconditionFailed() {
        S3Exception ex = (S3Exception)
                S3Exception.builder().statusCode(412).message("etag mismatch").build();
        assertThat(S3ExceptionMapper.map(ex, "k")).isInstanceOf(PreconditionFailedException.class);
    }

    @Test
    void status416MapsToRangeNotSatisfiable() {
        S3Exception ex = (S3Exception)
                S3Exception.builder().statusCode(416).message("out of range").build();
        assertThat(S3ExceptionMapper.map(ex, "k")).isInstanceOf(RangeNotSatisfiableException.class);
    }

    @Test
    void status500MapsToTransient() {
        S3Exception ex = (S3Exception)
                S3Exception.builder().statusCode(500).message("oops").build();
        assertThat(S3ExceptionMapper.map(ex, "k")).isInstanceOf(TransientStorageException.class);
    }

    @Test
    void status429MapsToTransient() {
        S3Exception ex = (S3Exception)
                S3Exception.builder().statusCode(429).message("throttled").build();
        assertThat(S3ExceptionMapper.map(ex, "k")).isInstanceOf(TransientStorageException.class);
    }

    @Test
    void unmappedAwsExceptionFallsBackToStorageException() {
        AwsServiceException ex = (AwsServiceException)
                AwsServiceException.builder().statusCode(418).message("teapot").build();
        StorageException mapped = S3ExceptionMapper.map(ex, "k");
        assertThat(mapped).isInstanceOf(StorageException.class).isNotInstanceOf(NotFoundException.class);
        assertThat(mapped.getCause()).isSameAs(ex);
    }
}
