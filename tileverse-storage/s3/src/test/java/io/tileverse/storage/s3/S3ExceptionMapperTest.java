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
        assertThat(mapped).isInstanceOf(StorageException.class);
        assertThat(mapped).isNotInstanceOf(NotFoundException.class);
        assertThat(mapped.getCause()).isSameAs(ex);
    }
}
