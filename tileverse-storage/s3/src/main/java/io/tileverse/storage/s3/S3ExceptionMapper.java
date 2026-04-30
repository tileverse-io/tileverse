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

import io.tileverse.storage.AccessDeniedException;
import io.tileverse.storage.ConflictException;
import io.tileverse.storage.NotFoundException;
import io.tileverse.storage.PreconditionFailedException;
import io.tileverse.storage.RangeNotSatisfiableException;
import io.tileverse.storage.StorageException;
import io.tileverse.storage.TransientStorageException;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.NoSuchUploadException;
import software.amazon.awssdk.services.s3.model.S3Exception;

/** Translate AWS SDK v2 exceptions into the storage exception hierarchy. */
final class S3ExceptionMapper {

    private S3ExceptionMapper() {}

    static StorageException map(Exception cause, String contextKey) {
        if (cause instanceof NoSuchKeyException || cause instanceof NoSuchBucketException) {
            return new NotFoundException(messageFor(cause, contextKey, "not found"), cause);
        }
        if (cause instanceof NoSuchUploadException) {
            return new NotFoundException(messageFor(cause, contextKey, "no such upload"), cause);
        }
        int status = statusOf(cause);
        switch (status) {
            case 404:
                return new NotFoundException(messageFor(cause, contextKey, "not found"), cause);
            case 403:
                return new AccessDeniedException(messageFor(cause, contextKey, "access denied"), cause);
            case 409:
                return new ConflictException(messageFor(cause, contextKey, "conflict"), cause);
            case 412:
                return new PreconditionFailedException(messageFor(cause, contextKey, "precondition failed"), cause);
            case 416:
                return new RangeNotSatisfiableException(messageFor(cause, contextKey, "range not satisfiable"), cause);
            case 429, 500, 502, 503, 504:
                return new TransientStorageException(messageFor(cause, contextKey, "transient"), cause);
            default:
                return new StorageException(messageFor(cause, contextKey, "S3 error"), cause);
        }
    }

    private static int statusOf(Exception cause) {
        if (cause instanceof AwsServiceException ase) {
            return ase.statusCode();
        }
        if (cause instanceof S3Exception s3e) {
            return s3e.statusCode();
        }
        return -1;
    }

    private static String messageFor(Throwable cause, String contextKey, String label) {
        return label + " for key '" + contextKey + "': " + cause.getMessage();
    }
}
