/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.azure;

import com.azure.core.exception.HttpResponseException;
import io.tileverse.storage.AccessDeniedException;
import io.tileverse.storage.ConflictException;
import io.tileverse.storage.NotFoundException;
import io.tileverse.storage.PreconditionFailedException;
import io.tileverse.storage.RangeNotSatisfiableException;
import io.tileverse.storage.StorageException;
import io.tileverse.storage.TransientStorageException;
import java.io.IOException;

/** Translate Azure SDK exceptions into the storage exception hierarchy. */
final class AzureExceptionMapper {

    private AzureExceptionMapper() {}

    static StorageException map(Exception cause, String contextKey) {
        int status = -1;
        if (cause instanceof HttpResponseException httpExc && httpExc.getResponse() != null) {
            status = httpExc.getResponse().getStatusCode();
        }
        switch (status) {
            case 404:
                return new NotFoundException(messageFor(cause, contextKey, "not found"), cause);
            case 403:
                return new AccessDeniedException(messageFor(cause, contextKey, "access denied"), cause);
            case 409:
                return new ConflictException(messageFor(cause, contextKey, "conflict"), cause);
            // A read with ifModifiedSince of an object not modified since that date
            case 304:
                return new PreconditionFailedException(messageFor(cause, contextKey, "not modified"), cause);
            case 412:
                return new PreconditionFailedException(messageFor(cause, contextKey, "precondition failed"), cause);
            case 416:
                return new RangeNotSatisfiableException(messageFor(cause, contextKey, "range not satisfiable"), cause);
            case 429, 500, 502, 503, 504:
                return new TransientStorageException(messageFor(cause, contextKey, "transient"), cause);
            default:
                return new StorageException(messageFor(cause, contextKey, "Azure error"), cause);
        }
    }

    /**
     * Maps a failure missed by the SDK exception catch of the caller. An error response keeps its status mapping,
     * whatever its SDK: a Data Lake read reports a missing file as a Blob error. An interrupted synchronous SDK call
     * clears the interrupt status of the calling thread; this restores it.
     */
    static StorageException mapUnexpected(RuntimeException failure, String contextKey) {
        if (failure instanceof HttpResponseException) {
            return map(failure, contextKey);
        }
        restoreInterruptStatus(failure);
        return new StorageException(messageFor(failure, contextKey, "Azure request failed"), failure);
    }

    /**
     * Wraps a failure of a stream read in the {@link IOException} required by the {@code InputStream} contract,
     * restoring the interrupt status of the calling thread.
     */
    static IOException mapReadFailure(RuntimeException failure) {
        restoreInterruptStatus(failure);
        return new IOException(failure);
    }

    private static String messageFor(Throwable cause, String contextKey, String label) {
        return label + " for key '" + contextKey + "': " + cause.getMessage();
    }

    /**
     * Only an {@link InterruptedException} counts: a transport timeout is an {@code InterruptedIOException} and must
     * leave the interrupt status alone.
     */
    private static void restoreInterruptStatus(RuntimeException failure) {
        if (causedByInterrupt(failure)) {
            Thread.currentThread().interrupt();
        }
    }

    private static boolean causedByInterrupt(Throwable failure) {
        Throwable cause = failure;
        while (cause != null) {
            if (cause instanceof InterruptedException) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }
}
