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
package io.tileverse.storage.azure;

import com.azure.core.exception.HttpResponseException;
import io.tileverse.storage.AccessDeniedException;
import io.tileverse.storage.ConflictException;
import io.tileverse.storage.NotFoundException;
import io.tileverse.storage.PreconditionFailedException;
import io.tileverse.storage.RangeNotSatisfiableException;
import io.tileverse.storage.StorageException;
import io.tileverse.storage.TransientStorageException;

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

    private static String messageFor(Throwable cause, String contextKey, String label) {
        return label + " for key '" + contextKey + "': " + cause.getMessage();
    }
}
