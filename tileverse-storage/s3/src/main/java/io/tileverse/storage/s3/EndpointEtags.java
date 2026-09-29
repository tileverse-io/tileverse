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

import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jspecify.annotations.Nullable;
import software.amazon.awssdk.core.exception.SdkException;

/**
 * Whether the async client of one set of SDK clients rejected a response for want of an {@code ETag} header. A service
 * exporting an existing tree of files answers without the header for files not written through its S3 API. A CRT-based
 * async client, passed in an {@link S3ClientBundle}, demands the header and fails such a request before delivering any
 * byte. The clients built by {@link S3ClientCache} make no such demand and never record anything here.
 *
 * <p>One instance per client set, shared by its Storages and readers. The record is per endpoint, never per object, and
 * only moves from absent to omitted.
 */
final class EndpointEtags {

    /** Bounds the cause-chain search against a cycle. */
    private static final int MAX_CAUSE_DEPTH = 8;

    private static final String MISSING_ETAG_ERROR = "missing required etag";

    private final AtomicBoolean omitted = new AtomicBoolean();

    /** Whether the async client rejected a response of this endpoint for want of an ETag header. */
    boolean omitted() {
        return omitted.get();
    }

    /** Records a rejection by the async client for want of the header. */
    void recordOmission() {
        omitted.set(true);
    }

    /**
     * Whether a failure is the CRT client rejecting a response for want of an ETag header. The CRT keeps no error code
     * in the {@link SdkException} it raises, which leaves the message text as the only signal.
     */
    static boolean rejectedForMissingEtag(Throwable failure) {
        Throwable cause = failure;
        for (int depth = 0; cause != null && depth < MAX_CAUSE_DEPTH; depth++) {
            if (cause instanceof SdkException && namesTheMissingEtag(cause.getMessage())) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    private static boolean namesTheMissingEtag(@Nullable String message) {
        return message != null && message.toLowerCase(Locale.ROOT).contains(MISSING_ETAG_ERROR);
    }
}
