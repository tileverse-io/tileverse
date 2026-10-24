/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.http;

import java.io.IOException;
import java.io.InputStream;
import java.net.http.HttpResponse;

/** Closes the bodies of answers left unread by the HTTP storage and its range reader. */
final class ResponseBodies {

    private ResponseBodies() {}

    /** Closes the body of {@code response} without reading it, ignoring a failure to close it. */
    static void closeQuietly(HttpResponse<InputStream> response) {
        try {
            response.body().close();
        } catch (IOException ignored) {
            // the body is left unread; a failed close loses nothing
        }
    }
}
