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
