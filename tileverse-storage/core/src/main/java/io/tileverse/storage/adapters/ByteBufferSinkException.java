/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tileverse.storage.adapters;

import java.io.IOException;

/**
 * Signals that a {@link ByteBufferOutputStream} could not take the bytes offered to it: the producer sent more than the
 * accepted count, or the target buffer refused the write. Neither is an I/O failure of the producer, and retrying the
 * download does not help.
 *
 * <p>Checked, because the sink lives inside SDK download pipelines that propagate an {@link IOException} raised by the
 * stream but may drop anything else, leaving the read to wait out its timeout.
 */
public final class ByteBufferSinkException extends IOException {

    private static final long serialVersionUID = 1L;

    /** Creates a sink failure with the given message. */
    public ByteBufferSinkException(String message) {
        super(message);
    }

    /** Creates a sink failure caused by the target buffer's own exception. */
    public ByteBufferSinkException(String message, Throwable cause) {
        super(message, cause);
    }
}
