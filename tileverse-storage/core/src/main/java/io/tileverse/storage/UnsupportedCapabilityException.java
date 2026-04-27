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
package io.tileverse.storage;

/**
 * Thrown when a caller invokes an operation that the backend does not support (e.g. {@code list} on an HTTP-only
 * Storage, or {@code presignGet} on local FS). The capability name (matching {@code StorageCapabilities} accessors)
 * appears in the message.
 */
@SuppressWarnings("serial")
public class UnsupportedCapabilityException extends StorageException {

    public UnsupportedCapabilityException(String capabilityName) {
        super("Storage capability not supported: " + capabilityName);
    }

    public UnsupportedCapabilityException(String capabilityName, Throwable cause) {
        super("Storage capability not supported: " + capabilityName, cause);
    }
}
