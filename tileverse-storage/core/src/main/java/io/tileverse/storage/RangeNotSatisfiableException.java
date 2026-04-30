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

/** Thrown when a requested byte range is not satisfiable (offset past EOF, etc.). Maps to HTTP 416. */
@SuppressWarnings("serial")
public class RangeNotSatisfiableException extends StorageException {

    public RangeNotSatisfiableException(String message) {
        super(message);
    }

    public RangeNotSatisfiableException(String message, Throwable cause) {
        super(message, cause);
    }
}
