/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.pmtiles;

import java.io.IOException;

/**
 * Exception thrown when an unsupported compression type is used.
 */
@SuppressWarnings("serial")
public class UnsupportedCompressionException extends IOException {
    public UnsupportedCompressionException(String message) {
        super(message);
    }

    public UnsupportedCompressionException(String message, Throwable cause) {
        super(message, cause);
    }
}
