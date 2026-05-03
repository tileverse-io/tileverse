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

import java.net.http.HttpClient;

/**
 * Package-private indirection between {@link HttpStorage} and the underlying JDK {@link HttpClient}. Two
 * implementations:
 *
 * <ul>
 *   <li>{@link LeasedHttpHandle} (SPI path): wraps an {@link HttpClientCache.Lease}; {@link #close()} releases the
 *       lease so the cached {@code HttpClient} can shut down when the last reference drops.
 *   <li>{@link BorrowedHttpHandle} (escape-hatch path): wraps a caller-supplied {@code HttpClient}; {@link #close()} is
 *       a no-op so the caller retains ownership.
 * </ul>
 */
interface HttpClientHandle extends AutoCloseable {

    HttpClient client();

    @Override
    void close();
}
