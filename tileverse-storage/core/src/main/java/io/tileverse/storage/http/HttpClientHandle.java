/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
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
