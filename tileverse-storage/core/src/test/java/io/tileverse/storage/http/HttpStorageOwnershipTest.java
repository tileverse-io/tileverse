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

import static io.tileverse.storage.http.HttpStorageProvider.HTTP_CONNECTION_TIMEOUT_MILLIS;
import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageConfig;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Verifies HttpStorage's client-ownership semantics:
 *
 * <ul>
 *   <li>Two {@code HttpStorage}s built from identical configs share one underlying {@code HttpClient} via
 *       {@link HttpClientCache}.
 *   <li>Two {@code HttpStorage}s built from configs that differ on a cache-key field get distinct clients.
 *   <li>Closing a {@link HttpRangeReader} does NOT shut down the {@code HttpClient}; a sibling reader on the same
 *       Storage continues to work, and the client survives until the last Storage holding a lease closes.
 * </ul>
 */
class HttpStorageOwnershipTest {

    private static HttpServer server;
    private static URI baseUri;

    @BeforeAll
    static void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            byte[] body = "hello".getBytes();
            String range = exchange.getRequestHeaders().getFirst("Range");
            int status = range == null ? 200 : 206;
            exchange.getResponseHeaders().add("Accept-Ranges", "bytes");
            exchange.getResponseHeaders().add("Content-Length", String.valueOf(body.length));
            exchange.sendResponseHeaders(status, body.length);
            try (var os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.start();
        baseUri = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/");
    }

    @AfterAll
    static void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    private final HttpStorageProvider provider = new HttpStorageProvider();

    @Test
    void sameConfigSharesUnderlyingHttpClient() {
        StorageConfig a = new StorageConfig(baseUri);
        StorageConfig b = new StorageConfig(baseUri);
        try (HttpStorage sa = (HttpStorage) provider.createStorage(a);
                HttpStorage sb = (HttpStorage) provider.createStorage(b)) {
            assertThat(sa.client()).isSameAs(sb.client());
        }
    }

    @Test
    void differentConfigGetsDifferentHttpClient() {
        StorageConfig fast = new StorageConfig(baseUri).setParameter(HTTP_CONNECTION_TIMEOUT_MILLIS, 1_000);
        StorageConfig slow = new StorageConfig(baseUri).setParameter(HTTP_CONNECTION_TIMEOUT_MILLIS, 30_000);
        try (HttpStorage sa = (HttpStorage) provider.createStorage(fast);
                HttpStorage sb = (HttpStorage) provider.createStorage(slow)) {
            assertThat(sa.client()).isNotSameAs(sb.client());
        }
    }

    @Test
    void closingOneRangeReaderDoesNotBreakSiblingReader() throws IOException {
        try (Storage storage = provider.createStorage(new StorageConfig(baseUri));
                RangeReader first = storage.openRangeReader("placeholder.txt");
                RangeReader second = storage.openRangeReader("placeholder.txt")) {

            first.close(); // must NOT shut down the underlying HttpClient

            ByteBuffer buf = ByteBuffer.allocate(5);
            int read = second.readRange(0, 5, buf);
            assertThat(read).isEqualTo(5);
            assertThat(new String(buf.array(), 0, 5)).isEqualTo("hello");
        }
    }

    @Test
    void closingStorageDoesNotShutDownClientWhileOtherStorageHoldsLease() throws IOException {
        // Two Storages, same config, share a client via cache. Closing one keeps the lease (the other still holds it),
        // so the surviving Storage's reader still works.
        try (Storage second = provider.createStorage(new StorageConfig(baseUri))) {
            HttpClient sharedClient;
            try (Storage first = provider.createStorage(new StorageConfig(baseUri))) {
                sharedClient = ((HttpStorage) first).client();
                assertThat(((HttpStorage) second).client()).isSameAs(sharedClient);
            }
            // first closed -> refcount dropped from 2 to 1; client must still serve requests via the second Storage
            try (RangeReader reader = second.openRangeReader("placeholder.txt")) {
                ByteBuffer buf = ByteBuffer.allocate(5);
                assertThat(reader.readRange(0, 5, buf)).isEqualTo(5);
            }
        }
    }
}
