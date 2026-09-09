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
package io.tileverse.storage.azure;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.azure.core.http.jdk.httpclient.JdkHttpClientBuilder;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobClientBuilder;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.tileverse.storage.StorageException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Drives {@link AzureBlobRangeReader} through the real SDK pipeline and the JDK transport against a local HTTP server.
 * The transport delivers the body on its own thread and drops an unchecked exception raised there by the sink, which
 * leaves the read waiting out its full timeout; a sink failure must reach the caller as a prompt error instead.
 */
class AzureBlobRangeReaderStreamFailureTest {

    private static final int BLOB_SIZE = 1000;
    private static final int EXTRA_BYTES = 50;

    private HttpServer server;
    private BlobClient blobClient;

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::serveRangeWithExtraBytes);
        server.start();
        String endpoint = "http://127.0.0.1:" + server.getAddress().getPort() + "/devstoreaccount1";
        blobClient = new BlobClientBuilder()
                .endpoint(endpoint)
                .containerName("container")
                .blobName("blob.bin")
                .httpClient(new JdkHttpClientBuilder().build())
                .buildClient();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    @Test
    @Timeout(value = 20, unit = TimeUnit.SECONDS)
    void aSinkFailureOnTheTransportThreadFailsTheReadPromptly() {
        AzureBlobRangeReader reader = new AzureBlobRangeReader(blobClient);
        ByteBuffer target = ByteBuffer.allocate(100);

        assertThatThrownBy(() -> reader.readRange(0, 100, target))
                .isInstanceOf(StorageException.class)
                .hasMessageContaining("more data than requested");
    }

    /** Answers every ranged GET with more bytes than the range asked for. */
    private void serveRangeWithExtraBytes(HttpExchange exchange) throws IOException {
        int length = requestedLength(exchange.getRequestHeaders()) + EXTRA_BYTES;
        Headers headers = exchange.getResponseHeaders();
        headers.set("Content-Type", "application/octet-stream");
        headers.set("Content-Range", "bytes 0-" + (length - 1) + "/" + BLOB_SIZE);
        headers.set("Accept-Ranges", "bytes");
        headers.set("ETag", "\"0x1\"");
        headers.set("Last-Modified", "Tue, 08 Sep 2026 00:00:00 GMT");
        headers.set("x-ms-blob-type", "BlockBlob");
        headers.set("x-ms-version", "2025-01-05");
        headers.set("x-ms-request-id", "00000000-0000-0000-0000-000000000000");
        exchange.sendResponseHeaders(206, length);
        try (OutputStream body = exchange.getResponseBody()) {
            body.write(new byte[length]);
        }
    }

    private static int requestedLength(Headers requestHeaders) {
        String range = requestHeaders.getFirst("x-ms-range");
        if (range == null) {
            range = requestHeaders.getFirst("Range");
        }
        String[] bounds = range.substring("bytes=".length()).split("-");
        return Integer.parseInt(bounds[1]) - Integer.parseInt(bounds[0]) + 1;
    }
}
