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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import io.tileverse.storage.CopyOptions;
import io.tileverse.storage.PreconditionFailedException;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageEntry;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Drives copies between two Azure Data Lake storages rooted at different prefixes of one container, through the real
 * SDK pipeline and the JDK transport against a local HTTP server recording the requests. Every blob exists on this
 * server, and every copy succeeds.
 */
class AzureDataLakeStorageCopyTest {

    private static final int BLOB_SIZE = 3;
    private static final String SOURCE_BLOB = "/devstoreaccount1/container/source/blob.bin";
    private static final String DESTINATION_BLOB = "/devstoreaccount1/container/destination/copy.bin";

    private final List<String> requests = new CopyOnWriteArrayList<>();
    private final List<String> copySources = new CopyOnWriteArrayList<>();
    private StandInBlobServer server;

    @BeforeEach
    void startServer() throws IOException {
        server = StandInBlobServer.start(this::answerCopyOrProperties);
    }

    @AfterEach
    void stopServer() throws InterruptedException {
        server.stop();
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aCopyToAnotherStorageWritesToTheDestinationAndStatsIt() throws IOException {
        try (Storage source = server.dataLakeStorageAt("source/");
                Storage destination = server.dataLakeStorageAt("destination/")) {
            StorageEntry.File copied = source.copy("blob.bin", destination, "copy.bin", CopyOptions.defaults());

            assertThat(copySources).containsExactly(SOURCE_BLOB);
            assertThat(requests).containsExactly("PUT " + DESTINATION_BLOB, "HEAD " + DESTINATION_BLOB);
            assertThat(copied.key()).isEqualTo("copy.bin");
            assertThat(copied.size()).isEqualTo(BLOB_SIZE);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aConditionalCopyToAnotherStorageChecksTheDestination() throws IOException {
        CopyOptions ifNotExists =
                CopyOptions.builder().ifNotExistsAtDestination(true).build();

        try (Storage source = server.dataLakeStorageAt("source/");
                Storage destination = server.dataLakeStorageAt("destination/")) {
            assertThatThrownBy(() -> source.copy("blob.bin", destination, "copy.bin", ifNotExists))
                    .isInstanceOf(PreconditionFailedException.class);

            assertThat(requests).containsExactly("HEAD " + DESTINATION_BLOB);
        }
    }

    private void answerCopyOrProperties(HttpExchange exchange) throws IOException {
        requests.add(
                exchange.getRequestMethod() + " " + exchange.getRequestURI().getPath());
        String copySource = exchange.getRequestHeaders().getFirst("x-ms-copy-source");
        if (copySource == null) {
            sendProperties(exchange);
        } else {
            copySources.add(URI.create(copySource).getPath());
            sendCopySuccess(exchange);
        }
    }

    /** Answers a properties request, a HEAD, for a blob of {@link #BLOB_SIZE} bytes. */
    private static void sendProperties(HttpExchange exchange) throws IOException {
        Headers headers = exchange.getResponseHeaders();
        setCommonBlobHeaders(headers);
        headers.set("Content-Length", String.valueOf(BLOB_SIZE));
        headers.set("Content-Type", "application/octet-stream");
        headers.set("x-ms-blob-type", "BlockBlob");
        exchange.sendResponseHeaders(200, -1);
        exchange.close();
    }

    /** Answers a synchronous copy from a URL as completed. */
    private static void sendCopySuccess(HttpExchange exchange) throws IOException {
        Headers headers = exchange.getResponseHeaders();
        setCommonBlobHeaders(headers);
        headers.set("x-ms-copy-id", "00000000-0000-0000-0000-000000000001");
        headers.set("x-ms-copy-status", "success");
        exchange.sendResponseHeaders(202, -1);
        exchange.close();
    }

    private static void setCommonBlobHeaders(Headers headers) {
        headers.set("ETag", "\"0x1\"");
        headers.set("Last-Modified", "Tue, 08 Sep 2026 00:00:00 GMT");
        headers.set("x-ms-version", "2025-01-05");
        headers.set("x-ms-request-id", "00000000-0000-0000-0000-000000000000");
    }
}
