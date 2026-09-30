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

import com.sun.net.httpserver.HttpExchange;
import io.tileverse.storage.NotFoundException;
import io.tileverse.storage.ReadHandle;
import io.tileverse.storage.Storage;
import io.tileverse.storage.azure.StandInBlobServer.RequestedRange;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Drives reads of the Azure Blob and Data Lake storages through the real SDK pipeline and the JDK transport against a
 * local HTTP server.
 */
class AzureStorageReadTest {

    private static final String KEY = "blob.bin";
    private static final byte[] BLOB = new byte[128];

    private StandInBlobServer server;

    @AfterEach
    void stopServer() throws InterruptedException {
        if (server != null) {
            server.stop();
        }
    }

    static Stream<Arguments> storages() {
        return Stream.of(
                storage("blob", StandInBlobServer::blobStorage),
                storage("data lake", StandInBlobServer::dataLakeStorage));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("storages")
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aMissingKeyFailsWithNotFound(String storageName, Function<StandInBlobServer, Storage> opener)
            throws IOException {
        server = StandInBlobServer.start(AzureStorageReadTest::answerNotFound);

        try (Storage storage = opener.apply(server)) {
            assertThatThrownBy(() -> storage.read(KEY)).isInstanceOf(NotFoundException.class);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("storages")
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void anInvalidReadArgumentThrowsTheExceptionOfTheInputStreamContract(
            String storageName, Function<StandInBlobServer, Storage> opener) throws IOException {
        server = StandInBlobServer.start(AzureStorageReadTest::sendTheWholeBlob);

        try (Storage storage = opener.apply(server);
                ReadHandle handle = storage.read(KEY)) {
            InputStream content = handle.content();
            byte[] buffer = new byte[4];

            assertThatThrownBy(() -> content.read(buffer, 2, 3)).isInstanceOf(IndexOutOfBoundsException.class);
            assertThatThrownBy(() -> content.read(null, 0, 1)).isInstanceOf(NullPointerException.class);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("storages")
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aResetToAnExpiredMarkThrowsAnIOException(String storageName, Function<StandInBlobServer, Storage> opener)
            throws IOException {
        server = StandInBlobServer.start(AzureStorageReadTest::sendTheWholeBlob);

        try (Storage storage = opener.apply(server);
                ReadHandle handle = storage.read(KEY)) {
            InputStream content = handle.content();
            content.mark(8);
            content.readNBytes(64);

            assertThatThrownBy(content::reset).isInstanceOf(IOException.class);
        }
    }

    private static Arguments storage(String name, Function<StandInBlobServer, Storage> opener) {
        return Arguments.of(name, opener);
    }

    private static void answerNotFound(HttpExchange exchange) throws IOException {
        exchange.getResponseHeaders().set("x-ms-error-code", "BlobNotFound");
        exchange.sendResponseHeaders(404, -1);
        exchange.close();
    }

    /** Answers the download of the first block with the whole blob, shorter than a block. */
    private static void sendTheWholeBlob(HttpExchange exchange) throws IOException {
        RequestedRange wholeBlob = new RequestedRange(0, BLOB.length);
        StandInBlobServer.sendPartialContentHeaders(exchange, wholeBlob, BLOB.length);
        try (OutputStream body = exchange.getResponseBody()) {
            body.write(BLOB);
        }
    }
}
