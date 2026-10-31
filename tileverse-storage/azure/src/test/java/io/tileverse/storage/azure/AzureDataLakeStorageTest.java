/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.azure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import io.tileverse.storage.CopyOptions;
import io.tileverse.storage.PreconditionFailedException;
import io.tileverse.storage.Storage;
import io.tileverse.storage.WriteOptions;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Drives requests with an ETag condition through the real SDK pipeline and the JDK transport against a local HTTP
 * server. Azurite has no Data Lake endpoint. The server fails each condition, as the service does for a file with
 * another ETag, and records the requests it received.
 */
class AzureDataLakeStorageTest {

    private static final String STALE_ETAG = "\"0x1\"";
    private static final List<String> CONDITION_HEADERS = List.of("If-Match", "x-ms-source-if-match");

    private final List<String> requests = new CopyOnWriteArrayList<>();
    private StandInBlobServer server;

    @BeforeEach
    void startServer() throws IOException {
        server = StandInBlobServer.start(this::failConditionalRequests);
    }

    @AfterEach
    void stopServer() throws InterruptedException {
        server.stop();
    }

    /** Writes an object through one of the write methods of a storage. */
    @FunctionalInterface
    interface Writer {
        void write(Storage storage, String key, byte[] data, WriteOptions options) throws IOException;
    }

    /** A stream commits by uploading its spool file, like a write from a file. */
    static Stream<Arguments> writers() {
        return Stream.of(
                Arguments.argumentSet("bytes", (Writer) Storage::put),
                Arguments.argumentSet("file", (Writer) AzureDataLakeStorageTest::putFromFile));
    }

    private static void putFromFile(Storage storage, String key, byte[] data, WriteOptions options)
            throws IOException {
        Path source = Files.createTempFile("datalake-", ".bin");
        try {
            Files.write(source, data);
            storage.put(key, source, options);
        } finally {
            Files.delete(source);
        }
    }

    @ParameterizedTest
    @MethodSource("writers")
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void staleEtagWriteFails(Writer writer) throws IOException {
        WriteOptions ifMatch = WriteOptions.builder().ifMatchEtag(STALE_ETAG).build();

        try (Storage storage = server.dataLakeStorage()) {
            assertThatThrownBy(() -> writer.write(storage, "file.bin", new byte[] {1}, ifMatch))
                    .isInstanceOf(PreconditionFailedException.class);
        }
        assertThat(requests).containsExactly("PUT If-Match: " + STALE_ETAG);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void staleSourceEtagMoveFails() throws IOException {
        CopyOptions ifMatch = CopyOptions.builder().ifMatchSourceEtag(STALE_ETAG).build();

        try (Storage storage = server.dataLakeStorage()) {
            assertThatThrownBy(() -> storage.move("a.bin", "b.bin", ifMatch))
                    .isInstanceOf(PreconditionFailedException.class);
        }
        assertThat(requests).containsExactly("PUT x-ms-source-if-match: " + STALE_ETAG);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void staleSourceEtagCopyFails() throws IOException {
        CopyOptions ifMatch = CopyOptions.builder().ifMatchSourceEtag(STALE_ETAG).build();

        try (Storage storage = server.dataLakeStorage()) {
            assertThatThrownBy(() -> storage.copy("a.bin", "b.bin", ifMatch))
                    .isInstanceOf(PreconditionFailedException.class);
        }
        assertThat(requests).containsExactly("PUT x-ms-source-if-match: " + STALE_ETAG);
    }

    /** Fails a request with an ETag condition with a 412, and any other request with a 400. */
    private void failConditionalRequests(HttpExchange exchange) throws IOException {
        Headers headers = exchange.getRequestHeaders();
        String condition = CONDITION_HEADERS.stream()
                .filter(headers::containsKey)
                .map(name -> name + ": " + headers.getFirst(name))
                .findFirst()
                .orElse("without a condition");
        requests.add(exchange.getRequestMethod() + " " + condition);
        boolean conditional = !condition.equals("without a condition");
        exchange.getResponseHeaders().set("x-ms-error-code", conditional ? "ConditionNotMet" : "UnexpectedRequest");
        exchange.sendResponseHeaders(conditional ? 412 : 400, -1);
        exchange.close();
    }
}
