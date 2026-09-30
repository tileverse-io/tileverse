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
import static org.assertj.core.api.SoftAssertions.assertSoftly;

import com.sun.net.httpserver.HttpExchange;
import io.tileverse.storage.CopyOptions;
import io.tileverse.storage.ReadHandle;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageException;
import io.tileverse.storage.azure.StandInBlobServer.RequestedRange;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Drives the Azure Blob and Data Lake storages through the real SDK pipeline and the JDK transport against a local HTTP
 * server leaving requests unanswered. An interrupted call fails with a {@link StorageException}, and a read of an open
 * stream with an {@link IOException}; either way the thread keeps its interrupt status.
 */
class AzureStorageInterruptedCallTest {

    private static final String KEY = "blob.bin";
    /** The SDK opens a blob stream by downloading a first block of 4 MiB. */
    private static final int FIRST_BLOCK_SIZE = 4 * 1024 * 1024;

    private static final int STREAMED_BLOB_SIZE = FIRST_BLOCK_SIZE + 16 * 1024;

    private final CountDownLatch requestHeld = new CountDownLatch(1);
    private final CountDownLatch testFinished = new CountDownLatch(1);
    private StandInBlobServer server;

    @AfterEach
    void stopServer() throws InterruptedException {
        testFinished.countDown();
        if (server != null) {
            server.stop();
        }
    }

    static Stream<Arguments> storages() {
        return Stream.of(
                storage("blob", StandInBlobServer::blobStorage),
                storage("data lake", StandInBlobServer::dataLakeStorage));
    }

    static Stream<Arguments> storageCalls() {
        List<Arguments> calls = List.of(
                call("stat", storage -> storage.stat(KEY)),
                call("list", storage -> storage.list("")),
                call("read", storage -> storage.read(KEY)),
                call("put", storage -> storage.put(KEY, new byte[] {1, 2, 3})),
                call("delete", storage -> storage.delete(KEY)),
                call("deleteAll", storage -> storage.deleteAll(List.of(KEY))),
                call("copy", storage -> storage.copy(KEY, "copy.bin", CopyOptions.defaults())),
                call("move", storage -> storage.move(KEY, "moved.bin", CopyOptions.defaults())));
        return storages().flatMap(storage -> calls.stream().map(call -> combine(storage, call)));
    }

    @ParameterizedTest(name = "{0} {2}")
    @MethodSource("storageCalls")
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void anInterruptedCallFailsWithAStorageExceptionAndKeepsTheInterruptStatus(
            String storageName, Function<StandInBlobServer, Storage> opener, String callName, Consumer<Storage> call)
            throws Exception {
        server = StandInBlobServer.start(this::holdUnanswered);

        try (Storage storage = opener.apply(server)) {
            InterruptedCall outcome = InterruptedCall.interruptWhen(this::aRequestIsHeld, () -> call.accept(storage));

            assertSoftly(softly -> {
                softly.assertThat(outcome.failure()).isInstanceOf(StorageException.class);
                softly.assertThat(outcome.interruptStatus())
                        .as("interrupt status after the call failed")
                        .isTrue();
            });
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("storages")
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void anInterruptedStreamReadFailsWithAnIOExceptionAndKeepsTheInterruptStatus(
            String storageName, Function<StandInBlobServer, Storage> opener) throws Exception {
        server = StandInBlobServer.start(this::answerTheFirstBlockOnly);

        try (Storage storage = opener.apply(server);
                ReadHandle handle = storage.read(KEY)) {
            InterruptedCall outcome = InterruptedCall.interruptWhen(
                    this::aRequestIsHeld, () -> handle.content().readAllBytes());

            assertSoftly(softly -> {
                softly.assertThat(outcome.failure()).isInstanceOf(IOException.class);
                softly.assertThat(outcome.interruptStatus())
                        .as("interrupt status after the stream read failed")
                        .isTrue();
            });
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void anInterruptedBatchDeleteNamesTheKeyOfTheCaller() throws Exception {
        server = StandInBlobServer.start(this::holdUnanswered);

        try (Storage storage = server.blobStorageAt("data/")) {
            InterruptedCall outcome = InterruptedCall.interruptWhen(
                    this::aRequestIsHeld, () -> storage.deleteAll(List.of("my file.bin")));

            assertThat(outcome.failure())
                    .isInstanceOf(StorageException.class)
                    .hasMessageContaining("key 'my file.bin'");
        }
    }

    private static Arguments storage(String name, Function<StandInBlobServer, Storage> opener) {
        return Arguments.of(name, opener);
    }

    private static Arguments call(String name, Consumer<Storage> call) {
        return Arguments.of(name, call);
    }

    private static Arguments combine(Arguments storage, Arguments call) {
        return Arguments.of(storage.get()[0], storage.get()[1], call.get()[0], call.get()[1]);
    }

    private boolean aRequestIsHeld() {
        return requestHeld.getCount() == 0;
    }

    private void holdUnanswered(HttpExchange exchange) {
        requestHeld.countDown();
        StandInBlobServer.holdUnanswered(exchange, testFinished);
    }

    /** Answers the download of the first block of the blob and holds every other request unanswered. */
    private void answerTheFirstBlockOnly(HttpExchange exchange) throws IOException {
        if (isFirstBlockDownload(exchange)) {
            sendFirstBlock(exchange);
        } else {
            holdUnanswered(exchange);
        }
    }

    private static boolean isFirstBlockDownload(HttpExchange exchange) {
        boolean ranged = exchange.getRequestHeaders().containsKey("x-ms-range")
                || exchange.getRequestHeaders().containsKey("Range");
        return ranged && RequestedRange.of(exchange).offset() == 0;
    }

    private static void sendFirstBlock(HttpExchange exchange) throws IOException {
        RequestedRange range = RequestedRange.of(exchange);
        StandInBlobServer.sendPartialContentHeaders(exchange, range, STREAMED_BLOB_SIZE);
        try (OutputStream body = exchange.getResponseBody()) {
            body.write(new byte[range.length()]);
        }
    }
}
