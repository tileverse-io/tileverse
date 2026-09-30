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
import static org.awaitility.Awaitility.await;

import com.azure.core.http.jdk.httpclient.JdkHttpClientBuilder;
import com.azure.storage.blob.BlobClient;
import com.sun.net.httpserver.HttpExchange;
import io.tileverse.storage.StorageException;
import io.tileverse.storage.azure.StandInBlobServer.RequestedRange;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Drives {@link AzureBlobRangeReader} through the real SDK pipeline and the JDK transport against a local HTTP server
 * sending a range body slowly and leaving a properties request unanswered. An interrupted read gives up waiting while a
 * chunk may still be on its way from the transport thread; once the read has thrown, the caller owns the target again
 * and no byte of that body may land in it. An interrupted read or size request keeps the interrupt status of its
 * thread.
 */
class AzureBlobRangeReaderInterruptedReadTest {

    private static final int BODY_SIZE = 1024 * 1024;
    private static final int CHUNK_SIZE = 16 * 1024;
    private static final long CHUNK_PAUSE_MILLIS = 25;
    private static final byte[] BODY = createBody();

    private final CountDownLatch propertiesRequested = new CountDownLatch(1);
    private final CountDownLatch testFinished = new CountDownLatch(1);
    private StandInBlobServer server;
    private volatile boolean serving;
    private BlobClient blobClient;

    private static byte[] createBody() {
        byte[] body = new byte[BODY_SIZE];
        for (int i = 0; i < BODY_SIZE; i++) {
            body[i] = (byte) (i % 251 + 1);
        }
        return body;
    }

    @BeforeEach
    void startServer() throws IOException {
        serving = true;
        server = StandInBlobServer.start(this::serve);
        blobClient = server.blobClient(new JdkHttpClientBuilder().build());
    }

    @AfterEach
    void stopServer() throws InterruptedException {
        serving = false;
        testFinished.countDown();
        server.stop();
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void anInterruptedReadLeavesTheTargetUntouchedOnceItThrows() throws InterruptedException {
        AzureBlobRangeReader reader = new AzureBlobRangeReader(blobClient);
        ByteBuffer target = ByteBuffer.allocate(BODY_SIZE);
        FutureTask<Integer> read = new FutureTask<>(() -> reader.readRange(0, BODY_SIZE, target));
        Thread readingThread = new Thread(read, "interrupted-range-read");

        readingThread.start();
        try {
            await().atMost(Duration.ofSeconds(10)).until(() -> target.position() > 0);
            readingThread.interrupt();

            assertThatThrownBy(() -> read.get(10, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(StorageException.class);
            assertTargetStaysUnchanged(target);
        } finally {
            readingThread.interrupt();
            readingThread.join(TimeUnit.SECONDS.toMillis(10));
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void anInterruptedReadKeepsTheInterruptStatus() throws Exception {
        AzureBlobRangeReader reader = new AzureBlobRangeReader(blobClient);
        ByteBuffer target = ByteBuffer.allocate(BODY_SIZE);

        InterruptedCall outcome = InterruptedCall.interruptWhen(
                () -> target.position() > 0, () -> reader.readRange(0, BODY_SIZE, target));

        assertThat(outcome.failure()).isInstanceOf(StorageException.class);
        assertThat(outcome.interruptStatus())
                .as("interrupt status after the read failed")
                .isTrue();
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void anInterruptedSizeRequestKeepsTheInterruptStatus() throws Exception {
        AzureBlobRangeReader reader = new AzureBlobRangeReader(blobClient);

        InterruptedCall outcome =
                InterruptedCall.interruptWhen(() -> propertiesRequested.getCount() == 0, reader::size);

        assertThat(outcome.failure()).isInstanceOf(StorageException.class);
        assertThat(outcome.interruptStatus())
                .as("interrupt status after size() failed")
                .isTrue();
    }

    private static void assertTargetStaysUnchanged(ByteBuffer target) {
        int positionAfterFailure = target.position();
        byte[] contentsAfterFailure = target.array().clone();

        await().pollInterval(Duration.ofMillis(20))
                .during(Duration.ofMillis(500))
                .atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> {
                    assertThat(target.position())
                            .as("target position after the read failed")
                            .isEqualTo(positionAfterFailure);
                    assertThat(Arrays.equals(target.array(), contentsAfterFailure))
                            .as("target bytes after the read failed")
                            .isTrue();
                });
    }

    /** Leaves a properties request unanswered until the test ends; answers a ranged GET slowly. */
    private void serve(HttpExchange exchange) throws IOException {
        if ("HEAD".equals(exchange.getRequestMethod())) {
            propertiesRequested.countDown();
            StandInBlobServer.holdUnanswered(exchange, testFinished);
        } else {
            serveRangeSlowly(exchange);
        }
    }

    /** Answers a ranged GET with the requested bytes of {@link #BODY}, a chunk at a time with a pause in between. */
    private void serveRangeSlowly(HttpExchange exchange) throws IOException {
        RequestedRange range = RequestedRange.of(exchange);
        StandInBlobServer.sendPartialContentHeaders(exchange, range, BODY_SIZE);
        int length = range.length();
        try (OutputStream body = exchange.getResponseBody()) {
            for (int sent = 0; sent < length && serving; sent += CHUNK_SIZE) {
                body.write(BODY, range.offset() + sent, Math.min(CHUNK_SIZE, length - sent));
                body.flush();
                pauseBetweenChunks();
            }
        }
    }

    @SuppressWarnings("java:S2925") // the pause is the behavior of the stand-in server; no assertion waits on it
    private static void pauseBetweenChunks() throws IOException {
        try {
            TimeUnit.MILLISECONDS.sleep(CHUNK_PAUSE_MILLIS);
        } catch (InterruptedException stopping) {
            Thread.currentThread().interrupt();
            throw new IOException("Server stopping", stopping);
        }
    }
}
