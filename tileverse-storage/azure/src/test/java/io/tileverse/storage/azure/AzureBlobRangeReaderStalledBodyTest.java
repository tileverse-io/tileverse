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

import com.azure.core.http.HttpClient;
import com.azure.core.http.jdk.httpclient.JdkHttpClientBuilder;
import com.azure.storage.blob.BlobClient;
import com.sun.net.httpserver.HttpExchange;
import io.tileverse.storage.StorageException;
import io.tileverse.storage.azure.StandInBlobServer.RequestedRange;
import java.io.IOException;
import java.io.OutputStream;
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Drives {@link AzureBlobRangeReader} through the real SDK pipeline and the JDK transport against a local HTTP server
 * sending part of a range body and then nothing, with the connection left open. The read has no deadline on the whole
 * range; the read timeout of the HTTP client must still end it. The SDK resumes the stalled download three times with a
 * growing backoff, and the read fails after about 13 seconds. An interrupted read ends the download for good: nothing
 * resumes it once the caller has given up.
 */
class AzureBlobRangeReaderStalledBodyTest {

    private static final int BLOB_SIZE = 64 * 1024;
    private static final Duration READ_TIMEOUT = Duration.ofMillis(500);

    private final CountDownLatch testFinished = new CountDownLatch(1);
    private final AtomicInteger rangeRequests = new AtomicInteger();
    private StandInBlobServer server;
    private BlobClient blobClient;

    @BeforeEach
    void startServer() throws IOException {
        server = StandInBlobServer.start(this::sendPartOfTheRangeThenStall);
        HttpClient httpClient =
                new JdkHttpClientBuilder().readTimeout(READ_TIMEOUT).build();
        blobClient = server.blobClient(httpClient);
    }

    @AfterEach
    void stopServer() throws InterruptedException {
        testFinished.countDown();
        server.stop();
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aStalledBodyFailsTheReadOnTheReadTimeout() {
        AzureBlobRangeReader reader = new AzureBlobRangeReader(blobClient);
        ByteBuffer target = ByteBuffer.allocate(BLOB_SIZE);

        assertThatThrownBy(() -> reader.readRange(0, BLOB_SIZE, target))
                .isInstanceOf(StorageException.class)
                .hasRootCauseInstanceOf(HttpTimeoutException.class);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void anInterruptedReadDoesNotResumeTheStalledDownload() throws InterruptedException {
        AzureBlobRangeReader reader = new AzureBlobRangeReader(blobClient);
        ByteBuffer target = ByteBuffer.allocate(BLOB_SIZE);
        FutureTask<Integer> read = new FutureTask<>(() -> reader.readRange(0, BLOB_SIZE, target));
        Thread readingThread = new Thread(read, "interrupted-stalled-read");

        readingThread.start();
        try {
            await().atMost(Duration.ofSeconds(10)).until(() -> target.position() > 0);
            readingThread.interrupt();
            assertThatThrownBy(() -> read.get(10, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(StorageException.class);

            await().during(Duration.ofSeconds(2))
                    .atMost(Duration.ofSeconds(3))
                    .untilAsserted(() -> assertThat(rangeRequests.get())
                            .as("range requests after the read failed")
                            .isEqualTo(1));
        } finally {
            readingThread.interrupt();
            readingThread.join(TimeUnit.SECONDS.toMillis(10));
        }
    }

    /** Answers a ranged GET with the first half of the range, then holds the connection open until the test ends. */
    private void sendPartOfTheRangeThenStall(HttpExchange exchange) throws IOException {
        rangeRequests.incrementAndGet();
        RequestedRange range = RequestedRange.of(exchange);
        StandInBlobServer.sendPartialContentHeaders(exchange, range, BLOB_SIZE);
        try (OutputStream body = exchange.getResponseBody()) {
            body.write(new byte[range.length() / 2]);
            body.flush();
            awaitTestFinished();
        }
    }

    private void awaitTestFinished() {
        try {
            testFinished.await();
        } catch (InterruptedException stopping) {
            Thread.currentThread().interrupt();
        }
    }
}
