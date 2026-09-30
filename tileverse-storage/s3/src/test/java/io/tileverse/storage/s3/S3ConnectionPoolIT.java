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
package io.tileverse.storage.s3;

import static io.tileverse.storage.RangeReaderTestSupport.counts;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.RangeRequest;
import io.tileverse.storage.StorageException;
import io.tileverse.storage.batch.BatchSettings;
import io.tileverse.storage.batch.CoalescingPolicy;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Requests beyond the pooled connections of a host, against a server holding every response for a while: they wait
 * their turn for as long as the acquisition timeout allows, on the async client of batches and on the sync client of
 * single reads alike.
 */
class S3ConnectionPoolIT {

    private static final long OBJECT_SIZE = 64L * 1024 * 1024;
    private static final int LENGTH = 1024;
    private static final long STRIDE = 1024L * 1024;
    private static final int POOLED_CONNECTIONS = 2;
    private static final Duration CONNECTION_TIMEOUT = Duration.ofSeconds(2);
    private static final Duration PATIENT = Duration.ofSeconds(30);

    /** Four attempts by the SDK, each waiting this long, end well within {@link #HELD_PAST_EVERY_ATTEMPT}. */
    private static final Duration IMPATIENT = Duration.ofMillis(100);

    private static final Duration HELD_BRIEFLY = Duration.ofMillis(300);
    private static final Duration HELD_PAST_EVERY_ATTEMPT = Duration.ofSeconds(2);

    private final AtomicInteger requestsHeld = new AtomicInteger();
    private final AtomicInteger mostRequestsHeldAtOnce = new AtomicInteger();

    private HttpServer server;
    private ExecutorService serverThreads;
    private Duration hold = HELD_BRIEFLY;
    private S3ClientCache.Lease lease;
    private S3Storage storage;

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 64);
        serverThreads = Executors.newFixedThreadPool(16);
        server.setExecutor(serverThreads);
        server.createContext("/", this::serveHoldingTheResponse);
        server.start();
    }

    @AfterEach
    void stopServer() throws IOException {
        if (storage != null) {
            storage.close();
        }
        server.stop(0);
        serverThreads.shutdownNow();
    }

    private void serveHoldingTheResponse(HttpExchange exchange) throws IOException {
        try (exchange) {
            exchange.getResponseHeaders().add("ETag", "\"held\"");
            if ("HEAD".equals(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().add("Content-Length", Long.toString(OBJECT_SIZE));
                exchange.sendResponseHeaders(200, -1);
                return;
            }
            String range = exchange.getRequestHeaders().getFirst("Range");
            holdCountingTheRequest();
            byte[] body = new byte[LENGTH];
            exchange.getResponseHeaders().add("Content-Range", range.replace("=", " ") + "/" + OBJECT_SIZE);
            exchange.sendResponseHeaders(206, body.length);
            exchange.getResponseBody().write(body);
        }
    }

    @SuppressWarnings("java:S2925") // the pause is the behavior of the stand-in server; no assertion waits on it
    private void holdCountingTheRequest() {
        mostRequestsHeldAtOnce.accumulateAndGet(requestsHeld.incrementAndGet(), Math::max);
        try {
            Thread.sleep(hold.toMillis());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } finally {
            requestsHeld.decrementAndGet();
        }
    }

    private RangeReader readerWaitingForAConnection(Duration acquisitionTimeout) {
        S3HttpClientSettings settings =
                new S3HttpClientSettings(POOLED_CONNECTIONS, CONNECTION_TIMEOUT, acquisitionTimeout);
        S3SharedHttpClient sharedHttpClient = new S3SharedHttpClient(settings::newAsyncHttpClient);
        S3ClientCache cache = new S3ClientCache(sharedHttpClient, settings::syncHttpClientBuilder);
        URI endpoint = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
        lease = cache.acquire(S3ClientCache.key("us-east-1", endpoint, true, null, null, null, true));
        URI baseUri = URI.create("s3://bucket/");
        BatchSettings noBoundInFlight = new BatchSettings(-1, CoalescingPolicy.DEFAULT_MAX_FETCH_BYTES, 0);
        storage = new S3Storage(baseUri, S3StorageBucketKey.parse(baseUri), lease, false, noBoundInFlight);
        return storage.openRangeReader("object.bin");
    }

    private static List<RangeRequest> ranges(int count) {
        List<RangeRequest> requests = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            requests.add(RangeRequest.of(i * STRIDE, LENGTH, ByteBuffer.allocate(LENGTH)));
        }
        return requests;
    }

    @Test
    void aBatchWiderThanThePoolWaitsForItsConnections() {
        RangeReader reader = readerWaitingForAConnection(PATIENT);

        int[] read = counts(reader.readRanges(ranges(3 * POOLED_CONNECTIONS)));

        assertThat(read).hasSize(3 * POOLED_CONNECTIONS).containsOnly(LENGTH);
        assertThat(mostRequestsHeldAtOnce).hasValue(POOLED_CONNECTIONS);
    }

    @Test
    void aBatchFailsOnceItsWaitForAConnectionRunsOut() {
        hold = HELD_PAST_EVERY_ATTEMPT;
        RangeReader reader = readerWaitingForAConnection(IMPATIENT);
        List<RangeRequest> widerThanThePool = ranges(POOLED_CONNECTIONS + 1);

        assertThatThrownBy(() -> reader.readRanges(widerThanThePool))
                .isInstanceOf(StorageException.class)
                .hasMessageContaining("failed to acquire a connection");
    }

    @Test
    void singleReadsBeyondThePoolWaitForTheirConnections() throws Exception {
        RangeReader reader = readerWaitingForAConnection(PATIENT);

        List<Integer> read = readAtOnce(reader, 3 * POOLED_CONNECTIONS);

        assertThat(read).hasSize(3 * POOLED_CONNECTIONS).containsOnly(LENGTH);
        assertThat(mostRequestsHeldAtOnce).hasValue(POOLED_CONNECTIONS);
    }

    @Test
    void aSingleReadFailsOnceItsWaitForAConnectionRunsOut() {
        hold = HELD_PAST_EVERY_ATTEMPT;
        RangeReader reader = readerWaitingForAConnection(IMPATIENT);

        assertThatThrownBy(() -> readAtOnce(reader, POOLED_CONNECTIONS + 1))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(StorageException.class)
                .hasMessageContaining("failed to acquire a connection");
    }

    private static List<Integer> readAtOnce(RangeReader reader, int readers) throws Exception {
        ExecutorService threads = Executors.newFixedThreadPool(readers);
        try {
            List<Future<Integer>> reads = new ArrayList<>();
            for (int i = 0; i < readers; i++) {
                long offset = i * STRIDE;
                reads.add(threads.submit(() -> reader.readRange(offset, LENGTH, ByteBuffer.allocate(LENGTH))));
            }
            List<Integer> counts = new ArrayList<>();
            for (Future<Integer> read : reads) {
                counts.add(read.get());
            }
            return counts;
        } finally {
            threads.shutdownNow();
        }
    }
}
