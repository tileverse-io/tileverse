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

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedFileUpload;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

/**
 * A multipart upload against a server holding each part for a while: it keeps at most an eighth of the pool of its host
 * in flight.
 */
class S3UploadPartsInFlightIT {

    private static final int POOLED_CONNECTIONS = 16;

    /** Enough threads to hold the four parts of the upload at once. */
    private static final int SERVER_THREADS = 16;

    private static final Duration CONNECTION_TIMEOUT = Duration.ofSeconds(2);
    private static final Duration PATIENT = Duration.ofSeconds(30);
    private static final Duration HELD = Duration.ofMillis(300);

    /** The default part size and threshold of the SDK. */
    private static final int PART_SIZE = 8 * 1024 * 1024;

    /** Three full parts and a fourth of one byte. */
    private static final int FILE_SIZE = 3 * PART_SIZE + 1;

    private static final String INITIATED = "<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key>"
            + "<UploadId>u</UploadId></InitiateMultipartUploadResult>";
    private static final String COMPLETED = "<CompleteMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key>"
            + "<ETag>\"done\"</ETag></CompleteMultipartUploadResult>";

    private final AtomicInteger partsReceived = new AtomicInteger();
    private final AtomicInteger partsHeld = new AtomicInteger();
    private final AtomicInteger mostPartsHeldAtOnce = new AtomicInteger();
    private final List<String> unexpectedRequests = new CopyOnWriteArrayList<>();

    private HttpServer server;
    private ExecutorService serverThreads;
    private S3ClientCache.Lease lease;

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 64);
        serverThreads = Executors.newFixedThreadPool(SERVER_THREADS);
        server.setExecutor(serverThreads);
        server.createContext("/", this::serveAMultipartUpload);
        server.start();
    }

    @AfterEach
    void stopServer() {
        if (lease != null) {
            lease.close();
        }
        server.stop(0);
        serverThreads.shutdownNow();
    }

    private void serveAMultipartUpload(HttpExchange exchange) throws IOException {
        try (exchange) {
            String method = exchange.getRequestMethod();
            String query = Objects.requireNonNullElse(exchange.getRequestURI().getRawQuery(), "");
            if ("POST".equals(method) && hasParameter(query, "uploads")) {
                answerWithXml(exchange, INITIATED);
            } else if ("PUT".equals(method) && hasParameter(query, "partNumber")) {
                receivePart(exchange);
            } else if ("POST".equals(method) && hasParameter(query, "uploadId")) {
                answerWithXml(exchange, COMPLETED);
            } else {
                unexpectedRequests.add(method + " " + exchange.getRequestURI());
                answerWithoutBody(exchange, 400);
            }
        }
    }

    private static boolean hasParameter(String query, String name) {
        return Arrays.stream(query.split("&")).anyMatch(parameter -> parameter.split("=")[0].equals(name));
    }

    private void receivePart(HttpExchange exchange) throws IOException {
        partsReceived.incrementAndGet();
        holdCountingThePart();
        exchange.getResponseHeaders().add("ETag", "\"part\"");
        answerWithoutBody(exchange, 200);
    }

    @SuppressWarnings("java:S2925") // the pause is the behavior of the stand-in server; no assertion waits on it
    private void holdCountingThePart() {
        mostPartsHeldAtOnce.accumulateAndGet(partsHeld.incrementAndGet(), Math::max);
        try {
            Thread.sleep(HELD.toMillis());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } finally {
            partsHeld.decrementAndGet();
        }
    }

    private static void answerWithXml(HttpExchange exchange, String xml) throws IOException {
        drainTheRequestBody(exchange);
        byte[] body = xml.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/xml");
        exchange.sendResponseHeaders(200, body.length);
        exchange.getResponseBody().write(body);
    }

    private static void answerWithoutBody(HttpExchange exchange, int status) throws IOException {
        drainTheRequestBody(exchange);
        exchange.sendResponseHeaders(status, -1);
    }

    private static void drainTheRequestBody(HttpExchange exchange) throws IOException {
        exchange.getRequestBody().transferTo(OutputStream.nullOutputStream());
    }

    private S3TransferManager transferManagerOfTheServer() {
        S3HttpClientSettings settings = new S3HttpClientSettings(POOLED_CONNECTIONS, CONNECTION_TIMEOUT, PATIENT);
        S3SharedHttpClient sharedHttpClient = new S3SharedHttpClient(() -> settings);
        S3ClientCache cache = new S3ClientCache(sharedHttpClient);
        URI endpoint = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
        lease = cache.acquire(S3ClientCache.key("us-east-1", endpoint, true, null, null, null, true));
        return lease.transferManager();
    }

    @Test
    void anUploadHoldsAtMostAnEighthOfThePool(@TempDir Path dir) throws Exception {
        Path source = dir.resolve("source.bin");
        Files.write(source, new byte[FILE_SIZE]);
        UploadFileRequest request = UploadFileRequest.builder()
                .source(source)
                .putObjectRequest(put -> put.bucket("bucket").key("key"))
                .build();
        S3TransferManager transferManager = transferManagerOfTheServer();

        FileUpload upload = transferManager.uploadFile(request);

        CompletedFileUpload completed = upload.completionFuture().get(30, TimeUnit.SECONDS);
        assertThat(completed.response().eTag()).isEqualTo("\"done\"");
        assertThat(partsReceived).hasValue(4);
        assertThat(mostPartsHeldAtOnce).as("an eighth of the pool").hasValue(2);
        assertThat(unexpectedRequests).isEmpty();
    }
}
