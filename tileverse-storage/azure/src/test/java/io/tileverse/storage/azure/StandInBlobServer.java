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

import com.azure.core.http.HttpClient;
import com.azure.core.http.jdk.httpclient.JdkHttpClientBuilder;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobClientBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.tileverse.storage.Storage;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A local HTTP server standing in for a blob endpoint, for driving the Azure SDK clients through the real pipeline.
 * Each test provides the handler answering the requests; {@link RequestedRange#of} and
 * {@link #sendPartialContentHeaders} parse a ranged GET and write the response headers expected by the SDK.
 */
final class StandInBlobServer {

    private static final String ACCOUNT = "devstoreaccount1";

    private final ExecutorService handlerThreads;
    private final HttpServer server;

    private StandInBlobServer(ExecutorService handlerThreads, HttpServer server) {
        this.handlerThreads = handlerThreads;
        this.server = server;
    }

    /** Starts a server on the loopback address, running {@code handler} for each request on a thread of its own. */
    static StandInBlobServer start(HttpHandler handler) throws IOException {
        ExecutorService handlerThreads = Executors.newCachedThreadPool();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(handlerThreads);
        server.createContext("/", handler);
        server.start();
        return new StandInBlobServer(handlerThreads, server);
    }

    /** Builds a client for a blob on this server, sending its requests through {@code httpClient}. */
    BlobClient blobClient(HttpClient httpClient) {
        return new BlobClientBuilder()
                .endpoint(accountEndpoint())
                .containerName("container")
                .blobName("blob.bin")
                .httpClient(httpClient)
                .buildClient();
    }

    /** Builds a Blob service client for the account on this server, sending its requests through {@code httpClient}. */
    BlobServiceClient serviceClient(HttpClient httpClient) {
        return new BlobServiceClientBuilder()
                .endpoint(accountEndpoint())
                .httpClient(httpClient)
                .buildClient();
    }

    /**
     * Builds a Data Lake service client for the account on this server, sending its requests through
     * {@code httpClient}. The builder refuses an anonymous client; this server ignores the signature of the made-up
     * key.
     */
    DataLakeServiceClient dataLakeServiceClient(HttpClient httpClient) {
        String madeUpKey = Base64.getEncoder().encodeToString("stand-in".getBytes(StandardCharsets.US_ASCII));
        StorageSharedKeyCredential credential = new StorageSharedKeyCredential(ACCOUNT, madeUpKey);
        return new DataLakeServiceClientBuilder()
                .endpoint(accountEndpoint())
                .credential(credential)
                .httpClient(httpClient)
                .buildClient();
    }

    /** Opens an Azure Blob storage on the container of this server. */
    Storage blobStorage() {
        return blobStorageAt("");
    }

    /** Opens an Azure Blob storage rooted at {@code prefix} in the container of this server. */
    Storage blobStorageAt(String prefix) {
        BlobServiceClient serviceClient = serviceClient(new JdkHttpClientBuilder().build());
        URI baseUri = URI.create(containerUri() + prefix);
        return new AzureBlobStorage(baseUri, AzureBlobLocation.parse(baseUri), new BorrowedAzureHandle(serviceClient));
    }

    /** Opens an Azure Data Lake storage on the container of this server. */
    Storage dataLakeStorage() {
        return dataLakeStorageAt("");
    }

    /** Opens an Azure Data Lake storage rooted at {@code prefix} in the container of this server. */
    Storage dataLakeStorageAt(String prefix) {
        HttpClient httpClient = new JdkHttpClientBuilder().build();
        BlobServiceClient blobClient = serviceClient(httpClient);
        DataLakeServiceClient dfsClient = dataLakeServiceClient(httpClient);
        URI baseUri = URI.create(containerUri() + prefix);
        BorrowedAzureHandle handle = new BorrowedAzureHandle(blobClient, dfsClient);
        return new AzureDataLakeStorage(baseUri, AzureBlobLocation.parse(baseUri), handle);
    }

    /** The URI of the container holding the blob of {@link #blobClient}, as a Storage base URI. */
    URI containerUri() {
        return URI.create(accountEndpoint() + "/container/");
    }

    private String accountEndpoint() {
        return "http://127.0.0.1:" + server.getAddress().getPort() + "/" + ACCOUNT;
    }

    /** Sends a 206 status and the blob headers for {@code range} of a blob of {@code blobSize} bytes. */
    static void sendPartialContentHeaders(HttpExchange exchange, RequestedRange range, int blobSize)
            throws IOException {
        Headers headers = exchange.getResponseHeaders();
        headers.set("Content-Type", "application/octet-stream");
        headers.set("Content-Range", "bytes " + range.offset() + "-" + range.lastByte() + "/" + blobSize);
        headers.set("Accept-Ranges", "bytes");
        headers.set("ETag", "\"0x1\"");
        headers.set("Last-Modified", "Tue, 08 Sep 2026 00:00:00 GMT");
        headers.set("x-ms-blob-type", "BlockBlob");
        headers.set("x-ms-version", "2025-01-05");
        headers.set("x-ms-request-id", "00000000-0000-0000-0000-000000000000");
        exchange.sendResponseHeaders(206, range.length());
    }

    /** Leaves a request unanswered until {@code released} opens or the server stops, then closes the exchange. */
    static void holdUnanswered(HttpExchange exchange, CountDownLatch released) {
        try {
            released.await();
        } catch (InterruptedException stopping) {
            Thread.currentThread().interrupt();
        }
        exchange.close();
    }

    /** Stops the server, closing its connections, and waits for the handler threads to end. */
    void stop() throws InterruptedException {
        server.stop(0);
        handlerThreads.shutdownNow();
        assertThat(handlerThreads.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }

    /** The first byte and the byte count of a ranged GET, read from {@code x-ms-range} or {@code Range}. */
    record RequestedRange(int offset, int length) {

        static RequestedRange of(HttpExchange exchange) {
            Headers requestHeaders = exchange.getRequestHeaders();
            String range = requestHeaders.getFirst("x-ms-range");
            if (range == null) {
                range = requestHeaders.getFirst("Range");
            }
            String[] bounds = range.substring("bytes=".length()).split("-");
            int offset = Integer.parseInt(bounds[0]);
            int lastByte = Integer.parseInt(bounds[1]);
            return new RequestedRange(offset, lastByte - offset + 1);
        }

        int lastByte() {
            return offset + length - 1;
        }
    }
}
