/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class HttpClientCacheTest {

    @Test
    void sameKeyReturnsSameClient() {
        HttpClientCache cache = new HttpClientCache();
        HttpClientCache.Key key = new HttpClientCache.Key(5_000, false);
        try (HttpClientCache.Lease a = cache.acquire(key);
                HttpClientCache.Lease b = cache.acquire(key)) {
            assertThat(a.client()).isSameAs(b.client());
        }
    }

    @Test
    void differentKeysReturnDifferentClients() {
        HttpClientCache cache = new HttpClientCache();
        HttpClientCache.Key fast = new HttpClientCache.Key(1_000, false);
        HttpClientCache.Key slow = new HttpClientCache.Key(30_000, false);
        try (HttpClientCache.Lease a = cache.acquire(fast);
                HttpClientCache.Lease b = cache.acquire(slow)) {
            assertThat(a.client()).isNotSameAs(b.client());
        }
    }

    @Test
    void trustAllFlagAffectsCacheKey() {
        HttpClientCache cache = new HttpClientCache();
        HttpClientCache.Key strict = new HttpClientCache.Key(5_000, false);
        HttpClientCache.Key trustAll = new HttpClientCache.Key(5_000, true);
        try (HttpClientCache.Lease a = cache.acquire(strict);
                HttpClientCache.Lease b = cache.acquire(trustAll)) {
            assertThat(a.client()).isNotSameAs(b.client());
        }
    }

    @Test
    void clientIsClosedWhenLastLeaseReleased() {
        HttpClientCache cache = new HttpClientCache();
        HttpClientCache.Key key = new HttpClientCache.Key(5_000, false);
        HttpClientCache.Lease a = cache.acquire(key);
        HttpClientCache.Lease b = cache.acquire(key);
        a.close();
        assertThat(cache.entryCount()).isEqualTo(1);
        b.close();
        assertThat(cache.entryCount()).isZero();
    }

    // HttpClient has no shutdown method before Java 21
    @Test
    void lastReleaseDropsOngoingRequests() throws IOException {
        assumeTrue(Runtime.version().feature() >= 21);
        try (ServerSocket silentServer = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
            silentServer.setSoTimeout(5_000);
            HttpClientCache cache = new HttpClientCache();
            HttpClientCache.Lease lease = cache.acquire(new HttpClientCache.Key(5_000, false));
            CompletableFuture<HttpResponse<Void>> response = sendRequest(lease.client(), silentServer);
            try (Socket unanswered = silentServer.accept()) {
                assertTimeoutPreemptively(Duration.ofSeconds(5), lease::close);
                assertThat(response).failsWithin(Duration.ofSeconds(5));
            }
        }
    }

    private static CompletableFuture<HttpResponse<Void>> sendRequest(HttpClient client, ServerSocket server) {
        URI uri = URI.create("http://127.0.0.1:" + server.getLocalPort() + "/");
        HttpRequest request = HttpRequest.newBuilder(uri).build();
        return client.sendAsync(request, BodyHandlers.discarding());
    }

    @Test
    void closingTheSameLeaseTwiceIsHarmless() {
        HttpClientCache cache = new HttpClientCache();
        HttpClientCache.Key key = new HttpClientCache.Key(5_000, false);
        HttpClientCache.Lease lease = cache.acquire(key);
        lease.close();
        lease.close();
        assertThat(cache.entryCount()).isZero();
    }
}
