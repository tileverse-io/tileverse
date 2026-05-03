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
package io.tileverse.storage.http;

import java.lang.reflect.Method;
import java.net.http.HttpClient;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.jspecify.annotations.NullMarked;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-process reference-counted cache of JDK {@link HttpClient} instances, keyed by HTTP-config tuple (connection
 * timeout, trust-all-certificates flag). Multiple {@link HttpStorage} instances sharing the same key share one
 * underlying client; the client is shut down when the last lease is released.
 *
 * <h2>Why one client serves many target servers</h2>
 *
 * <p>The JDK {@code HttpClient} maintains a separate connection pool per <em>origin</em> (scheme+host+port) internally,
 * and HTTP/2 multiplexes streams over a single connection per origin. Sharing one client across many target servers is
 * therefore safe: each origin still gets isolated connections within the shared pool. The cache only needs to fan out
 * on properties that can ONLY be set at {@code HttpClient} construction and that affect request semantics globally.
 *
 * <h2>What the cache key must include</h2>
 *
 * <p>Properties currently customised by {@link HttpStorageProvider} and therefore in the {@link Key}:
 *
 * <ul>
 *   <li><b>connect timeout</b> -- set on the builder; cannot be changed per-request.
 *   <li><b>SSL context</b> (the {@code trustAllCertificates} flag selects between the JDK default and a trust-all
 *       context) -- set on the builder; cannot be changed per-request.
 * </ul>
 *
 * <p>Properties NOT in the key today, with the rationale:
 *
 * <ul>
 *   <li><b>{@link HttpAuthentication}</b> -- applied per-request by {@code HttpStorage} via the
 *       {@link HttpAuthentication#authenticate} hook, so multiple Storages with different auth can share one client.
 *       (Cloud SDK clients are credential-bound at construction; their caches DO key on credentials.)
 *   <li><b>Executor</b>, <b>proxy selector</b>, <b>{@code Authenticator}</b>, <b>{@code CookieHandler}</b>, <b>HTTP
 *       version preference</b> -- none are configured today (we use the JDK defaults). If a future feature lets callers
 *       inject any of these via {@code StorageConfig}, that field MUST be added to {@link Key}; otherwise the cache
 *       would hand back a client with the wrong settings, or (in the cookie case) leak state across origins.
 * </ul>
 *
 * <h2>JVM-wide knobs (not per-client)</h2>
 *
 * <p>The pool's idle behaviour is governed by JVM-wide system properties -- {@code jdk.httpclient.connectionPoolSize}
 * (default 0, meaning unbounded) and {@code jdk.httpclient.keepalive.timeout}. These are not per-client and so are
 * unrelated to cache-key decisions; they just shape how aggressively a heavily-shared client recycles idle connections.
 */
@NullMarked
final class HttpClientCache {

    private static final Logger log = LoggerFactory.getLogger(HttpClientCache.class);

    static final HttpClientCache INSTANCE = new HttpClientCache();

    /**
     * Cache key. See the class-level javadoc for the full set of {@link HttpClient.Builder} properties that affect
     * request semantics globally and why each one is or isn't included here.
     */
    record Key(int connectTimeoutMillis, boolean trustAllCertificates) {}

    /** A reference-counted handle. Closing decrements the refcount; when zero, the {@link HttpClient} is shut down. */
    final class Lease implements AutoCloseable {
        private final Key key;
        private final Entry entry;
        private boolean closed;

        Lease(Key key, Entry entry) {
            this.key = key;
            this.entry = entry;
        }

        HttpClient client() {
            return entry.client;
        }

        @Override
        public synchronized void close() {
            if (closed) return;
            closed = true;
            release(key);
        }
    }

    private static final class Entry {
        final HttpClient client;
        int refCount;

        Entry(HttpClient client) {
            this.client = client;
            this.refCount = 0;
        }

        void shutdown() {
            // HttpClient implements AutoCloseable starting with Java 21 and gets shutdownNow() to immediately
            // discard ongoing requests. Reflective access keeps the source compatible with Java 17.
            try {
                Method shutdownNow = client.getClass().getMethod("shutdownNow");
                shutdownNow.invoke(client);
                return;
            } catch (Exception e) {
                log.warn("Error calling shutdownNow on HttpClient", e);
            }
            if (client instanceof AutoCloseable closeable) {
                try {
                    closeable.close();
                } catch (Exception e) {
                    log.warn("Error closing HttpClient", e);
                }
            }
        }
    }

    private final Map<Key, Entry> entries = new ConcurrentHashMap<>();

    int entryCount() {
        return entries.size();
    }

    Lease acquire(Key key) {
        Entry entry = entries.compute(key, (k, existing) -> {
            Entry e = existing == null ? new Entry(build(k)) : existing;
            e.refCount++;
            return e;
        });
        return new Lease(key, entry);
    }

    private synchronized void release(Key key) {
        entries.compute(key, (k, e) -> {
            if (e == null) return null;
            e.refCount--;
            if (e.refCount <= 0) {
                e.shutdown();
                return null;
            }
            return e;
        });
    }

    private static HttpClient build(Key key) {
        HttpClient.Builder builder =
                HttpClient.newBuilder().connectTimeout(Duration.ofMillis(key.connectTimeoutMillis()));
        if (key.trustAllCertificates()) {
            builder.sslContext(trustAllSslContext());
        }
        return builder.build();
    }

    /**
     * Builds an {@link SSLContext} that accepts any server certificate without chain validation. Only used when the
     * cache key has {@code trustAllCertificates = true} (development-only escape hatch for self-signed certs whose
     * issuing CA isn't in the JDK truststore).
     *
     * <p>Note: this disables the certificate-chain check only, not hostname verification. JDK {@link HttpClient}
     * applies endpoint identification ({@code SSLParameters.endpointIdentificationAlgorithm = "HTTPS"}) regardless, so
     * a self-signed cert whose subject doesn't match the request host still fails.
     */
    private static SSLContext trustAllSslContext() {
        TrustManager trustAll = new X509TrustManager() {
            @Override
            public void checkClientTrusted(X509Certificate[] chain, String authType) {
                // no-op: accept any client cert (we are the client; this method is unused here)
            }

            @Override
            public void checkServerTrusted(X509Certificate[] chain, String authType) {
                // no-op: accept any server cert without chain validation (the whole point of trust-all)
            }

            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
            }
        };
        try {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, new TrustManager[] {trustAll}, new SecureRandom());
            return sslContext;
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Failed to configure trust-all SSL context", e);
        }
    }
}
