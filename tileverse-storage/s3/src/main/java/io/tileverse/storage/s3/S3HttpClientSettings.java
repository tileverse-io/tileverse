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

import java.time.Duration;
import java.util.Objects;
import org.jspecify.annotations.NullMarked;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.http.crt.AwsCrtHttpClient;

/**
 * Connection settings of the HTTP clients behind the S3 clients built by {@link S3ClientCache}: the HTTP client shared
 * by every async client of the process, and the HTTP client of each sync client. Each HTTP client keeps one connection
 * pool per host.
 *
 * <p>The settings belong to the process, read through {@link ProcessSettings}: a pool shared by every Storage has no
 * Storage to take its parameters from.
 *
 * @param maxConcurrency connections pooled per host by each HTTP client
 * @param connectionTimeout longest wait for a connection to open
 * @param connectionAcquisitionTimeout longest wait of a request for a pooled connection; the request fails afterwards
 */
@NullMarked
record S3HttpClientSettings(int maxConcurrency, Duration connectionTimeout, Duration connectionAcquisitionTimeout) {

    static final String MAX_CONCURRENCY = "io.tileverse.storage.s3-http-client.max-concurrency";
    static final String CONNECTION_TIMEOUT = "io.tileverse.storage.s3-http-client.connection-timeout";
    static final String CONNECTION_ACQUISITION_TIMEOUT =
            "io.tileverse.storage.s3-http-client.connection-acquisition-timeout";

    static final int DEFAULT_MAX_CONCURRENCY = 50;
    static final Duration DEFAULT_CONNECTION_TIMEOUT = Duration.ofSeconds(2);

    /**
     * Three times the wait of the SDK, for a burst of requests wider than the pool to get its turn on a slow link, and
     * short enough to fail a request before its own caller gives up.
     */
    static final Duration DEFAULT_CONNECTION_ACQUISITION_TIMEOUT = Duration.ofSeconds(30);

    S3HttpClientSettings {
        Objects.requireNonNull(connectionTimeout, "connectionTimeout");
        Objects.requireNonNull(connectionAcquisitionTimeout, "connectionAcquisitionTimeout");
        if (maxConcurrency <= 0) {
            throw new IllegalArgumentException("maxConcurrency must be positive: " + maxConcurrency);
        }
        requirePositive(connectionTimeout, "connectionTimeout");
        requirePositive(connectionAcquisitionTimeout, "connectionAcquisitionTimeout");
    }

    private static void requirePositive(Duration duration, String name) {
        if (duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive: " + duration);
        }
    }

    static S3HttpClientSettings ofProcess() {
        return resolve(ProcessSettings.ofProcess());
    }

    static S3HttpClientSettings resolve(ProcessSettings process) {
        int maxConcurrency = process.positiveInt(MAX_CONCURRENCY, DEFAULT_MAX_CONCURRENCY);
        Duration connectionTimeout = process.positiveDuration(CONNECTION_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT);
        Duration connectionAcquisitionTimeout =
                process.positiveDuration(CONNECTION_ACQUISITION_TIMEOUT, DEFAULT_CONNECTION_ACQUISITION_TIMEOUT);
        return new S3HttpClientSettings(maxConcurrency, connectionTimeout, connectionAcquisitionTimeout);
    }

    /** The caller owns the client and closes it. */
    SdkAsyncHttpClient newAsyncHttpClient() {
        return AwsCrtAsyncHttpClient.builder()
                .maxConcurrency(maxConcurrency)
                .connectionTimeout(connectionTimeout)
                .connectionAcquisitionTimeout(connectionAcquisitionTimeout)
                .build();
    }

    /** For a sync S3 client to build its HTTP client from: the S3 client then owns it and closes it. */
    AwsCrtHttpClient.Builder syncHttpClientBuilder() {
        return AwsCrtHttpClient.builder()
                .maxConcurrency(maxConcurrency)
                .connectionTimeout(connectionTimeout)
                .connectionAcquisitionTimeout(connectionAcquisitionTimeout);
    }
}
