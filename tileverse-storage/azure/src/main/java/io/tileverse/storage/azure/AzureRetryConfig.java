/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.azure;

import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import java.time.Duration;
import java.util.Objects;

/**
 * Scalar subset of {@link RequestRetryOptions} that the SPI exposes via {@code storage.azure.*-retries-*} parameters.
 * Not in the public API; the typed {@code RequestRetryOptions} object is reachable only via the SDK-injection path on
 * {@link AzureBlobStorageProvider#open}.
 */
record AzureRetryConfig(int maxTries, Duration retryDelay, Duration maxRetryDelay, Duration tryTimeout) {

    AzureRetryConfig {
        Objects.requireNonNull(retryDelay, "retryDelay");
        Objects.requireNonNull(maxRetryDelay, "maxRetryDelay");
        Objects.requireNonNull(tryTimeout, "tryTimeout");
        if (maxTries < 1) {
            throw new IllegalArgumentException("maxTries must be >= 1: " + maxTries);
        }
    }

    RequestRetryOptions toSdkOptions() {
        return new RequestRetryOptions(
                RetryPolicyType.EXPONENTIAL,
                maxTries,
                Math.toIntExact(tryTimeout.toSeconds()),
                retryDelay.toMillis(),
                maxRetryDelay.toMillis(),
                null /* secondaryHost */);
    }
}
