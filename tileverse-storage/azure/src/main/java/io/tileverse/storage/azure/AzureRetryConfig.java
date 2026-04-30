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
