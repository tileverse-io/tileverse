/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.cache;

import static java.util.Objects.requireNonNull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.jspecify.annotations.NullMarked;

/**
 * The value stored in a claim when its loader throws.
 *
 * <p>A claim never completes exceptionally. Caffeine logs a warning for a future completed exceptionally, and
 * {@link CompletableFuture#join()} wraps its throwable in a {@link CompletionException}. The loading caller completes
 * the claim with a failed load instead, and the waiters rethrow the same throwable.
 *
 * <p>The loading caller removes the claim from the pool before completing it. The pool weighs a failed load zero and
 * ignores its removal, and the partitions treat it as absent.
 *
 * @param failure the throwable of the loader
 */
@NullMarked
record FailedLoad(Throwable failure) {

    FailedLoad {
        requireNonNull(failure, "failure");
    }

    /**
     * Throws the failure. A runtime exception or an error is thrown as it is. A checked throwable can only come from a
     * sneaky throw, and is wrapped in an {@link IllegalStateException}.
     */
    void rethrow() {
        if (failure instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        throw new IllegalStateException(failure);
    }
}
