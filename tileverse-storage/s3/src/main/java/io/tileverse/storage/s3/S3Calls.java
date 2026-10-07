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

import io.tileverse.storage.StorageException;
import java.io.InterruptedIOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.jspecify.annotations.NullMarked;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Makes calls of the async S3 client for a caller blocked on the outcome, and maps failures onto the storage exception
 * hierarchy.
 *
 * <p>An interrupt of the waiting thread cancels the request and fails the call with a {@link StorageException} caused
 * by an {@link InterruptedIOException}, the interrupt flag left set, as the local file reader does. Cancelling is best
 * effort: the SDK may still deliver the response, and a caller writing into its own buffer fences that writer off.
 */
@NullMarked
final class S3Calls {

    private S3Calls() {}

    /**
     * Makes a call and waits for its outcome.
     *
     * @param key the key concerned by the call, named by its failures
     * @param call issues the request; never invoked by a thread already interrupted
     * @return the value of the completed call
     * @throws StorageException the mapped failure, or one caused by an {@link InterruptedIOException} on interrupt
     */
    static <T> T await(String key, Supplier<CompletableFuture<T>> call) {
        return await(key, call, late -> {});
    }

    /**
     * Makes a call and waits for its outcome, handing {@code discardLate} a value completing despite the cancel of an
     * interrupted wait: a response stream holds its connection until closed.
     */
    static <T> T await(String key, Supplier<CompletableFuture<T>> call, Consumer<? super T> discardLate) {
        failIfInterrupted(key);
        CompletableFuture<T> pending = start(key, call);
        try {
            return pending.get();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            cancel(pending, discardLate);
            throw interruptedWaitingFor(key);
        } catch (ExecutionException | CancellationException failure) {
            throw map(failure, key);
        }
    }

    /**
     * Maps a failure of the async client onto the storage exception hierarchy, past the completion wrappers.
     *
     * @throws Error the failure itself, when it is one
     */
    static StorageException map(Throwable failure, String key) {
        Throwable cause = unwrap(failure);
        if (cause instanceof Error error) {
            throw error;
        }
        if (cause instanceof StorageException storageFailure) {
            return storageFailure;
        }
        if (cause instanceof SdkException && cause.getCause() instanceof StorageException raisedInTransformer) {
            return raisedInTransformer;
        }
        if (cause instanceof S3Exception serviceFailure) {
            return S3ExceptionMapper.map(serviceFailure, key);
        }
        return new StorageException("S3 request failed for key '" + key + "': " + cause.getMessage(), cause);
    }

    private static void failIfInterrupted(String key) {
        if (Thread.currentThread().isInterrupted()) {
            throw interruptedWaitingFor(key);
        }
    }

    private static <T> CompletableFuture<T> start(String key, Supplier<CompletableFuture<T>> call) {
        try {
            return call.get();
        } catch (RuntimeException failedToStart) {
            throw map(failedToStart, key);
        }
    }

    private static <T> void cancel(CompletableFuture<T> pending, Consumer<? super T> discardLate) {
        boolean cancelled = pending.cancel(true);
        boolean completedAnyway = !cancelled && pending.isDone() && !pending.isCompletedExceptionally();
        if (completedAnyway) {
            discardLate.accept(pending.join());
        }
    }

    private static StorageException interruptedWaitingFor(String key) {
        return new StorageException("Interrupted waiting for S3 for key '" + key + "'", new InterruptedIOException());
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable cause = failure;
        while ((cause instanceof CompletionException || cause instanceof ExecutionException)
                && cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }
}
