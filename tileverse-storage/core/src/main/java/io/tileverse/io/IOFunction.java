/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.io;

import java.io.IOException;

/** Throwing function for I/O operations */
@FunctionalInterface
public interface IOFunction<T, R> {

    R apply(T t) throws IOException;
}
