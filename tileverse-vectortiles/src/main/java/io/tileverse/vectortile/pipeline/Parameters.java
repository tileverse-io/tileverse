/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.pipeline;

/** Checks of the numbers given to the classes of this package, such as a scale, an extent or a buffer. */
final class Parameters {

    private Parameters() {}

    /**
     * Returns the value if it is positive and finite.
     *
     * @param value the number to check
     * @param name the name of the number in the message of the exception, such as {@code "scale"}
     * @return the value
     * @throws IllegalArgumentException if the value is 0 or below, infinite, or NaN
     */
    static double requirePositiveAndFinite(double value, String name) {
        if (value > 0 && Double.isFinite(value)) {
            return value;
        }
        throw new IllegalArgumentException("The " + name + " must be positive and finite: " + value);
    }

    /**
     * Returns the value if it is 0 or above, and finite.
     *
     * @param value the number to check
     * @param name the name of the number in the message of the exception, such as {@code "buffer"}
     * @return the value
     * @throws IllegalArgumentException if the value is negative, infinite, or NaN
     */
    static double requireNotNegativeAndFinite(double value, String name) {
        if (value >= 0 && Double.isFinite(value)) {
            return value;
        }
        throw new IllegalArgumentException("The " + name + " must be at least 0 and finite: " + value);
    }
}
