/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.pmtiles;

/**
 * Reference implementation of Hilbert curve algorithms for testing.
 */
class HilbertCurve {

    /**
     * Convert (x,y) to d (Hilbert curve distance)
     */
    public static long xy2d(int n, int x, int y) {
        long d = 0;
        for (int s = n / 2; s > 0; s /= 2) {
            int rx = (x & s) > 0 ? 1 : 0;
            int ry = (y & s) > 0 ? 1 : 0;
            d += s * (long) s * ((3 * rx) ^ ry);
            rotate(n, s, rx, ry, x, y);
        }
        return d;
    }

    /**
     * Convert d (Hilbert curve distance) to (x,y)
     */
    public static int[] d2xy(int n, long d) {
        int x = 0, y = 0;
        for (int s = 1; s < n; s *= 2) {
            int rx = 1 & (int) (d / 2);
            int ry = 1 & (int) (d ^ rx);
            rotate2(s, rx, ry, x, y);
            x += s * rx;
            y += s * ry;
            d /= 4;
        }
        return new int[] {x, y};
    }

    private static void rotate(int n, int s, int rx, int ry, int x, int y) {
        // Note: This function modifies x and y in place in the calling code,
        // but in Java primitive parameters are passed by value.
        // This is why we need to take a different approach for our actual implementation.
        // This reference implementation is included for documentation purposes.
        if (ry == 0) {
            if (rx == 1) {
                x = n - 1 - x;
                y = n - 1 - y;
            }
            // Swap x and y
            int t = x;
            x = y;
            y = t;
        }
    }

    private static void rotate2(int s, int rx, int ry, int x, int y) {
        if (ry == 0) {
            if (rx == 1) {
                x = s - 1 - x;
                y = s - 1 - y;
            }
            // Swap x and y
            int t = x;
            x = y;
            y = t;
        }
    }
}
