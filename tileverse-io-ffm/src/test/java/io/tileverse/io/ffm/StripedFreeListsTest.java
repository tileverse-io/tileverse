/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.io.ffm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class StripedFreeListsTest {

    @Test
    void defaultCountFollowsTheCores() {
        assertThat(StripedFreeLists.defaultStripeCount(1)).isEqualTo(1);
        assertThat(StripedFreeLists.defaultStripeCount(4)).isEqualTo(4);
        assertThat(StripedFreeLists.defaultStripeCount(6)).isEqualTo(8);
        assertThat(StripedFreeLists.defaultStripeCount(32)).isEqualTo(32);
        assertThat(StripedFreeLists.defaultStripeCount(48)).isEqualTo(64);
        assertThat(StripedFreeLists.defaultStripeCount(256)).isEqualTo(64);
    }

    // A stripe's share of the small cap must hold the largest small buffers; a small cap gets fewer stripes.
    @Test
    void smallCapsGetFewerStripes() {
        long megabyte = 1L << 20;
        assertThat(StripedFreeLists.stripeCount(16, 512 * megabyte, 4 * megabyte))
                .isEqualTo(16);
        assertThat(StripedFreeLists.stripeCount(16, 16 * megabyte, 4 * megabyte))
                .isEqualTo(2);
        assertThat(StripedFreeLists.stripeCount(16, 4 * megabyte, 4 * megabyte)).isEqualTo(1);
        assertThat(StripedFreeLists.stripeCount(16, megabyte, 8192)).isEqualTo(16);
        assertThat(StripedFreeLists.stripeCount(16, 0, 8192)).isEqualTo(1);
    }

    // A thread masks its id to find its stripe, and a mask needs a power of two.
    @Test
    void rejectsNonPowerOfTwoCounts() {
        assertThatThrownBy(() -> new StripedFreeLists(1 << 20, 3))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("power of two");
        assertThatThrownBy(() -> new StripedFreeLists(1 << 20, 0)).isInstanceOf(IllegalArgumentException.class);
    }
}
