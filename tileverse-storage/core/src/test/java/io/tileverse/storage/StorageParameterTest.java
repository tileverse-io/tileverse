/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class StorageParameterTest {

    @Test
    void groupAndSubgroupRoundTripIndependently() {
        StorageParameter<String> p = StorageParameter.builder()
                .key("x")
                .title("test")
                .description("test")
                .type(String.class)
                .group("auth")
                .subgroup("oauth")
                .build();

        assertThat(p.group()).isEqualTo("auth");
        assertThat(p.subgroup()).contains("oauth");
    }
}
