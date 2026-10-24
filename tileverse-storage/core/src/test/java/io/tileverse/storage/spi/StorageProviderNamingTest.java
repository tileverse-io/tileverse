/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.spi;

import static org.assertj.core.api.Assertions.assertThat;

import io.tileverse.storage.StorageConfig;
import io.tileverse.storage.file.FileStorageProvider;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class StorageProviderNamingTest {

    private final StorageProvider provider = new FileStorageProvider();

    @Test
    void aConfigWithNoProviderIdNamesNoProvider() {
        StorageConfig config = new StorageConfig("file:///data/");

        assertThat(provider.isNamedBy(config)).isFalse();
    }

    @ParameterizedTest
    @MethodSource("spellingsOfTheProviderId")
    void aConfigNamesTheProviderByItsIdIgnoringCase(String providerId) {
        StorageConfig config = new StorageConfig("file:///data/").providerId(providerId);

        assertThat(provider.isNamedBy(config)).isTrue();
        assertThat(provider.canProcess(config)).isTrue();
    }

    static Stream<String> spellingsOfTheProviderId() {
        return Stream.of("file", "FILE", "File");
    }

    @Test
    void aConfigNamingAnotherProviderIsNotServed() {
        StorageConfig config = new StorageConfig("file:///data/").providerId("http");

        assertThat(provider.isNamedBy(config)).isFalse();
        assertThat(provider.canProcess(config)).isFalse();
    }
}
