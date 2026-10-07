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
