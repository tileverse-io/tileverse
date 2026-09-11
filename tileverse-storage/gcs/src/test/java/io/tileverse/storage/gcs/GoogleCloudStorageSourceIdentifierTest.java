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
package io.tileverse.storage.gcs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.StorageOptions;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import java.io.IOException;
import java.net.URI;
import org.junit.jupiter.api.Test;

/**
 * The source identifier of a GCS reader partitions the shared range cache, hence it has to differ whenever the bytes
 * could differ: the same bucket and object on two hosts (an emulator and the public API, or two emulators) are two
 * different objects. The client reports its host through its options, which are real objects here; the mocked client
 * itself receives no request because opening a reader performs no I/O.
 */
class GoogleCloudStorageSourceIdentifierTest {

    private static final URI BASE_URI = URI.create("gs://bucket/prefix/");
    private static final String KEY = "tiles/file.pmtiles";

    @Test
    void defaultHostRendersTheCanonicalGsUri() throws IOException {
        try (Storage storage = open(null)) {
            assertThat(identifierOf(storage)).isEqualTo("gs://bucket/prefix/" + KEY);
        }
    }

    @Test
    void hostOverrideIsPartOfTheIdentifier() throws IOException {
        try (Storage storage = open("http://localhost:4443")) {
            assertThat(identifierOf(storage)).isEqualTo("http://localhost:4443/bucket/prefix/" + KEY);
        }
    }

    @Test
    void sameObjectOnDifferentHostsGetsDifferentIdentifiers() throws IOException {
        try (Storage first = open("http://emulator-a:4443");
                Storage second = open("http://emulator-b:4443")) {
            assertThat(identifierOf(first)).isNotEqualTo(identifierOf(second));
        }
    }

    @Test
    void sameObjectOnTheSameHostGetsTheSameIdentifier() throws IOException {
        try (Storage first = open("http://localhost:4443");
                Storage second = open("http://localhost:4443")) {
            assertThat(identifierOf(first)).isEqualTo(identifierOf(second));
        }
    }

    @Test
    void trailingSlashOnTheHostDoesNotSplitTheIdentifier() throws IOException {
        try (Storage bare = open("http://localhost:4443");
                Storage slashed = open("http://localhost:4443/")) {
            assertThat(identifierOf(slashed)).isEqualTo(identifierOf(bare));
        }
    }

    private static Storage open(String host) {
        StorageOptions.Builder options =
                StorageOptions.newBuilder().setProjectId("test-project").setCredentials(NoCredentials.getInstance());
        if (host != null) {
            options.setHost(host);
        }
        com.google.cloud.storage.Storage client = mock(com.google.cloud.storage.Storage.class);
        when(client.getOptions()).thenReturn(options.build());
        return GoogleCloudStorageProvider.open(BASE_URI, client);
    }

    private static String identifierOf(Storage storage) throws IOException {
        try (RangeReader reader = storage.openRangeReader(KEY)) {
            return reader.getSourceIdentifier();
        }
    }
}
