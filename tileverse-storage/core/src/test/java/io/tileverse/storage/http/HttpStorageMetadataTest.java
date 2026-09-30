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
package io.tileverse.storage.http;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import io.tileverse.storage.ReadHandle;
import io.tileverse.storage.StorageEntry;
import io.tileverse.storage.batch.BatchSettings;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests of the metadata read by {@link HttpStorage} from the headers of an answer to a read or a stat. */
class HttpStorageMetadataTest {

    private static final String KEY = "data.bin";
    private static final String PATH = "/" + KEY;
    private static final byte[] DATA = "some bytes".getBytes(StandardCharsets.US_ASCII);

    // Loopback only: a wildcard socket may share its port with an IPv4 listener of another process, and that
    // listener then receives the requests.
    @RegisterExtension
    WireMockExtension wm = WireMockExtension.newInstance()
            .options(wireMockConfig().dynamicPort().bindAddress("127.0.0.1"))
            .build();

    private HttpStorage storage;

    @BeforeEach
    void openStorage() {
        URI baseUri = URI.create("http://127.0.0.1:" + wm.getPort() + "/");
        HttpClientHandle client = new BorrowedHttpHandle(HttpClient.newHttpClient());
        storage = new HttpStorage(baseUri, client, HttpAuthentication.NONE, BatchSettings.httpDefaults());
    }

    @AfterEach
    void closeStorage() {
        storage.close();
    }

    /** Each row names a way to get the metadata of {@link #KEY}. */
    static Stream<Arguments> metadataReads() {
        MetadataRead read = HttpStorageMetadataTest::metadataOfARead;
        MetadataRead stat = storage -> storage.stat(KEY).orElseThrow();
        return Stream.of(Arguments.argumentSet("read", read), Arguments.argumentSet("stat", stat));
    }

    @ParameterizedTest
    @MethodSource("metadataReads")
    void lastModifiedIsReadFromTheAnswer(MetadataRead read) throws IOException {
        stubObjectLastModified("Tue, 29 Sep 2026 10:00:00 GMT");

        StorageEntry.File metadata = read.from(storage);

        assertThat(metadata.lastModified()).isEqualTo(Instant.parse("2026-09-29T10:00:00Z"));
    }

    /** A server may send a Last-Modified in another format than the one required by RFC 9110. */
    @ParameterizedTest
    @MethodSource("metadataReads")
    void unparseableLastModifiedReadsAsAbsent(MetadataRead read) throws IOException {
        stubObjectLastModified("2026-09-29T10:00:00Z");

        StorageEntry.File metadata = read.from(storage);

        assertThat(metadata.lastModified()).isEqualTo(Instant.EPOCH);
    }

    private void stubObjectLastModified(String lastModified) {
        wm.stubFor(any(urlEqualTo(PATH))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Last-Modified", lastModified)
                        .withBody(DATA)));
    }

    private static StorageEntry.File metadataOfARead(HttpStorage storage) throws IOException {
        try (ReadHandle handle = storage.read(KEY)) {
            return handle.metadata();
        }
    }

    /** Gets the metadata of {@link #KEY}. */
    @FunctionalInterface
    private interface MetadataRead {
        StorageEntry.File from(HttpStorage storage) throws IOException;
    }
}
