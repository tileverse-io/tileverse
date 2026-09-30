/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tileverse.storage.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.tileverse.storage.StorageException;
import io.tileverse.storage.TransientStorageException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandler;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Single-range reads must consume the response body through its end before closing it: the JDK HTTP client turns an
 * early close into an HTTP/2 stream cancellation, and servers count those against their rapid-reset protection.
 */
@ExtendWith(MockitoExtension.class)
class HttpRangeReaderBodyDrainTest {

    private static final URI SOURCE = URI.create("http://example.test/data.bin");

    @Mock
    private HttpClient httpClient;

    private EndTrackingInputStream body;

    private static byte[] bytes(int count) {
        byte[] out = new byte[count];
        for (int i = 0; i < count; i++) {
            out[i] = (byte) (i + 1);
        }
        return out;
    }

    /** A reader whose next GET answers 206 with {@code content} as the body and no Content-Length header. */
    private HttpRangeReader readerServing(byte[] content) throws IOException, InterruptedException {
        Map<String, List<String>> headers = Map.of("Content-Range", List.of(contentRange(content)));
        return readerServing(content, headers);
    }

    /** A reader whose next GET answers 206 with {@code content} as the body and a Content-Length header of its size. */
    private HttpRangeReader readerDeclaringTheLengthOf(byte[] content) throws IOException, InterruptedException {
        Map<String, List<String>> headers = Map.of(
                "Content-Range", List.of(contentRange(content)),
                "Content-Length", List.of(String.valueOf(content.length)));
        return readerServing(content, headers);
    }

    /** A reader whose next GET answers 206 with {@code content} as the whole object and no Content-Length header. */
    private HttpRangeReader readerServingTheWholeObject(byte[] content) throws IOException, InterruptedException {
        String wholeObject = "bytes 0-" + (content.length - 1) + "/" + content.length;
        Map<String, List<String>> headers = Map.of("Content-Range", List.of(wholeObject));
        return readerServing(content, headers);
    }

    /**
     * A reader whose next GET answers 206 with {@code content} as the body and a Content-Range of {@code declared}
     * bytes.
     */
    private HttpRangeReader readerDeclaringARangeOf(int declared, byte[] content)
            throws IOException, InterruptedException {
        String declaredRange = "bytes 0-" + (declared - 1) + "/1000";
        Map<String, List<String>> headers = Map.of("Content-Range", List.of(declaredRange));
        return readerServing(content, headers);
    }

    private static String contentRange(byte[] content) {
        return "bytes 0-" + (content.length - 1) + "/1000";
    }

    @SuppressWarnings("unchecked")
    private HttpRangeReader readerServing(byte[] content, Map<String, List<String>> headers)
            throws IOException, InterruptedException {
        body = new EndTrackingInputStream(content);
        HttpResponse<InputStream> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(206);
        when(response.headers()).thenReturn(HttpHeaders.of(headers, (name, value) -> true));
        when(response.body()).thenReturn(body);
        when(httpClient.<InputStream>send(any(HttpRequest.class), any(BodyHandler.class)))
                .thenReturn(response);
        return new HttpRangeReader(SOURCE, httpClient, HttpAuthentication.NONE);
    }

    @Test
    void readsTheBodyThroughItsEndBeforeClosingIt() throws IOException, InterruptedException {
        HttpRangeReader reader = readerServing(bytes(10));
        ByteBuffer target = ByteBuffer.allocate(10);

        int read = reader.readRange(0, 10, target);

        assertThat(read).isEqualTo(10);
        assertThat(body.endObserved()).as("end of stream observed").isTrue();
        assertThat(body.closedBeforeEnd()).as("closed before the end").isFalse();
    }

    @Test
    void aBodyLongerThanRequestedIsAStorageError() throws IOException, InterruptedException {
        HttpRangeReader reader = readerServing(bytes(15));
        ByteBuffer target = ByteBuffer.allocate(32);

        assertThatThrownBy(() -> reader.readRange(0, 10, target))
                .isInstanceOf(StorageException.class)
                .hasMessageContaining("more data than requested");
    }

    @Test
    void aDeclaredLengthLongerThanRequestedIsAStorageErrorAndClosesTheBody() throws IOException, InterruptedException {
        HttpRangeReader reader = readerDeclaringTheLengthOf(bytes(15));
        ByteBuffer target = ByteBuffer.allocate(32);

        assertThatThrownBy(() -> reader.readRange(0, 10, target))
                .isInstanceOf(StorageException.class)
                .hasMessageContaining("more data than requested");
        assertThat(body.closed()).as("body closed").isTrue();
    }

    @Test
    void aBodyEndingEarlyYieldsTheShortCount() throws IOException, InterruptedException {
        HttpRangeReader reader = readerServingTheWholeObject(bytes(4));
        ByteBuffer target = ByteBuffer.allocate(10);

        int read = reader.readRange(0, 10, target);

        assertThat(read).isEqualTo(4);
        assertThat(target.position()).isEqualTo(4);
        assertThat(body.closedBeforeEnd()).isFalse();
    }

    @Test
    void aBodyEndingBeforeItsContentRangeIsATransientErrorAfterTheDrain() throws IOException, InterruptedException {
        HttpRangeReader reader = readerDeclaringARangeOf(10, bytes(4));
        ByteBuffer target = ByteBuffer.allocate(10);

        assertThatThrownBy(() -> reader.readRange(0, 10, target)).isInstanceOf(TransientStorageException.class);
        assertThat(body.closedBeforeEnd()).as("closed before the end").isFalse();
    }
}
