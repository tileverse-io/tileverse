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
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.headRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static io.tileverse.storage.RangeReaderTestSupport.counts;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.extension.ResponseDefinitionTransformerV2;
import com.github.tomakehurst.wiremock.http.ResponseDefinition;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import io.tileverse.storage.BatchReadResult;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.RangeReaderTestSupport;
import io.tileverse.storage.RangeRequest;
import io.tileverse.storage.TransientStorageException;
import io.tileverse.storage.batch.BatchSettings;
import io.tileverse.storage.batch.CoalescingPolicy;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Batched-read tests for {@link HttpRangeReader} against stubbed multi-range responses: multipart routing, part
 * reordering and coalescing, the 200 and single-range 206 fallbacks, and 416 semantics.
 */
class HttpRangeReaderMultiRangeTest {

    private static final String TEST_PATH = "/multi-range.bin";
    private static final int FILE_SIZE = 1_000_000;
    private static final String BOUNDARY = "RANGE_PART";

    private final HoldUntilReleased holdUntilReleased = new HoldUntilReleased();

    // Loopback only: a wildcard socket may share its port with an IPv4 listener of another process, and that
    // listener then receives the requests.
    @RegisterExtension
    WireMockExtension wm = WireMockExtension.newInstance()
            .options(wireMockConfig().dynamicPort().bindAddress("127.0.0.1").extensions(holdUntilReleased))
            .build();

    private RangeReader reader;

    @BeforeEach
    void createReader() {
        URI uri = URI.create("http://localhost:" + wm.getPort() + TEST_PATH);
        reader = RangeReaderTestSupport.httpReader(uri);
    }

    @AfterEach
    void closeReader() throws IOException {
        reader.close();
    }

    private static byte byteAt(long offset) {
        return (byte) (offset % 251);
    }

    private static byte[] slice(long offset, int length) {
        byte[] data = new byte[length];
        for (int i = 0; i < length; i++) {
            data[i] = byteAt(offset + i);
        }
        return data;
    }

    /** Builds a multipart/byteranges body; each part row is {offset, length}. */
    private static byte[] multipartBody(long[][] parts) throws IOException {
        return multipartBody(parts, String.valueOf(FILE_SIZE));
    }

    /**
     * Builds a multipart/byteranges body giving {@code total} as the object size, {@code *} for unknown; each part row
     * is {offset, length}.
     */
    private static byte[] multipartBody(long[][] parts, String total) throws IOException {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        for (long[] part : parts) {
            long first = part[0];
            long last = part[0] + part[1] - 1;
            body.write(("--" + BOUNDARY + "\r\n"
                            + "Content-Type: application/octet-stream\r\n"
                            + "Content-Range: bytes " + first + "-" + last + "/" + total + "\r\n"
                            + "\r\n")
                    .getBytes(StandardCharsets.ISO_8859_1));
            body.write(slice(first, (int) part[1]));
            body.write("\r\n".getBytes(StandardCharsets.ISO_8859_1));
        }
        body.write(("--" + BOUNDARY + "--\r\n").getBytes(StandardCharsets.ISO_8859_1));
        return body.toByteArray();
    }

    private static ResponseDefinitionBuilder multipartResponse(byte[] body) {
        return aResponse()
                .withStatus(206)
                .withHeader("Content-Type", "multipart/byteranges; boundary=" + BOUNDARY)
                .withHeader("ETag", "\"multi-range-fixture\"")
                .withBody(body);
    }

    private static List<RangeRequest> batchOf(long[][] ranges) {
        List<RangeRequest> requests = new ArrayList<>();
        for (long[] range : ranges) {
            requests.add(RangeRequest.of(range[0], (int) range[1], ByteBuffer.allocate((int) range[1])));
        }
        return requests;
    }

    private static void assertContents(List<RangeRequest> requests, int[] counts) {
        for (int i = 0; i < requests.size(); i++) {
            RangeRequest request = requests.get(i);
            long offset = request.range().offset();
            int expected = (int) Math.max(0, Math.min(request.range().length(), FILE_SIZE - offset));
            assertThat(counts[i]).as("bytes for entry " + i).isEqualTo(expected);
            ByteBuffer target = request.target().duplicate().flip();
            assertThat(target.remaining()).as("entry " + i).isEqualTo(expected);
            for (int b = 0; b < expected; b++) {
                assertThat(target.get(b)).as("byte " + b + " of entry " + i).isEqualTo(byteAt(offset + b));
            }
        }
    }

    @Test
    void farApartRangesTravelInOneMultipartGet() throws IOException {
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=0-99,600000-600199"))
                .willReturn(multipartResponse(multipartBody(new long[][] {{0, 100}, {600_000, 200}}))));
        List<RangeRequest> requests = batchOf(new long[][] {{0, 100}, {600_000, 200}});

        BatchReadResult result = reader.readRanges(requests);

        assertContents(requests, counts(result));
        wm.verify(1, getRequestedFor(urlEqualTo(TEST_PATH)));
        wm.verify(0, headRequestedFor(urlEqualTo(TEST_PATH)));
        assertThat(reader.size()).hasValue(FILE_SIZE);
        wm.verify(0, headRequestedFor(urlEqualTo(TEST_PATH)));
        assertThat(result.fetches()).as("one multi-range GET").isEqualTo(1);
        assertThat(result.bytesRequested()).isEqualTo(300);
        assertThat(result.bytesTransferred()).as("the two parts' payload").isEqualTo(300);
        assertThat(result.bytesFromCache()).isZero();
    }

    @Test
    void reorderedPartsRouteByContentRange() throws IOException {
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=0-99,600000-600199"))
                .willReturn(multipartResponse(multipartBody(new long[][] {{600_000, 200}, {0, 100}}))));
        List<RangeRequest> requests = batchOf(new long[][] {{0, 100}, {600_000, 200}});

        int[] counts = counts(reader.readRanges(requests));

        assertContents(requests, counts);
    }

    @Test
    void serverCoalescedPartsCoverSeveralFetches() throws IOException {
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=0-99,300000-300099"))
                .willReturn(multipartResponse(multipartBody(new long[][] {{0, 300_100}}))));
        List<RangeRequest> requests = batchOf(new long[][] {{0, 100}, {300_000, 100}});

        BatchReadResult result = reader.readRanges(requests);

        assertContents(requests, counts(result));
        assertThat(result.fetches()).isEqualTo(1);
        assertThat(result.bytesTransferred())
                .as("the server sent one part spanning both fetches and the gap between them")
                .isEqualTo(300_100);
    }

    @Test
    void singleRange206CoveringTheGroupScattersWithSkips() {
        byte[] span = slice(0, 600_200);
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=0-99,600000-600199"))
                .willReturn(aResponse()
                        .withStatus(206)
                        .withHeader("Content-Type", "application/octet-stream")
                        .withHeader("Content-Range", "bytes 0-600199/" + FILE_SIZE)
                        .withBody(span)));
        List<RangeRequest> requests = batchOf(new long[][] {{0, 100}, {600_000, 200}});

        int[] counts = counts(reader.readRanges(requests));

        assertContents(requests, counts);
    }

    /**
     * A single-range body ending before the end of its Content-Range fails the batch, like a truncated multipart body.
     */
    @Test
    void singleRange206EndingBeforeItsContentRangeFailsTheBatch() {
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=0-99,600000-600199"))
                .willReturn(aResponse()
                        .withStatus(206)
                        .withHeader("Content-Type", "application/octet-stream")
                        .withHeader("Content-Range", "bytes 0-600199/" + FILE_SIZE)
                        .withBody(slice(0, 600_100))));
        List<RangeRequest> requests = batchOf(new long[][] {{0, 100}, {600_000, 200}});

        assertThatThrownBy(() -> reader.readRanges(requests)).isInstanceOf(TransientStorageException.class);
    }

    @Test
    void http200FlipsTheReaderToPerFetchGets() {
        stubSingleRangeGets();
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=0-99,600000-600199"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/octet-stream")
                        .withBody(slice(0, 1000))));
        List<RangeRequest> requests = batchOf(new long[][] {{0, 100}, {600_000, 200}});

        int[] counts = counts(reader.readRanges(requests));

        assertContents(requests, counts);

        List<RangeRequest> second = batchOf(new long[][] {{0, 100}, {600_000, 200}});
        assertContents(second, counts(reader.readRanges(second)));
        wm.verify(1, getRequestedFor(urlEqualTo(TEST_PATH)).withHeader("Range", matching("bytes=.*,.*")));
    }

    /**
     * Batches answered with the single range {@code 0-99}, leaving out the range {@code 600000-600199} before the end
     * of the object. Each row names a batch and the Range header of its multi-range GET.
     */
    static Stream<Arguments> batchesLeavingOutARangeBeforeTheEnd() {
        return Stream.of(
                Arguments.argumentSet(
                        "only the range before the end",
                        new long[][] {{0, 100}, {600_000, 200}},
                        "bytes=0-99,600000-600199"),
                Arguments.argumentSet(
                        "beside a range past the end",
                        new long[][] {{0, 100}, {600_000, 200}, {2_000_000, 10}},
                        "bytes=0-99,600000-600199,2000000-2000009"));
    }

    @ParameterizedTest
    @MethodSource("batchesLeavingOutARangeBeforeTheEnd")
    void singleRange206LeavingOutARangeBeforeTheEndFallsBackToPerFetchGets(long[][] ranges, String rangeHeader) {
        stubSingleRangeGets();
        stubUnsatisfiableSingleRangeGet(2_000_000, 10);
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo(rangeHeader))
                .willReturn(aResponse()
                        .withStatus(206)
                        .withHeader("Content-Type", "application/octet-stream")
                        .withHeader("Content-Range", "bytes 0-99/" + FILE_SIZE)
                        .withBody(slice(0, 100))));
        List<RangeRequest> requests = batchOf(ranges);

        int[] counts = counts(reader.readRanges(requests));

        assertContents(requests, counts);
        List<RangeRequest> next = batchOf(new long[][] {{0, 100}, {600_000, 200}});
        assertContents(next, counts(reader.readRanges(next)));
        wm.verify(1, getRequestedFor(urlEqualTo(TEST_PATH)).withHeader("Range", matching("bytes=.*,.*")));
    }

    /**
     * nginx, Apache httpd, Caddy and lighttpd drop the ranges past the end from a multi-range request and answer the
     * only range left as a plain single-range 206. The range past the end reads as end of file, and the reader keeps
     * sending multi-range GETs.
     */
    @Test
    void singleRange206LeavingOutOnlyRangesPastTheEndKeepsMultiRangeGets() throws IOException {
        stubSingleRangeGets();
        stubUnsatisfiableSingleRangeGet(2_000_000, 10);
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=0-99,2000000-2000009"))
                .willReturn(aResponse()
                        .withStatus(206)
                        .withHeader("Content-Type", "application/octet-stream")
                        .withHeader("Content-Range", "bytes 0-99/" + FILE_SIZE)
                        .withBody(slice(0, 100))));
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=0-99,600000-600199"))
                .willReturn(multipartResponse(multipartBody(new long[][] {{0, 100}, {600_000, 200}}))));
        List<RangeRequest> requests = batchOf(new long[][] {{0, 100}, {2_000_000, 10}});

        BatchReadResult result = reader.readRanges(requests);

        assertContents(requests, counts(result));
        assertThat(result.fetches()).as("one multi-range GET").isEqualTo(1);
        List<RangeRequest> next = batchOf(new long[][] {{0, 100}, {600_000, 200}});
        assertContents(next, counts(reader.readRanges(next)));
        wm.verify(2, getRequestedFor(urlEqualTo(TEST_PATH)).withHeader("Range", matching("bytes=.*,.*")));
        wm.verify(2, getRequestedFor(urlEqualTo(TEST_PATH)));
    }

    @Test
    void groupWide416ReportsZeroWithoutFlippingTheReader() {
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", matching("bytes=.*,.*"))
                .willReturn(aResponse().withStatus(416).withHeader("Content-Range", "bytes */" + FILE_SIZE)));
        List<RangeRequest> requests = batchOf(new long[][] {{2_000_000, 10}, {3_000_000, 10}});

        int[] counts = counts(reader.readRanges(requests));

        assertThat(counts).containsExactly(0, 0);

        int[] again = counts(reader.readRanges(batchOf(new long[][] {{2_000_000, 10}, {3_000_000, 10}})));
        assertThat(again).containsExactly(0, 0);
        wm.verify(2, getRequestedFor(urlEqualTo(TEST_PATH)).withHeader("Range", matching("bytes=.*,.*")));
        wm.verify(2, getRequestedFor(urlEqualTo(TEST_PATH)));
    }

    /**
     * A server may reject the whole set of ranges with a 416 as soon as one of them starts past the end, as Tomcat
     * does. The in-bounds range is read with a GET of its own; the one past the end reads as end of file.
     */
    @Test
    void groupWide416ReadsTheRangesBeforeTheEndWithTheirOwnGet() {
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=0-99,2000000-2000009"))
                .willReturn(aResponse().withStatus(416).withHeader("Content-Range", "bytes */" + FILE_SIZE)));
        stubSingleRangeGet(0, 100);
        List<RangeRequest> requests = batchOf(new long[][] {{0, 100}, {2_000_000, 10}});

        BatchReadResult result = reader.readRanges(requests);

        assertContents(requests, counts(result));
        wm.verify(1, getRequestedFor(urlEqualTo(TEST_PATH)).withHeader("Range", equalTo("bytes=0-99")));
        wm.verify(2, getRequestedFor(urlEqualTo(TEST_PATH)));
        assertThat(result.fetches())
                .as("the rejected multi-range GET and the GET of the in-bounds range")
                .isEqualTo(2);
        assertThat(result.bytesTransferred()).isEqualTo(100);
    }

    @Test
    void groupWide416WithoutAKnownSizeReadsEveryRangeWithItsOwnGet() {
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=0-99,2000000-2000009"))
                .willReturn(aResponse().withStatus(416)));
        stubSingleRangeGet(0, 100);
        stubUnsatisfiableSingleRangeGet(2_000_000, 10);
        List<RangeRequest> requests = batchOf(new long[][] {{0, 100}, {2_000_000, 10}});

        int[] counts = counts(reader.readRanges(requests));

        assertContents(requests, counts);
        wm.verify(1, getRequestedFor(urlEqualTo(TEST_PATH)).withHeader("Range", equalTo("bytes=0-99")));
        wm.verify(1, getRequestedFor(urlEqualTo(TEST_PATH)).withHeader("Range", equalTo("bytes=2000000-2000009")));
    }

    @Test
    void groupWide416WithoutContentRangeUsesTheSizeAlreadyKnown() {
        stubSingleRangeGet(0, 100);
        reader.readRange(0, 100, ByteBuffer.allocate(100));
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=0-99,2000000-2000009"))
                .willReturn(aResponse().withStatus(416)));
        List<RangeRequest> requests = batchOf(new long[][] {{0, 100}, {2_000_000, 10}});

        int[] counts = counts(reader.readRanges(requests));

        assertContents(requests, counts);
        wm.verify(2, getRequestedFor(urlEqualTo(TEST_PATH)).withHeader("Range", equalTo("bytes=0-99")));
        wm.verify(0, getRequestedFor(urlEqualTo(TEST_PATH)).withHeader("Range", equalTo("bytes=2000000-2000009")));
        wm.verify(0, headRequestedFor(urlEqualTo(TEST_PATH)));
    }

    @Test
    void unsatisfiablePartsAreOmittedAndReportZero() throws IOException {
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=0-99,1500000-1500009"))
                .willReturn(multipartResponse(multipartBody(new long[][] {{0, 100}}))));
        List<RangeRequest> requests = batchOf(new long[][] {{0, 100}, {1_500_000, 10}});

        int[] counts = counts(reader.readRanges(requests));

        assertThat(counts[0]).isEqualTo(100);
        assertThat(counts[1]).isZero();
        assertContents(requests, counts);
        wm.verify(1, getRequestedFor(urlEqualTo(TEST_PATH)));
    }

    /**
     * Batches planning a fetch of 0-99 and a fetch of 600000-600199, the second one landing straight in its target or
     * merging two nearby requests.
     */
    static Stream<Arguments> batchesWithASecondFetch() {
        return Stream.of(
                Arguments.argumentSet("direct fetch", (Object) new long[][] {{0, 100}, {600_000, 200}}),
                Arguments.argumentSet("merged fetch", (Object) new long[][] {{0, 100}, {600_000, 100}, {600_150, 50}}));
    }

    @ParameterizedTest
    @MethodSource("batchesWithASecondFetch")
    void satisfiableRangeMissingFromTheMultipartAnswerIsReadWithItsOwnGet(long[][] ranges) throws IOException {
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=0-99,600000-600199"))
                .willReturn(multipartResponse(multipartBody(new long[][] {{0, 100}}))));
        stubSingleRangeGet(600_000, 200);
        List<RangeRequest> requests = batchOf(ranges);

        BatchReadResult result = reader.readRanges(requests);

        assertContents(requests, counts(result));
        wm.verify(1, getRequestedFor(urlEqualTo(TEST_PATH)).withHeader("Range", equalTo("bytes=600000-600199")));
        assertThat(result.fetches())
                .as("the multi-range GET and the GET of the missing range")
                .isEqualTo(2);
        assertThat(result.bytesTransferred())
                .as("the part payload and the missing range")
                .isEqualTo(300);
    }

    /**
     * Parts of which none holds the fetch {@code 0-99} of a 1,000,000-byte object whole, as allowed by RFC 9110 section
     * 15.3.7. Each row names a batch and the parts answering its multi-range GET of {@code 0-99,600000-600199}.
     */
    static Stream<Arguments> partsCuttingAFetchShort() {
        return Stream.of(
                Arguments.argumentSet(
                        "part ending inside the fetch",
                        new long[][] {{0, 100}, {600_000, 200}},
                        new long[][] {{0, 50}, {600_000, 200}}),
                Arguments.argumentSet(
                        "fetch split into two parts",
                        new long[][] {{0, 100}, {600_000, 200}},
                        new long[][] {{0, 50}, {50, 50}, {600_000, 200}}),
                Arguments.argumentSet(
                        "merged fetch cut before its second request",
                        new long[][] {{0, 50}, {60, 40}, {600_000, 200}},
                        new long[][] {{0, 55}, {600_000, 200}}));
    }

    @ParameterizedTest
    @MethodSource("partsCuttingAFetchShort")
    void fetchCutShortBeforeTheEndIsReadWithItsOwnGet(long[][] ranges, long[][] parts) throws IOException {
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=0-99,600000-600199"))
                .willReturn(multipartResponse(multipartBody(parts))));
        stubSingleRangeGet(0, 100);
        List<RangeRequest> requests = batchOf(ranges);

        BatchReadResult result = reader.readRanges(requests);

        assertContents(requests, counts(result));
        wm.verify(1, getRequestedFor(urlEqualTo(TEST_PATH)).withHeader("Range", equalTo("bytes=0-99")));
        assertThat(result.fetches())
                .as("the multi-range GET and the GET of the fetch cut short")
                .isEqualTo(2);
    }

    /** A fetch cut short by one part needs no GET of its own when a later part of the answer holds it whole. */
    @Test
    void fetchAnsweredWholeByALaterPartNeedsNoExtraGet() throws IOException {
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=0-99,600000-600199"))
                .willReturn(multipartResponse(multipartBody(new long[][] {{0, 50}, {0, 100}, {600_000, 200}}))));
        stubSingleRangeGet(0, 100);
        List<RangeRequest> requests = batchOf(new long[][] {{0, 100}, {600_000, 200}});

        BatchReadResult result = reader.readRanges(requests);

        assertContents(requests, counts(result));
        assertThat(result.fetches()).as("one multi-range GET").isEqualTo(1);
        wm.verify(0, getRequestedFor(urlEqualTo(TEST_PATH)).withHeader("Range", equalTo("bytes=0-99")));
    }

    /**
     * The GET of a fetch missing from the multipart answer is answered in part and completed by a GET of the rest. The
     * fetch count leaves the completing GET out.
     */
    @Test
    void missingFetchAnsweredInPartIsCompletedByAGetOfTheRest() throws IOException {
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=0-99,600000-600199"))
                .willReturn(multipartResponse(multipartBody(new long[][] {{0, 100}}))));
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=600000-600199"))
                .willReturn(aResponse()
                        .withStatus(206)
                        .withHeader("Content-Range", "bytes 600000-600099/" + FILE_SIZE)
                        .withBody(slice(600_000, 100))));
        stubSingleRangeGet(600_100, 100);
        List<RangeRequest> requests = batchOf(new long[][] {{0, 100}, {600_000, 200}});

        BatchReadResult result = reader.readRanges(requests);

        assertContents(requests, counts(result));
        wm.verify(1, getRequestedFor(urlEqualTo(TEST_PATH)).withHeader("Range", equalTo("bytes=600000-600199")));
        wm.verify(1, getRequestedFor(urlEqualTo(TEST_PATH)).withHeader("Range", equalTo("bytes=600100-600199")));
        wm.verify(3, getRequestedFor(urlEqualTo(TEST_PATH)));
        assertThat(result.fetches())
                .as("the multi-range GET and the GET of the missing fetch")
                .isEqualTo(2);
        assertThat(result.bytesTransferred())
                .as("the part payload and the missing fetch")
                .isEqualTo(300);
    }

    /**
     * Each row names an answer telling that an object of unknown size ends before the rest of a fetch. A server may
     * ignore a range past the end of the object and answer the whole object.
     */
    static Stream<Arguments> answersToTheRestPastTheEnd() {
        return Stream.of(
                Arguments.argumentSet("416", aResponse().withStatus(416)),
                Arguments.argumentSet(
                        "200 with the whole object", aResponse().withStatus(200).withBody(slice(0, FILE_SIZE))));
    }

    /**
     * With the object size unknown, a part ending before the end of its fetch holds the fetch in part, even when the
     * object ends there: the fetch is read with a GET of its own, and the answer to the GET of the rest ends it.
     */
    @ParameterizedTest
    @MethodSource("answersToTheRestPastTheEnd")
    void fetchStraddlingTheEndOfAnObjectOfUnknownSizeEndsWithTheObject(ResponseDefinitionBuilder rest)
            throws IOException {
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=0-99,999950-1000049"))
                .willReturn(multipartResponse(multipartBody(new long[][] {{0, 100}, {999_950, 50}}, "*"))));
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=999950-1000049"))
                .willReturn(aResponse()
                        .withStatus(206)
                        .withHeader("Content-Range", "bytes 999950-999999/*")
                        .withBody(slice(999_950, 50))));
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=1000000-1000049"))
                .willReturn(rest));
        List<RangeRequest> requests = batchOf(new long[][] {{0, 100}, {999_950, 100}});

        BatchReadResult result = reader.readRanges(requests);

        assertContents(requests, counts(result));
        wm.verify(1, getRequestedFor(urlEqualTo(TEST_PATH)).withHeader("Range", equalTo("bytes=999950-1000049")));
        wm.verify(1, getRequestedFor(urlEqualTo(TEST_PATH)).withHeader("Range", equalTo("bytes=1000000-1000049")));
        wm.verify(3, getRequestedFor(urlEqualTo(TEST_PATH)));
    }

    @Test
    void repeatedPartWritesItsFetchOnce() throws IOException {
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=0-99,600000-600199"))
                .willReturn(multipartResponse(multipartBody(new long[][] {{0, 100}, {0, 100}, {600_000, 200}}))));
        List<RangeRequest> requests = batchOf(new long[][] {{0, 100}, {600_000, 200}});

        int[] counts = counts(reader.readRanges(requests));

        assertContents(requests, counts);
    }

    /**
     * A negative gap disables merging: the reader routes the whole batch through the template, one GET per planned
     * fetch, never attempting a multi-range GET.
     */
    @Test
    void negativeGapRoutesThroughPerFetchGets() {
        stubSingleRangeGets();
        BatchSettings noMerging = new BatchSettings(-1, CoalescingPolicy.DEFAULT_MAX_FETCH_BYTES, 8);
        URI uri = URI.create("http://localhost:" + wm.getPort() + TEST_PATH);
        try (HttpRangeReader unmerged =
                new HttpRangeReader(uri, HttpClient.newHttpClient(), HttpAuthentication.NONE, noMerging)) {
            List<RangeRequest> requests = batchOf(new long[][] {{0, 100}, {600_000, 200}});

            int[] counts = counts(unmerged.readRanges(requests));

            assertContents(requests, counts);
            wm.verify(0, getRequestedFor(urlEqualTo(TEST_PATH)).withHeader("Range", matching("bytes=.*,.*")));
        }
    }

    /**
     * Requests overlapping beyond a 1000-byte fetch cap plan into overlapping fetches. Each row names a batch and the
     * parts of a server answering all its ranges in one multi-range GET. The reader must never send that combined GET.
     */
    static Stream<Arguments> overlappingFetches() {
        return Stream.of(
                Arguments.argumentSet(
                        "overlapping requests, coalesced answer",
                        new long[][] {{0, 600}, {300, 800}},
                        new long[][] {{0, 1100}}),
                Arguments.argumentSet(
                        "overlapping requests, parts in reverse order",
                        new long[][] {{0, 600}, {300, 800}},
                        new long[][] {{300, 800}, {0, 600}}),
                Arguments.argumentSet(
                        "duplicate requests above the cap, one part per range",
                        new long[][] {{0, 1500}, {0, 1500}},
                        new long[][] {{0, 1500}, {0, 1500}}),
                Arguments.argumentSet(
                        "duplicate requests above the cap, coalesced answer",
                        new long[][] {{0, 1500}, {0, 1500}},
                        new long[][] {{0, 1500}}),
                Arguments.argumentSet(
                        "request inside an oversized one, coalesced answer",
                        new long[][] {{0, 1500}, {200, 100}},
                        new long[][] {{0, 1500}}));
    }

    @ParameterizedTest
    @MethodSource("overlappingFetches")
    void overlappingFetchesReadTheirOwnBytes(long[][] ranges, long[][] combinedAnswer) throws IOException {
        String combinedRangeHeader = stubMultiRangeGet(ranges, combinedAnswer);
        for (long[] range : ranges) {
            stubSingleRangeGet(range[0], (int) range[1]);
        }
        BatchSettings smallFetches = new BatchSettings(1000, 1000, 8);
        URI uri = URI.create("http://localhost:" + wm.getPort() + TEST_PATH);
        try (HttpRangeReader capped =
                new HttpRangeReader(uri, HttpClient.newHttpClient(), HttpAuthentication.NONE, smallFetches)) {
            List<RangeRequest> requests = batchOf(ranges);

            int[] counts = counts(capped.readRanges(requests));

            assertContents(requests, counts);
            wm.verify(0, getRequestedFor(urlEqualTo(TEST_PATH)).withHeader("Range", equalTo(combinedRangeHeader)));
        }
    }

    /**
     * Two groups (101 fetches, cap 100): the first succeeds as multipart and scatters, the second gets a 200. The
     * fallback must restore every target position before re-reading; a double write would leave twice the bytes in a
     * target and fail the content assertions.
     */
    @Test
    void fallbackAfterAPartiallyScatteredGroupRestoresTargetPositions() throws IOException {
        int fetchCount = 101;
        long stride = 300_000L;
        long[][] ranges = new long[fetchCount][];
        StringBuilder firstGroupSpecs = new StringBuilder("bytes=");
        long[][] firstGroupParts = new long[100][];
        for (int i = 0; i < fetchCount; i++) {
            ranges[i] = new long[] {i * stride, 10};
            if (i < 100) {
                if (i > 0) {
                    firstGroupSpecs.append(',');
                }
                firstGroupSpecs.append(i * stride).append('-').append(i * stride + 9);
                firstGroupParts[i] = ranges[i];
            }
        }
        String lastFetchSpec = "bytes=" + (100 * stride) + "-" + (100 * stride + 9);

        // constant-content fixture: any correct read yields bytes of 7; a position bug shows as extra bytes
        byte[] constantTen = new byte[10];
        Arrays.fill(constantTen, (byte) 7);
        ByteArrayOutputStream firstBody = new ByteArrayOutputStream();
        for (long[] part : firstGroupParts) {
            firstBody.write(("--" + BOUNDARY + "\r\n" + "Content-Range: bytes " + part[0] + "-" + (part[0] + 9) + "/"
                            + (40_000_000L) + "\r\n" + "\r\n")
                    .getBytes(StandardCharsets.ISO_8859_1));
            firstBody.write(constantTen);
            firstBody.write("\r\n".getBytes(StandardCharsets.ISO_8859_1));
        }
        firstBody.write(("--" + BOUNDARY + "--\r\n").getBytes(StandardCharsets.ISO_8859_1));

        // ORDER MATTERS: WireMock picks the most recently added matching stub; register the generic
        // single-range stub first and the specific stubs after it
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", matching("bytes=\\d+-\\d+"))
                .willReturn(aResponse()
                        .withStatus(206)
                        .withHeader("Content-Type", "application/octet-stream")
                        .withBody(constantTen)));
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo(firstGroupSpecs.toString()))
                .willReturn(aResponse()
                        .withStatus(206)
                        .withHeader("Content-Type", "multipart/byteranges; boundary=" + BOUNDARY)
                        .withBody(firstBody.toByteArray())));
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .inScenario("multi-range-flip")
                .whenScenarioStateIs(Scenario.STARTED)
                .withHeader("Range", equalTo(lastFetchSpec))
                .willSetStateTo("fell-back")
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/octet-stream")
                        .withBody(constantTen)));

        List<RangeRequest> requests = batchOf(ranges);
        int[] counts = counts(reader.readRanges(requests));

        for (int i = 0; i < fetchCount; i++) {
            assertThat(counts[i]).as("bytes for entry " + i).isEqualTo(10);
            ByteBuffer target = requests.get(i).target().duplicate().flip();
            assertThat(target.remaining()).as("no double write on entry " + i).isEqualTo(10);
            for (int b = 0; b < 10; b++) {
                assertThat(target.get(b)).isEqualTo((byte) 7);
            }
        }
    }

    /**
     * Under a 1000-byte fetch cap the batch plans as two groups, {@code 0-99} and {@code 50-1549,600000-600099}. The
     * second group's answer leaves out {@code 600000-600099}, read with an extra GET, and the first group's GET gets a
     * 200. The 200 is held until the second group's GET arrives, as a refusal keeps groups not yet started from
     * running. The fallback reads the whole batch again, and the result counts every GET sent before and after the
     * refusal.
     */
    @Test
    @Timeout(30)
    void refusalAfterAnExtraGetReadsTheWholeBatchAgain() throws IOException {
        stubSingleRangeGet(0, 100);
        stubSingleRangeGet(50, 1500);
        stubSingleRangeGet(600_000, 100);
        // registered after the single-range stub of 0-99: WireMock picks the most recently added matching stub
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .inScenario("first-group-refused")
                .whenScenarioStateIs(Scenario.STARTED)
                .withHeader("Range", equalTo("bytes=0-99"))
                .willSetStateTo("refused")
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/octet-stream")
                        .withBody(slice(0, 1000))
                        .withTransformers(HoldUntilReleased.NAME)));
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=50-1549,600000-600099"))
                .willReturn(multipartResponse(multipartBody(new long[][] {{50, 1500}}))
                        .withTransformer(HoldUntilReleased.NAME, HoldUntilReleased.RELEASE, true)));
        BatchSettings smallFetches = new BatchSettings(1000, 1000, 8);
        URI uri = URI.create("http://localhost:" + wm.getPort() + TEST_PATH);
        try (HttpRangeReader capped =
                new HttpRangeReader(uri, HttpClient.newHttpClient(), HttpAuthentication.NONE, smallFetches)) {
            List<RangeRequest> requests = batchOf(new long[][] {{0, 100}, {50, 1500}, {600_000, 100}});

            BatchReadResult result = capped.readRanges(requests);

            assertContents(requests, counts(result));
            wm.verify(2, getRequestedFor(urlEqualTo(TEST_PATH)).withHeader("Range", equalTo("bytes=0-99")));
            wm.verify(1, getRequestedFor(urlEqualTo(TEST_PATH)).withHeader("Range", equalTo("bytes=50-1549")));
            wm.verify(2, getRequestedFor(urlEqualTo(TEST_PATH)).withHeader("Range", equalTo("bytes=600000-600099")));
            wm.verify(6, getRequestedFor(urlEqualTo(TEST_PATH)));
            assertThat(result.fetches())
                    .as("both multi-range GETs, the extra GET, and one GET per planned fetch after the refusal")
                    .isEqualTo(6);
            assertThat(result.bytesTransferred())
                    .as("the part and the extra GET before the refusal, then the whole batch again")
                    .isEqualTo(1500 + 100 + 1700);
        }
    }

    private void stubSingleRangeGets() {
        stubSingleRangeGet(0, 100);
        stubSingleRangeGet(600_000, 200);
    }

    private void stubSingleRangeGet(long offset, int length) {
        long last = offset + length - 1;
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=" + offset + "-" + last))
                .willReturn(aResponse()
                        .withStatus(206)
                        .withHeader("Content-Type", "application/octet-stream")
                        .withHeader("Content-Range", "bytes " + offset + "-" + last + "/" + FILE_SIZE)
                        .withBody(slice(offset, length))));
    }

    private void stubUnsatisfiableSingleRangeGet(long offset, int length) {
        long last = offset + length - 1;
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=" + offset + "-" + last))
                .willReturn(aResponse().withStatus(416).withHeader("Content-Range", "bytes */" + FILE_SIZE)));
    }

    /**
     * Stubs one multi-range GET naming {@code ranges} in order, answered with {@code parts}, and returns its Range
     * header.
     */
    private String stubMultiRangeGet(long[][] ranges, long[][] parts) throws IOException {
        StringBuilder rangeHeader = new StringBuilder("bytes=");
        for (int i = 0; i < ranges.length; i++) {
            if (i > 0) {
                rangeHeader.append(',');
            }
            rangeHeader.append(ranges[i][0]).append('-').append(ranges[i][0] + ranges[i][1] - 1);
        }
        String header = rangeHeader.toString();
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo(header))
                .willReturn(multipartResponse(multipartBody(parts))));
        return header;
    }

    /**
     * Holds the responses of the stubs naming it until a stub with the {@value #RELEASE} parameter is matched, letting
     * a test pick the last one answered of two concurrent requests.
     */
    private static final class HoldUntilReleased implements ResponseDefinitionTransformerV2 {
        static final String NAME = "hold-until-released";
        static final String RELEASE = "release";

        private final CountDownLatch released = new CountDownLatch(1);

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public boolean applyGlobally() {
            return false;
        }

        @Override
        public ResponseDefinition transform(ServeEvent serveEvent) {
            boolean release = serveEvent.getTransformerParameters().getBoolean(RELEASE, false);
            if (release) {
                released.countDown();
            } else {
                awaitRelease();
            }
            return serveEvent.getResponseDefinition();
        }

        private void awaitRelease() {
            try {
                if (!released.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("no releasing request arrived");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while holding a response", e);
            }
        }
    }
}
