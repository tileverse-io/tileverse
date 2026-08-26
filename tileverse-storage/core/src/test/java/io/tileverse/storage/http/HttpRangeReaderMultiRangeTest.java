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
import static org.assertj.core.api.Assertions.assertThat;

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.RangeReaderTestSupport;
import io.tileverse.storage.RangeRequest;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

/**
 * Batched-read tests for {@link HttpRangeReader} against stubbed multi-range responses: multipart routing, part
 * reordering and coalescing, the 200 / uncovering-206 fallbacks, and 416 semantics.
 *
 * <p>Runs its methods on the same thread: {@link #negativeGapOverrideRoutesThroughPerFetchGets} mutates the
 * {@code io.tileverse.storage.batch.http.maxgap} system property that every other test here reads live through
 * {@link HttpRangeReader#coalescingPolicy()}, and {@code @ResourceLock} alone does not exclude sibling methods that
 * never declare a lock on it.
 */
@Execution(ExecutionMode.SAME_THREAD)
class HttpRangeReaderMultiRangeTest {

    private static final String TEST_PATH = "/multi-range.bin";
    private static final int FILE_SIZE = 1_000_000;
    private static final String BOUNDARY = "RANGE_PART";

    @RegisterExtension
    WireMockExtension wm = WireMockExtension.newInstance()
            .options(wireMockConfig().dynamicPort())
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
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        for (long[] part : parts) {
            long first = part[0];
            long last = part[0] + part[1] - 1;
            body.write(("--" + BOUNDARY + "\r\n"
                            + "Content-Type: application/octet-stream\r\n"
                            + "Content-Range: bytes " + first + "-" + last + "/" + FILE_SIZE + "\r\n"
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

        int[] counts = reader.readRanges(requests);

        assertContents(requests, counts);
        wm.verify(1, getRequestedFor(urlEqualTo(TEST_PATH)));
        wm.verify(0, headRequestedFor(urlEqualTo(TEST_PATH)));
        assertThat(reader.size()).hasValue(FILE_SIZE);
        wm.verify(0, headRequestedFor(urlEqualTo(TEST_PATH)));
    }

    @Test
    void reorderedPartsRouteByContentRange() throws IOException {
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=0-99,600000-600199"))
                .willReturn(multipartResponse(multipartBody(new long[][] {{600_000, 200}, {0, 100}}))));
        List<RangeRequest> requests = batchOf(new long[][] {{0, 100}, {600_000, 200}});

        int[] counts = reader.readRanges(requests);

        assertContents(requests, counts);
    }

    @Test
    void serverCoalescedPartsCoverSeveralFetches() throws IOException {
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=0-99,300000-300099"))
                .willReturn(multipartResponse(multipartBody(new long[][] {{0, 300_100}}))));
        List<RangeRequest> requests = batchOf(new long[][] {{0, 100}, {300_000, 100}});

        int[] counts = reader.readRanges(requests);

        assertContents(requests, counts);
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

        int[] counts = reader.readRanges(requests);

        assertContents(requests, counts);
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

        int[] counts = reader.readRanges(requests);

        assertContents(requests, counts);

        List<RangeRequest> second = batchOf(new long[][] {{0, 100}, {600_000, 200}});
        assertContents(second, reader.readRanges(second));
        wm.verify(1, getRequestedFor(urlEqualTo(TEST_PATH)).withHeader("Range", matching("bytes=.*,.*")));
    }

    @Test
    void uncoveringSingleRange206FallsBackToPerFetchGets() {
        stubSingleRangeGets();
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=0-99,600000-600199"))
                .willReturn(aResponse()
                        .withStatus(206)
                        .withHeader("Content-Type", "application/octet-stream")
                        .withHeader("Content-Range", "bytes 0-99/" + FILE_SIZE)
                        .withBody(slice(0, 100))));
        List<RangeRequest> requests = batchOf(new long[][] {{0, 100}, {600_000, 200}});

        int[] counts = reader.readRanges(requests);

        assertContents(requests, counts);
        wm.verify(1, getRequestedFor(urlEqualTo(TEST_PATH)).withHeader("Range", matching("bytes=.*,.*")));
    }

    @Test
    void groupWide416ReportsZeroWithoutFlippingTheReader() {
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", matching("bytes=.*,.*"))
                .willReturn(aResponse().withStatus(416).withHeader("Content-Range", "bytes */" + FILE_SIZE)));
        List<RangeRequest> requests = batchOf(new long[][] {{2_000_000, 10}, {3_000_000, 10}});

        int[] counts = reader.readRanges(requests);

        assertThat(counts).containsExactly(0, 0);

        int[] again = reader.readRanges(batchOf(new long[][] {{2_000_000, 10}, {3_000_000, 10}}));
        assertThat(again).containsExactly(0, 0);
        wm.verify(2, getRequestedFor(urlEqualTo(TEST_PATH)).withHeader("Range", matching("bytes=.*,.*")));
    }

    @Test
    void unsatisfiablePartsAreOmittedAndReportZero() throws IOException {
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=0-99,1500000-1500009"))
                .willReturn(multipartResponse(multipartBody(new long[][] {{0, 100}}))));
        List<RangeRequest> requests = batchOf(new long[][] {{0, 100}, {1_500_000, 10}});

        int[] counts = reader.readRanges(requests);

        assertThat(counts[0]).isEqualTo(100);
        assertThat(counts[1]).isZero();
        assertContents(requests, counts);
    }

    /**
     * A negative gap override disables merging: the planner returns unsorted, duplicate-preserving direct fetches that
     * the multipart router cannot safely group. The reader routes the whole batch through the template instead, never
     * attempting a multi-range GET.
     */
    @Test
    @ResourceLock(Resources.SYSTEM_PROPERTIES)
    void negativeGapOverrideRoutesThroughPerFetchGets() {
        stubSingleRangeGets();
        System.setProperty("io.tileverse.storage.batch.http.maxgap", "-1");
        try {
            List<RangeRequest> requests = batchOf(new long[][] {{0, 100}, {600_000, 200}});

            int[] counts = reader.readRanges(requests);

            assertContents(requests, counts);
            wm.verify(0, getRequestedFor(urlEqualTo(TEST_PATH)).withHeader("Range", matching("bytes=.*,.*")));
        } finally {
            System.clearProperty("io.tileverse.storage.batch.http.maxgap");
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
        int[] counts = reader.readRanges(requests);

        for (int i = 0; i < fetchCount; i++) {
            assertThat(counts[i]).as("bytes for entry " + i).isEqualTo(10);
            ByteBuffer target = requests.get(i).target().duplicate().flip();
            assertThat(target.remaining()).as("no double write on entry " + i).isEqualTo(10);
            for (int b = 0; b < 10; b++) {
                assertThat(target.get(b)).isEqualTo((byte) 7);
            }
        }
    }

    private void stubSingleRangeGets() {
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=0-99"))
                .willReturn(aResponse()
                        .withStatus(206)
                        .withHeader("Content-Type", "application/octet-stream")
                        .withHeader("Content-Range", "bytes 0-99/" + FILE_SIZE)
                        .withBody(slice(0, 100))));
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=600000-600199"))
                .willReturn(aResponse()
                        .withStatus(206)
                        .withHeader("Content-Type", "application/octet-stream")
                        .withHeader("Content-Range", "bytes 600000-600199/" + FILE_SIZE)
                        .withBody(slice(600_000, 200))));
    }
}
