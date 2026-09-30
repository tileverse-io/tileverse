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
package io.tileverse.storage.http;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.head;
import static com.github.tomakehurst.wiremock.client.WireMock.headRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.RangeReaderTestSupport;
import io.tileverse.storage.StorageException;
import io.tileverse.storage.TransientStorageException;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Comprehensive tests for HttpRangeReader using WireMock. */
@Slf4j
class HttpRangeReaderTest {

    private static final String TEST_PATH = "/test-pmtiles";
    private static final byte[] TEST_DATA = createTestData(100_000); // 100KB of test data
    private static final String LAST_MODIFIED = "Tue, 29 Sep 2026 10:00:00 GMT";
    private static final String LATER_LAST_MODIFIED = "Tue, 29 Sep 2026 11:00:00 GMT";

    // Loopback only: a wildcard socket may share its port with an IPv4 listener of another process, and that
    // listener then receives the requests.
    @RegisterExtension
    WireMockExtension wm = WireMockExtension.newInstance()
            .options(wireMockConfig().dynamicPort().bindAddress("127.0.0.1"))
            .build();

    private URI testUri;
    private RangeReader rangeReader;

    /** Creates test data with a predictable pattern. */
    private static byte[] createTestData(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) (i % 256);
        }
        return data;
    }

    @BeforeEach
    void setUp() {
        testUri = URI.create("http://localhost:" + wm.getPort() + TEST_PATH);

        // Set up WireMock stubs
        // HEAD request to get content length
        wm.stubFor(head(urlEqualTo(TEST_PATH))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Length", String.valueOf(TEST_DATA.length))
                        .withHeader("Accept-Ranges", "bytes")));

        // Fallback range response for tests that read without stubbing their own range request. The reader
        // always sends a Range header; this stub simulates a server honoring bytes=0-99, the range the
        // no-HEAD tests below use.
        int fallbackStart = 0;
        int fallbackEnd = 99;
        byte[] fallbackBytes = Arrays.copyOfRange(TEST_DATA, fallbackStart, fallbackEnd + 1);
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=" + fallbackStart + "-" + fallbackEnd))
                .willReturn(aResponse()
                        .withStatus(206)
                        .withHeader(
                                "Content-Range", "bytes " + fallbackStart + "-" + fallbackEnd + "/" + TEST_DATA.length)
                        .withHeader("Content-Length", String.valueOf(fallbackBytes.length))
                        .withBody(fallbackBytes)));

        // Individual range request stubs - we'll create these for each test as needed

        // Create reader
        rangeReader = RangeReaderTestSupport.httpReader(testUri);
    }

    @Test
    void testGetSize() {
        assertThat(rangeReader.size()).hasValue(TEST_DATA.length);

        // Verify that HEAD request was made
        wm.verify(headRequestedFor(urlEqualTo(TEST_PATH)));
    }

    @Test
    void readOnlyWorkloadIssuesNoHeadRequest() {
        ByteBuffer target = ByteBuffer.allocate(100);
        int read = rangeReader.readRange(0, 100, target);

        assertEquals(100, read);
        wm.verify(0, headRequestedFor(urlEqualTo(TEST_PATH)));
    }

    @Test
    void sizeCapturedFromRangeResponseWithoutHead() {
        rangeReader.readRange(0, 100, ByteBuffer.allocate(100));

        assertEquals(OptionalLong.of(TEST_DATA.length), rangeReader.size());
        wm.verify(0, headRequestedFor(urlEqualTo(TEST_PATH)));
    }

    @Test
    void sizeBeforeAnyReadIssuesExactlyOneHead() {
        assertEquals(OptionalLong.of(TEST_DATA.length), rangeReader.size());
        assertEquals(OptionalLong.of(TEST_DATA.length), rangeReader.size());
        wm.verify(1, headRequestedFor(urlEqualTo(TEST_PATH)));
    }

    @Test
    void sizeFallsBackToHeadWhenRangeResponseLacksContentRange() throws IOException {
        // Distinct path: none of the @BeforeEach TEST_PATH stubs can match it, which avoids the stub-precedence
        // question that range stubs added for TEST_PATH by other tests must consider.
        String path = "/no-content-range-on-range-response";
        byte[] responseBytes = Arrays.copyOfRange(TEST_DATA, 0, 100);

        // A real server violating the spec: 206 with a body but no Content-Range header.
        wm.stubFor(get(urlEqualTo(path))
                .withHeader("Range", equalTo("bytes=0-99"))
                .willReturn(aResponse()
                        .withStatus(206)
                        .withHeader("Content-Length", String.valueOf(responseBytes.length))
                        .withBody(responseBytes)));
        wm.stubFor(head(urlEqualTo(path))
                .willReturn(
                        aResponse().withStatus(200).withHeader("Content-Length", String.valueOf(TEST_DATA.length))));

        URI uri = URI.create("http://localhost:" + wm.getPort() + path);
        try (RangeReader reader = RangeReaderTestSupport.httpReader(uri)) {
            reader.readRange(0, 100, ByteBuffer.allocate(100));

            assertEquals(OptionalLong.of(TEST_DATA.length), reader.size());
            wm.verify(1, headRequestedFor(urlEqualTo(path)));
        }
    }

    @Test
    void testReadEntireFile() {
        // Stub the range request
        int start = 0;
        int end = TEST_DATA.length - 1;
        byte[] responseBytes = Arrays.copyOfRange(TEST_DATA, start, end + 1);

        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=" + start + "-" + end))
                .willReturn(aResponse()
                        .withStatus(206)
                        .withHeader("Content-Range", "bytes " + start + "-" + end + "/" + TEST_DATA.length)
                        .withHeader("Content-Length", String.valueOf(responseBytes.length))
                        .withBody(responseBytes)));

        ByteBuffer buffer = ByteBuffer.allocate(TEST_DATA.length);
        rangeReader.readRange(0, TEST_DATA.length, buffer);
        buffer.flip();

        assertEquals(TEST_DATA.length, buffer.remaining());

        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);

        // Verify first few and last few bytes match expected pattern
        for (int i = 0; i < 10; i++) {
            assertEquals((byte) (i % 256), bytes[i], "Byte mismatch at index " + i);
        }

        for (int i = TEST_DATA.length - 10; i < TEST_DATA.length; i++) {
            assertEquals((byte) (i % 256), bytes[i], "Byte mismatch at index " + i);
        }

        // Verify that range request was made
        wm.verify(getRequestedFor(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=0-" + (TEST_DATA.length - 1))));
    }

    @Test
    void testReadRange() {
        int offset = 1000;
        int length = 500;
        int end = offset + length - 1;

        // Create range response
        byte[] responseBytes = Arrays.copyOfRange(TEST_DATA, offset, offset + length);

        // Stub the range request
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=" + offset + "-" + end))
                .willReturn(aResponse()
                        .withStatus(206)
                        .withHeader("Content-Range", "bytes " + offset + "-" + end + "/" + TEST_DATA.length)
                        .withHeader("Content-Length", String.valueOf(responseBytes.length))
                        .withBody(responseBytes)));

        ByteBuffer buffer = ByteBuffer.allocate(length);
        rangeReader.readRange(offset, length, buffer);
        buffer.flip();

        assertEquals(length, buffer.remaining());

        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);

        // Verify that the bytes match expected pattern
        for (int i = 0; i < length; i++) {
            assertEquals((byte) ((offset + i) % 256), bytes[i], "Byte mismatch at index " + i);
        }

        // Verify that range request was made
        wm.verify(getRequestedFor(urlEqualTo(TEST_PATH)).withHeader("Range", equalTo("bytes=" + offset + "-" + end)));
    }

    @Test
    void testReadRangeFromEnd() {
        int offset = TEST_DATA.length - 500;
        int length = 500;
        int end = offset + length - 1;

        // Create range response
        byte[] responseBytes = Arrays.copyOfRange(TEST_DATA, offset, offset + length);

        // Stub the range request
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=" + offset + "-" + end))
                .willReturn(aResponse()
                        .withStatus(206)
                        .withHeader("Content-Range", "bytes " + offset + "-" + end + "/" + TEST_DATA.length)
                        .withHeader("Content-Length", String.valueOf(responseBytes.length))
                        .withBody(responseBytes)));

        ByteBuffer buffer = ByteBuffer.allocate(length);
        rangeReader.readRange(offset, length, buffer);
        buffer.flip();

        assertEquals(length, buffer.remaining());

        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);

        // Verify that the bytes match expected pattern
        for (int i = 0; i < length; i++) {
            assertEquals((byte) ((offset + i) % 256), bytes[i], "Byte mismatch at index " + i);
        }

        // Verify that range request was made
        wm.verify(getRequestedFor(urlEqualTo(TEST_PATH)).withHeader("Range", equalTo("bytes=" + offset + "-" + end)));
    }

    @Test
    void testReadRangeBeyondEnd() {
        // Set up a specific stub for this test
        int offset = TEST_DATA.length - 200;
        int length = 500; // This goes beyond the end of the data
        int end = TEST_DATA.length - 1; // Only returns to the end

        // Create range response - only including available data
        byte[] responseBytes = Arrays.copyOfRange(TEST_DATA, offset, TEST_DATA.length);

        // Stub the range request: the client asks for the full range past EOF, and the server
        // satisfies it with only the available bytes, like a real HTTP server would.
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=" + offset + "-" + (offset + length - 1)))
                .willReturn(aResponse()
                        .withStatus(206)
                        .withHeader("Content-Range", "bytes " + offset + "-" + end + "/" + TEST_DATA.length)
                        .withHeader("Content-Length", String.valueOf(responseBytes.length))
                        .withBody(responseBytes)));

        ByteBuffer buffer = ByteBuffer.allocate(length);
        rangeReader.readRange(offset, length, buffer);
        buffer.flip();

        // Should only get back 200 bytes (to the end of file)
        assertEquals(200, buffer.remaining());

        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);

        // Verify that the bytes match expected pattern
        for (int i = 0; i < 200; i++) {
            assertEquals((byte) ((offset + i) % 256), bytes[i], "Byte mismatch at index " + i);
        }

        // Verify that the client requested the full range as asked, without pre-truncating at EOF
        wm.verify(getRequestedFor(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=" + offset + "-" + (offset + length - 1))));
    }

    @Test
    void answerForOtherBytesThanRequestedIsAStorageError() {
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=50-149"))
                .willReturn(rangeAnswer(0, 100)));
        ByteBuffer target = ByteBuffer.allocate(100);

        assertThatThrownBy(() -> rangeReader.readRange(50, 100, target))
                .isInstanceOf(StorageException.class)
                .isNotInstanceOf(TransientStorageException.class)
                .hasMessageContaining("other bytes than requested");
    }

    /** RFC 9110 section 15.3.7 lets a server answer a subset of a range; the client asks again for the rest. */
    @Test
    void rangeAnsweredInPartIsCompletedWithAnotherGet() {
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=1000-1099"))
                .willReturn(rangeAnswer(1000, 50)));
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=1050-1099"))
                .willReturn(rangeAnswer(1050, 50)));
        ByteBuffer target = ByteBuffer.allocate(100);

        int read = rangeReader.readRange(1000, 100, target);

        assertThat(read).isEqualTo(100);
        assertThat(target.flip()).isEqualTo(ByteBuffer.wrap(TEST_DATA, 1000, 100));
        wm.verify(2, getRequestedFor(urlEqualTo(TEST_PATH)));
    }

    /**
     * Each row names the answer to a read of the range {@code 1000-1099} with its bytes {@code 1000-1049}, and the
     * answer to the GET of its rest from another version of the object. A 416 while the first answer tells that the
     * object goes on past 1050 means it shrank.
     */
    static Stream<Arguments> restAnswersFromAnotherVersion() {
        return Stream.of(
                Arguments.argumentSet(
                        "another ETag",
                        rangeAnswer(1000, 50).withHeader("ETag", "\"A\""),
                        rangeAnswer(1050, 50).withHeader("ETag", "\"B\"")),
                Arguments.argumentSet(
                        "another Last-Modified without ETag",
                        rangeAnswer(1000, 50).withHeader("Last-Modified", LAST_MODIFIED),
                        rangeAnswer(1050, 50).withHeader("Last-Modified", LATER_LAST_MODIFIED)),
                Arguments.argumentSet(
                        "another object size",
                        rangeAnswer(1000, 50),
                        rangeAnswerWithObjectSize(1050, 50, 2 * TEST_DATA.length)),
                Arguments.argumentSet(
                        "another ETag with an unknown object size",
                        rangeAnswerOfAnUnknownSize(1000, 50).withHeader("ETag", "\"A\""),
                        rangeAnswerOfAnUnknownSize(1050, 50).withHeader("ETag", "\"B\"")),
                Arguments.argumentSet(
                        "416 before the end of the object",
                        rangeAnswer(1000, 50),
                        aResponse().withStatus(416)));
    }

    /** One range read never returns bytes of two versions of the object. */
    @ParameterizedTest
    @MethodSource("restAnswersFromAnotherVersion")
    void restAnsweredFromAnotherVersionFailsTheRead(ResponseDefinitionBuilder first, ResponseDefinitionBuilder rest) {
        stubRangeThenRest(first, rest);
        ByteBuffer target = ByteBuffer.allocate(100);

        assertThatThrownBy(() -> rangeReader.readRange(1000, 100, target))
                .isInstanceOf(TransientStorageException.class)
                .hasMessageContaining("changed during the read");
        byte[] restOfTarget = Arrays.copyOfRange(target.array(), 50, 100);
        assertThat(restOfTarget).as("bytes of the rest in the target").isEqualTo(new byte[50]);
    }

    /** An answer without an ETag between two answers with different ETags hides no change of the object. */
    @Test
    void restAnsweredFromAnotherVersionAfterAnAnswerWithoutETagFailsTheRead() {
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=1000-1099"))
                .willReturn(rangeAnswer(1000, 30).withHeader("ETag", "\"A\"")));
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=1030-1099"))
                .willReturn(rangeAnswer(1030, 40)));
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=1070-1099"))
                .willReturn(rangeAnswer(1070, 30).withHeader("ETag", "\"B\"")));
        ByteBuffer target = ByteBuffer.allocate(100);

        assertThatThrownBy(() -> rangeReader.readRange(1000, 100, target))
                .isInstanceOf(TransientStorageException.class)
                .hasMessageContaining("changed during the read");
        wm.verify(3, getRequestedFor(urlEqualTo(TEST_PATH)));
    }

    /**
     * Each row names the answer to a read of the range {@code 1000-1099} and the answer to the GET of its rest from
     * 1050. One of them is rejected while telling another object size than the one reported by a HEAD.
     */
    static Stream<Arguments> rejectedAnswersTellingAnotherObjectSize() {
        int otherSize = 2 * TEST_DATA.length;
        ResponseDefinitionBuilder restBodyShorterThanItsContentRange = aResponse()
                .withStatus(206)
                .withHeader("Content-Range", "bytes 1050-1099/" + otherSize)
                .withBody(Arrays.copyOfRange(TEST_DATA, 1050, 1080));
        return Stream.of(
                Arguments.argumentSet(
                        "first answer with other bytes than requested",
                        rangeAnswerWithObjectSize(0, 100, otherSize),
                        rangeAnswer(1050, 50)),
                Arguments.argumentSet(
                        "rest from another version",
                        rangeAnswerOfAnUnknownSize(1000, 50).withHeader("ETag", "\"A\""),
                        rangeAnswerWithObjectSize(1050, 50, otherSize).withHeader("ETag", "\"B\"")),
                Arguments.argumentSet(
                        "rest with other bytes than requested",
                        rangeAnswerOfAnUnknownSize(1000, 50),
                        rangeAnswerWithObjectSize(0, 50, otherSize)),
                Arguments.argumentSet(
                        "rest with a body shorter than its Content-Range",
                        rangeAnswerOfAnUnknownSize(1000, 50),
                        restBodyShorterThanItsContentRange));
    }

    /** An answer failing the read leaves the object size to a HEAD. */
    @ParameterizedTest
    @MethodSource("rejectedAnswersTellingAnotherObjectSize")
    void rejectedAnswerLeavesTheObjectSizeUnknown(ResponseDefinitionBuilder first, ResponseDefinitionBuilder rest) {
        stubRangeThenRest(first, rest);
        ByteBuffer target = ByteBuffer.allocate(100);
        assertThatThrownBy(() -> rangeReader.readRange(1000, 100, target)).isInstanceOf(StorageException.class);

        OptionalLong size = rangeReader.size();

        assertThat(size).hasValue(TEST_DATA.length);
        wm.verify(1, headRequestedFor(urlEqualTo(TEST_PATH)));
    }

    /**
     * Each row names the answer to a read of the range {@code 1000-1099} with its bytes {@code 1000-1049}, and the
     * answer to the GET of its rest from the same version of the object. The ETag, when both answers have one, decides
     * over Last-Modified.
     */
    static Stream<Arguments> restAnswersFromTheSameVersion() {
        return Stream.of(
                Arguments.argumentSet(
                        "same ETag and object size",
                        rangeAnswer(1000, 50).withHeader("ETag", "\"A\""),
                        rangeAnswer(1050, 50).withHeader("ETag", "\"A\"")),
                Arguments.argumentSet(
                        "same weak ETag",
                        rangeAnswer(1000, 50).withHeader("ETag", "W/\"A\""),
                        rangeAnswer(1050, 50).withHeader("ETag", "W/\"A\"")),
                Arguments.argumentSet(
                        "same ETag and another Last-Modified",
                        rangeAnswer(1000, 50).withHeader("ETag", "\"A\"").withHeader("Last-Modified", LAST_MODIFIED),
                        rangeAnswer(1050, 50)
                                .withHeader("ETag", "\"A\"")
                                .withHeader("Last-Modified", LATER_LAST_MODIFIED)));
    }

    @ParameterizedTest
    @MethodSource("restAnswersFromTheSameVersion")
    void restAnsweredFromTheSameVersionCompletesTheRange(
            ResponseDefinitionBuilder first, ResponseDefinitionBuilder rest) {
        stubRangeThenRest(first, rest);
        ByteBuffer target = ByteBuffer.allocate(100);

        int read = rangeReader.readRange(1000, 100, target);

        assertThat(read).isEqualTo(100);
        assertThat(target.flip()).isEqualTo(ByteBuffer.wrap(TEST_DATA, 1000, 100));
    }

    /**
     * Stubs the range {@code 1000-1099} answered with {@code first}, and the GET of its rest from 1050 with
     * {@code rest}.
     */
    private void stubRangeThenRest(ResponseDefinitionBuilder first, ResponseDefinitionBuilder rest) {
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=1000-1099"))
                .willReturn(first));
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", equalTo("bytes=1050-1099"))
                .willReturn(rest));
    }

    /**
     * Each row names an answer telling that an object of unknown size ends before the rest of a range. A server may
     * ignore a range past the end of the object and answer the whole object.
     */
    static Stream<Arguments> answersToTheRestPastTheEnd() {
        byte[] objectEndingAtTheRest = Arrays.copyOfRange(TEST_DATA, 0, 1050);
        return Stream.of(
                Arguments.argumentSet("416", aResponse().withStatus(416)),
                Arguments.argumentSet("200 with the whole object", wholeObjectAnswer(objectEndingAtTheRest)),
                Arguments.argumentSet(
                        "200 with the whole object and its Content-Length",
                        wholeObjectAnswerWithItsLength(objectEndingAtTheRest)));
    }

    @ParameterizedTest
    @MethodSource("answersToTheRestPastTheEnd")
    void rangeAnsweredInPartWithAnUnknownSizeKeepsTheShortCountWhenTheObjectEndsBeforeTheRest(
            ResponseDefinitionBuilder rest) {
        stubRangeThenRest(rangeAnswerOfAnUnknownSize(1000, 50), rest);
        ByteBuffer target = ByteBuffer.allocate(100);

        int read = rangeReader.readRange(1000, 100, target);

        assertThat(read).isEqualTo(50);
        assertThat(target.flip()).isEqualTo(ByteBuffer.wrap(TEST_DATA, 1000, 50));
        wm.verify(1, getRequestedFor(urlEqualTo(TEST_PATH)).withHeader("Range", equalTo("bytes=1050-1099")));
        wm.verify(2, getRequestedFor(urlEqualTo(TEST_PATH)));
    }

    /** Each row names a first answer to the range {@code 1000-1099}, with or without the object size. */
    static Stream<Arguments> firstAnswersWithAndWithoutTheObjectSize() {
        return Stream.of(
                Arguments.argumentSet("known object size", rangeAnswer(1000, 50)),
                Arguments.argumentSet("unknown object size", rangeAnswerOfAnUnknownSize(1000, 50)));
    }

    /**
     * The object size or the Content-Length of the 200 tells that the object goes on past 1050: the server ignored the
     * range of the rest.
     */
    @ParameterizedTest
    @MethodSource("firstAnswersWithAndWithoutTheObjectSize")
    void restAnsweredWholeFailsTheReadOfAnObjectGoingOnPastTheRest(ResponseDefinitionBuilder first) {
        stubRangeThenRest(first, wholeObjectAnswerWithItsLength(TEST_DATA));
        ByteBuffer target = ByteBuffer.allocate(100);

        assertThatThrownBy(() -> rangeReader.readRange(1000, 100, target))
                .isInstanceOf(StorageException.class)
                .isNotInstanceOf(TransientStorageException.class)
                .hasMessageContaining("ignored the Range header");
        byte[] restOfTarget = Arrays.copyOfRange(target.array(), 50, 100);
        assertThat(restOfTarget).as("bytes of the rest in the target").isEqualTo(new byte[50]);
    }

    /** A 206 answering {@code length} bytes of {@link #TEST_DATA} from {@code offset}, as told by its Content-Range. */
    private static ResponseDefinitionBuilder rangeAnswer(int offset, int length) {
        return rangeAnswerWithObjectSize(offset, length, TEST_DATA.length);
    }

    /**
     * A 206 answering {@code length} bytes of {@link #TEST_DATA} from {@code offset}, with {@code objectSize} as the
     * total of its Content-Range.
     */
    private static ResponseDefinitionBuilder rangeAnswerWithObjectSize(int offset, int length, int objectSize) {
        int last = offset + length - 1;
        return aResponse()
                .withStatus(206)
                .withHeader("Content-Range", "bytes " + offset + "-" + last + "/" + objectSize)
                .withBody(Arrays.copyOfRange(TEST_DATA, offset, offset + length));
    }

    /**
     * A 206 answering {@code length} bytes of {@link #TEST_DATA} from {@code offset}, with {@code *} as the total of
     * its Content-Range.
     */
    private static ResponseDefinitionBuilder rangeAnswerOfAnUnknownSize(int offset, int length) {
        int last = offset + length - 1;
        return aResponse()
                .withStatus(206)
                .withHeader("Content-Range", "bytes " + offset + "-" + last + "/*")
                .withBody(Arrays.copyOfRange(TEST_DATA, offset, offset + length));
    }

    /** A 200 answering all of {@code object}, as sent by a server ignoring the range of a GET. */
    private static ResponseDefinitionBuilder wholeObjectAnswer(byte[] object) {
        return aResponse().withStatus(200).withBody(object);
    }

    /** A {@link #wholeObjectAnswer(byte[])} declaring the length of {@code object} in its Content-Length. */
    private static ResponseDefinitionBuilder wholeObjectAnswerWithItsLength(byte[] object) {
        return wholeObjectAnswer(object).withHeader("Content-Length", String.valueOf(object.length));
    }

    @Test
    void testReadZeroLength() {
        int offset = 100;
        int length = 0;

        // The implementation now returns an empty buffer for zero-length requests without making HTTP requests
        ByteBuffer buffer = ByteBuffer.allocate(length);
        rangeReader.readRange(offset, length, buffer);
        buffer.flip();

        assertEquals(0, buffer.remaining());

        // No HTTP request should have been made
        wm.verify(0, getRequestedFor(urlEqualTo(TEST_PATH)).withHeader("Range", matching(".*")));
    }

    @Test
    void readPastEofReturnsZeroOn416() {
        wm.stubFor(get(urlEqualTo(TEST_PATH))
                .withHeader("Range", matching("bytes=200000-.*"))
                .willReturn(aResponse().withStatus(416).withHeader("Content-Range", "bytes */" + TEST_DATA.length)));

        ByteBuffer target = ByteBuffer.allocate(100);
        int read = rangeReader.readRange(200_000, 100, target);

        assertEquals(0, read);
        assertEquals(0, target.position());
    }

    @Test
    void testReadWithNegativeOffset() {
        ByteBuffer buff = ByteBuffer.allocate(1);
        assertThatThrownBy(() -> rangeReader.readRange(-1, 10, buff)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testReadWithNegativeLength() {
        ByteBuffer buff = ByteBuffer.allocate(1);
        assertThatThrownBy(() -> rangeReader.readRange(0, -1, buff)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rangeIgnoringServerFails() {
        wm.stubFor(get(urlEqualTo("/ignores-range"))
                .willReturn(aResponse().withStatus(200).withBody(TEST_DATA)));
        RangeReader ignoring =
                RangeReaderTestSupport.httpReader(URI.create(wm.getRuntimeInfo().getHttpBaseUrl() + "/ignores-range"));

        assertThatThrownBy(() -> ignoring.readRange(0, 100))
                .isInstanceOf(io.tileverse.storage.StorageException.class)
                .hasMessageContaining("Range");
    }

    @Test
    void missingObjectThrowsNotFoundOnFirstRead() {
        wm.stubFor(get(urlEqualTo("/missing")).willReturn(aResponse().withStatus(404)));
        RangeReader missing =
                RangeReaderTestSupport.httpReader(URI.create(wm.getRuntimeInfo().getHttpBaseUrl() + "/missing"));

        assertThatThrownBy(() -> missing.readRange(0, 100)).isInstanceOf(io.tileverse.storage.NotFoundException.class);
    }

    @Test
    void testServerReturningEntireFileForRangeRequest() throws IOException {
        // Request parameters
        int offset = 1000;
        int length = 200;
        int end = offset + length - 1;

        // Create a stub for a server that ignores range requests and returns the whole file
        wm.stubFor(get(urlEqualTo("/ignore-range"))
                .withHeader("Range", equalTo("bytes=" + offset + "-" + end))
                .willReturn(aResponse()
                        .withStatus(200) // Not 206 - indicates full file, not partial content
                        .withHeader("Content-Length", String.valueOf(TEST_DATA.length))
                        .withBody(TEST_DATA)));

        URI ignoreRangeUri = URI.create("http://localhost:" + wm.getPort() + "/ignore-range");

        // Should throw StorageException when server doesn't support range requests (returns 200 instead of 206)
        try (RangeReader ignoreRangeReader = RangeReaderTestSupport.httpReader(ignoreRangeUri)) {
            assertThatThrownBy(() -> ignoreRangeReader.readRange(offset, length, ByteBuffer.allocate(length)))
                    .isInstanceOf(io.tileverse.storage.StorageException.class);
        }
    }

    @Test
    void testServerReturningErrorForRangeRequest() throws IOException {
        // Specific request parameters
        int offset = 0;
        int length = 100;
        int end = offset + length - 1;

        // Create a stub for a server that returns an error for the actual range request.
        // 416 is excluded here: it is a past-EOF signal handled elsewhere, not a generic error.
        wm.stubFor(get(urlEqualTo("/error"))
                .withHeader("Range", equalTo("bytes=" + offset + "-" + end))
                .willReturn(aResponse().withStatus(500).withBody("Internal Server Error")));

        URI errorUri = URI.create("http://localhost:" + wm.getPort() + "/error");

        // Should be able to create the reader
        try (RangeReader errorReader = RangeReaderTestSupport.httpReader(errorUri)) {
            // But reading a range should throw
            assertThatThrownBy(() -> errorReader.readRange(offset, length, ByteBuffer.allocate(length)))
                    .isInstanceOf(io.tileverse.storage.StorageException.class);

            // Verify that range request was made
            wm.verify(
                    getRequestedFor(urlEqualTo("/error")).withHeader("Range", equalTo("bytes=" + offset + "-" + end)));
        }
    }

    @Test
    void testConcurrentRangeRequests() throws Exception {
        int numThreads = 10;
        int regionSize = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        AtomicBoolean failure = new AtomicBoolean(false);
        CountDownLatch startLatch = new CountDownLatch(1);

        try {
            // Create futures for concurrent execution
            Future<?>[] futures = new Future<?>[numThreads];

            // Set up stubs for all possible range requests
            for (int i = 0; i < numThreads; i++) {
                final int regionStart = i * regionSize;
                final int regionEnd = regionStart + regionSize - 1;

                // Create range response for this region
                byte[] responseBytes = Arrays.copyOfRange(TEST_DATA, regionStart, regionStart + regionSize);

                // Stub the range request for this region
                wm.stubFor(get(urlEqualTo(TEST_PATH))
                        .withHeader("Range", equalTo("bytes=" + regionStart + "-" + regionEnd))
                        .willReturn(aResponse()
                                .withStatus(206)
                                .withHeader(
                                        "Content-Range",
                                        "bytes " + regionStart + "-" + regionEnd + "/" + TEST_DATA.length)
                                .withHeader("Content-Length", String.valueOf(responseBytes.length))
                                .withBody(responseBytes)));
            }

            // Submit tasks for concurrent execution
            for (int i = 0; i < numThreads; i++) {
                final int threadIndex = i;
                final int regionStart = threadIndex * regionSize;

                futures[i] = executor.submit(() -> {
                    try {
                        startLatch.await(); // Wait for all threads to be ready

                        // Read a region
                        ByteBuffer buffer = ByteBuffer.allocate(regionSize);
                        rangeReader.readRange(regionStart, regionSize, buffer);
                        buffer.flip();
                        byte[] data = new byte[buffer.remaining()];
                        buffer.get(data);

                        // Verify data is correct
                        for (int j = 0; j < data.length; j++) {
                            if (data[j] != (byte) ((regionStart + j) % 256)) {
                                log.debug(
                                        "Thread {}: Data mismatch at index {}, expected {} but got {}",
                                        threadIndex,
                                        j,
                                        (regionStart + j) % 256,
                                        data[j] & 0xFF);
                                failure.set(true);
                                return;
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        failure.set(true);
                    }
                });
            }

            // Start all threads at once
            startLatch.countDown();

            // Wait for completion and check for failures
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);

            assertFalse(failure.get(), "One or more threads encountered errors during concurrent reads");

            // Verify that all range requests were made
            for (int i = 0; i < numThreads; i++) {
                final int regionStart = i * regionSize;
                final int regionEnd = regionStart + regionSize - 1;

                wm.verify(getRequestedFor(urlEqualTo(TEST_PATH))
                        .withHeader("Range", equalTo("bytes=" + regionStart + "-" + regionEnd)));
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testMissingContentLengthHeader() throws IOException {
        // Test behavior when Content-Length header is missing
        wm.stubFor(head(urlEqualTo("/no-content-length"))
                .willReturn(
                        aResponse().withStatus(200).withHeader("Accept-Ranges", "bytes")
                        // No Content-Length header
                        ));

        URI noContentLengthUri = URI.create("http://localhost:" + wm.getPort() + "/no-content-length");

        try (RangeReader reader = RangeReaderTestSupport.httpReader(noContentLengthUri)) {
            assertThat(reader.size()).isEmpty();
        }
    }

    @Test
    void testInvalidContentLengthHeader() {
        // Test behavior when Content-Length header is invalid
        // Let's use a simpler approach by creating a mock HttpRangeReader that throws on size()

        // Create a valid response for the HEAD request during construction
        wm.stubFor(head(urlEqualTo("/invalid-content-length"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Accept-Ranges", "bytes")
                        .withHeader("Content-Length", "-1")));

        URI invalidContentLengthUri = URI.create("http://localhost:" + wm.getPort() + "/invalid-content-length");

        RangeReader reader = RangeReaderTestSupport.httpReader(invalidContentLengthUri);

        assertThat(reader.size()).isEmpty();
    }

    @Test
    void testConnectionFailure() {
        // Test behavior when connection fails
        URI nonExistentUri = URI.create("http://non-existent-host.example/test.pmtiles");

        // Should throw when size() is called (triggering initialization)
        RangeReader reader = RangeReaderTestSupport.httpReader(nonExistentUri);
        assertThatThrownBy(reader::size).isInstanceOf(io.tileverse.storage.TransientStorageException.class);
    }

    @Test
    void testServerErrorOnHead() {
        // Test behavior when server returns error on HEAD
        wm.stubFor(head(urlEqualTo("/server-error"))
                .willReturn(aResponse().withStatus(500).withBody("Internal Server Error")));

        URI serverErrorUri = URI.create("http://localhost:" + wm.getPort() + "/server-error");

        // Should throw when size() is called (triggering initialization)
        RangeReader reader = RangeReaderTestSupport.httpReader(serverErrorUri);
        assertThatThrownBy(reader::size)
                .isInstanceOf(io.tileverse.storage.StorageException.class)
                .hasMessageContaining("Failed to connect");
    }

    @Test
    void openRangeReaderUriPreservesQueryStringOnTheWire() throws IOException {
        // Pre-signed URL shape: the query string is the authorization. The Storage's URI overload must preserve it
        // into the resolved leaf URL so the request actually carries the signature.
        String pathWithoutQuery = "/signed";
        wm.stubFor(head(urlEqualTo(pathWithoutQuery + "?signature=abc123&expires=42"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Length", String.valueOf(TEST_DATA.length))
                        .withHeader("Accept-Ranges", "bytes")));

        URI signedUri =
                URI.create("http://localhost:" + wm.getPort() + pathWithoutQuery + "?signature=abc123&expires=42");
        URI parent = signedUri.resolve(".");
        try (io.tileverse.storage.Storage storage = io.tileverse.storage.http.HttpStorageProvider.open(
                        parent, java.net.http.HttpClient.newHttpClient());
                RangeReader r = storage.openRangeReader(signedUri)) {
            // size() triggers the HEAD that the stub above matches on the FULL URL including query params
            assertThat(r.size()).hasValue((long) TEST_DATA.length);
        }

        // Verify the HEAD request actually had the query string on the wire.
        wm.verify(headRequestedFor(urlEqualTo(pathWithoutQuery + "?signature=abc123&expires=42")));
    }
}
