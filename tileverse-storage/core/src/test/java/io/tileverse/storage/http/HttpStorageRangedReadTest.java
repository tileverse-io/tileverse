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
import static com.github.tomakehurst.wiremock.client.WireMock.absent;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import io.tileverse.storage.ReadHandle;
import io.tileverse.storage.ReadOptions;
import io.tileverse.storage.StorageException;
import io.tileverse.storage.TransientStorageException;
import io.tileverse.storage.batch.BatchSettings;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Optional;
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
 * Tests of {@link HttpStorage#read(String, ReadOptions)} checking the status of each answer, checking its bytes against
 * the range, and completing a range answered in part.
 */
class HttpStorageRangedReadTest {

    private static final String KEY = "data.bin";
    private static final String PATH = "/" + KEY;
    private static final byte[] DATA = createData(100_000);
    private static final String MOVED_PATH = "/moved/" + KEY;
    private static final byte[] REDIRECT_PAGE =
            ("<html><body>Moved to " + MOVED_PATH + "</body></html>").getBytes(StandardCharsets.US_ASCII);
    private static final Instant LAST_SEEN = Instant.parse("2026-09-29T10:00:00Z");
    private static final String LAST_MODIFIED = "Tue, 29 Sep 2026 10:00:00 GMT";
    private static final String LATER_LAST_MODIFIED = "Tue, 29 Sep 2026 11:00:00 GMT";

    // Loopback only: a wildcard socket may share its port with an IPv4 listener of another process, and that
    // listener then receives the requests.
    @RegisterExtension
    WireMockExtension wm = WireMockExtension.newInstance()
            .options(wireMockConfig().dynamicPort().bindAddress("127.0.0.1"))
            .build();

    private HttpStorage storage;

    private static byte[] createData(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) (i % 251);
        }
        return data;
    }

    @BeforeEach
    void openStorage() {
        URI baseUri = URI.create("http://127.0.0.1:" + wm.getPort() + "/");
        storage = storageAt(baseUri);
    }

    private static HttpStorage storageAt(URI baseUri) {
        HttpClientHandle client = new BorrowedHttpHandle(HttpClient.newHttpClient());
        return new HttpStorage(baseUri, client, HttpAuthentication.NONE, BatchSettings.httpDefaults());
    }

    @AfterEach
    void closeStorage() {
        storage.close();
    }

    @Test
    void rangedReadAnsweredWithTheWholeObjectIsAStorageError() {
        wm.stubFor(get(urlEqualTo(PATH))
                .withHeader("Range", equalTo("bytes=1000-1099"))
                .willReturn(aResponse().withStatus(200).withBody(DATA)));
        ReadOptions range = ReadOptions.range(1000, 100);

        assertThatThrownBy(() -> storage.read(KEY, range))
                .isInstanceOf(StorageException.class)
                .isNotInstanceOf(TransientStorageException.class)
                .hasMessageContaining("ignored the Range header");
    }

    /**
     * Each row names an answer of {@code length} bytes from {@code offset} to a read of the range {@code 1000-1099}.
     */
    static Stream<Arguments> answersForOtherBytes() {
        return Stream.of(
                Arguments.argumentSet("starting before the range", 900, 100),
                Arguments.argumentSet("starting inside the range", 1050, 50),
                Arguments.argumentSet("ending past the range", 1000, 200));
    }

    @ParameterizedTest
    @MethodSource("answersForOtherBytes")
    void answerForOtherBytesThanRequestedIsAStorageError(int offset, int length) {
        wm.stubFor(get(urlEqualTo(PATH))
                .withHeader("Range", equalTo("bytes=1000-1099"))
                .willReturn(rangeAnswer(offset, length)));
        ReadOptions range = ReadOptions.range(1000, 100);

        assertThatThrownBy(() -> storage.read(KEY, range))
                .isInstanceOf(StorageException.class)
                .isNotInstanceOf(TransientStorageException.class)
                .hasMessageContaining("other bytes than requested");
    }

    /**
     * Each row names an answer with a status other than 200 to a read of the whole object, or other than 206 to a
     * ranged read.
     */
    static Stream<Arguments> answersWithAWrongStatus() {
        ReadOptions wholeObject = ReadOptions.defaults();
        ReadOptions range = ReadOptions.range(1000, 100);
        ReadOptions fromOffset = ReadOptions.fromOffset(1000);
        return Stream.of(
                Arguments.argumentSet("redirect to a read of the whole object", redirect(), wholeObject),
                Arguments.argumentSet(
                        "304 to a conditional read of the whole object",
                        aResponse().withStatus(304),
                        modifiedSince(wholeObject)),
                Arguments.argumentSet(
                        "204 to a read of the whole object", aResponse().withStatus(204), wholeObject),
                Arguments.argumentSet("206 to a read of the whole object", rangeAnswer(0, DATA.length), wholeObject),
                Arguments.argumentSet("redirect to a ranged read", redirect(), range),
                Arguments.argumentSet("redirect to a read from an offset", redirect(), fromOffset),
                Arguments.argumentSet(
                        "304 to a conditional ranged read", aResponse().withStatus(304), modifiedSince(range)));
    }

    /**
     * Neither the production client nor the client of this test follows redirects; a 304 or 204 has no content, and a
     * 206 answers a range.
     */
    @ParameterizedTest
    @MethodSource("answersWithAWrongStatus")
    void answerWithAWrongStatusIsAStorageError(ResponseDefinitionBuilder answer, ReadOptions options) {
        wm.stubFor(get(urlEqualTo(PATH)).willReturn(answer));
        int status = answer.build().getStatus();

        assertThatThrownBy(() -> storage.read(KEY, options))
                .isExactlyInstanceOf(StorageException.class)
                .hasMessageEndingWith(": " + status);
    }

    @Test
    void redirectIsReportedAsNotFollowed() {
        wm.stubFor(get(urlEqualTo(PATH)).willReturn(redirect()));
        ReadOptions wholeObject = ReadOptions.defaults();

        assertThatThrownBy(() -> storage.read(KEY, wholeObject)).hasMessageStartingWith("Redirect not followed");
    }

    /** Each row names a way to read the content of a read of the range {@code 1000-1099}. */
    static Stream<Arguments> readsOfTheRange() {
        ContentRead allBytes = InputStream::readAllBytes;
        ContentRead rangeLength = content -> content.readNBytes(100);
        ContentRead byteByByte = HttpStorageRangedReadTest::readEachByte;
        return Stream.of(
                Arguments.argumentSet("all bytes", allBytes),
                Arguments.argumentSet("the length of the range", rangeLength),
                Arguments.argumentSet("byte by byte", byteByByte));
    }

    /** The read of the last byte of the range also checks that the body ends there. */
    @ParameterizedTest
    @MethodSource("readsOfTheRange")
    void rangeAnsweredWholeIsReadWithOneGet(ContentRead read) throws IOException {
        wm.stubFor(get(urlEqualTo(PATH))
                .withHeader("Range", equalTo("bytes=1000-1099"))
                .willReturn(rangeAnswer(1000, 100)));

        byte[] content = readWith(read, ReadOptions.range(1000, 100));

        assertThat(content).isEqualTo(Arrays.copyOfRange(DATA, 1000, 1100));
        wm.verify(1, getRequestedFor(urlEqualTo(PATH)));
    }

    @Test
    void aClosedStreamOfARangeAnsweredWholeRefusesReads() throws IOException {
        wm.stubFor(get(urlEqualTo(PATH))
                .withHeader("Range", equalTo("bytes=1000-1099"))
                .willReturn(rangeAnswer(1000, 100)));
        ReadHandle handle = storage.read(KEY, ReadOptions.range(1000, 100));
        InputStream content = handle.content();
        content.skipNBytes(100);

        handle.close();

        assertThatThrownBy(content::read).isInstanceOf(IOException.class).hasMessageContaining("closed");
    }

    /** RFC 9110 section 15.3.7 lets a server answer a subset of a range; the read asks again for the rest. */
    @Test
    void rangeAnsweredInPartIsCompletedWithAsManyGetsAsNeeded() throws IOException {
        wm.stubFor(get(urlEqualTo(PATH))
                .withHeader("Range", equalTo("bytes=1000-1099"))
                .willReturn(rangeAnswer(1000, 30)));
        wm.stubFor(get(urlEqualTo(PATH))
                .withHeader("Range", equalTo("bytes=1030-1099"))
                .willReturn(rangeAnswer(1030, 40)));
        wm.stubFor(get(urlEqualTo(PATH))
                .withHeader("Range", equalTo("bytes=1070-1099"))
                .willReturn(rangeAnswer(1070, 30)));

        byte[] content = readAll(ReadOptions.range(1000, 100));

        assertThat(content).isEqualTo(Arrays.copyOfRange(DATA, 1000, 1100));
        wm.verify(3, getRequestedFor(urlEqualTo(PATH)));
    }

    @Test
    void aClosedCompletingStreamRefusesReads() throws IOException {
        wm.stubFor(get(urlEqualTo(PATH))
                .withHeader("Range", equalTo("bytes=1000-1099"))
                .willReturn(rangeAnswer(1000, 30)));
        ReadHandle handle = storage.read(KEY, ReadOptions.range(1000, 100));
        InputStream content = handle.content();

        handle.close();

        assertThatThrownBy(content::read).isInstanceOf(IOException.class).hasMessageContaining("closed");
    }

    @Test
    void rangeAnsweredInPartIsCompletedForSingleByteReads() throws IOException {
        wm.stubFor(get(urlEqualTo(PATH))
                .withHeader("Range", equalTo("bytes=1000-1099"))
                .willReturn(rangeAnswer(1000, 50)));
        wm.stubFor(get(urlEqualTo(PATH))
                .withHeader("Range", equalTo("bytes=1050-1099"))
                .willReturn(rangeAnswer(1050, 50)));

        byte[] content = readByteByByte(ReadOptions.range(1000, 100));

        assertThat(content).isEqualTo(Arrays.copyOfRange(DATA, 1000, 1100));
    }

    /** A read to the end of the object asks for the rest to the end, and stops at the object size. */
    @Test
    void readFromAnOffsetAnsweredInPartIsCompletedToTheEndOfTheObject() throws IOException {
        wm.stubFor(get(urlEqualTo(PATH))
                .withHeader("Range", equalTo("bytes=99900-"))
                .willReturn(rangeAnswer(99_900, 50)));
        wm.stubFor(get(urlEqualTo(PATH))
                .withHeader("Range", equalTo("bytes=99950-"))
                .willReturn(rangeAnswer(99_950, 50)));

        byte[] content = readAll(ReadOptions.fromOffset(99_900));

        assertThat(content).isEqualTo(Arrays.copyOfRange(DATA, 99_900, 100_000));
        wm.verify(2, getRequestedFor(urlEqualTo(PATH)));
    }

    /**
     * Each row names an answer telling that an object of unknown size ends before the rest of a range. A server may
     * ignore a range past the end of the object and answer the whole object.
     */
    static Stream<Arguments> answersToTheRestPastTheEnd() {
        return Stream.of(
                Arguments.argumentSet("416", aResponse().withStatus(416)),
                Arguments.argumentSet("200 with the whole object", wholeObjectAnswer()),
                Arguments.argumentSet(
                        "200 with the whole object and its Content-Length", wholeObjectAnswerWithItsLength()));
    }

    @ParameterizedTest
    @MethodSource("answersToTheRestPastTheEnd")
    void rangeAnsweredInPartWithAnUnknownSizeEndsWithTheObject(ResponseDefinitionBuilder rest) throws IOException {
        stubEndOfAnObjectOfUnknownSize("bytes=99950-100049", "bytes=100000-100049", rest);

        byte[] content = readAll(ReadOptions.range(99_950, 100));

        assertThat(content).isEqualTo(Arrays.copyOfRange(DATA, 99_950, 100_000));
        wm.verify(1, getRequestedFor(urlEqualTo(PATH)).withHeader("Range", equalTo("bytes=100000-100049")));
    }

    @ParameterizedTest
    @MethodSource("answersToTheRestPastTheEnd")
    void rangeAnsweredInPartWithAnUnknownSizeEndsWithTheObjectForSingleByteReads(ResponseDefinitionBuilder rest)
            throws IOException {
        stubEndOfAnObjectOfUnknownSize("bytes=99950-100049", "bytes=100000-100049", rest);

        byte[] content = readByteByByte(ReadOptions.range(99_950, 100));

        assertThat(content).isEqualTo(Arrays.copyOfRange(DATA, 99_950, 100_000));
    }

    /** A read to the end of an object of unknown size can only end with the answer to the GET of the rest. */
    @ParameterizedTest
    @MethodSource("answersToTheRestPastTheEnd")
    void readFromAnOffsetAnsweredInPartWithAnUnknownSizeEndsWithTheObject(ResponseDefinitionBuilder rest)
            throws IOException {
        stubEndOfAnObjectOfUnknownSize("bytes=99950-", "bytes=100000-", rest);

        byte[] content = readAll(ReadOptions.fromOffset(99_950));

        assertThat(content).isEqualTo(Arrays.copyOfRange(DATA, 99_950, 100_000));
        wm.verify(1, getRequestedFor(urlEqualTo(PATH)).withHeader("Range", equalTo("bytes=100000-")));
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
    void restAnsweredWholeFailsTheReadOfAnObjectGoingOnPastTheRest(ResponseDefinitionBuilder first) throws IOException {
        stubRangeThenRest(first, wholeObjectAnswerWithItsLength());

        try (ReadHandle handle = storage.read(KEY, ReadOptions.range(1000, 100))) {
            InputStream content = handle.content();

            assertThatThrownBy(content::readAllBytes)
                    .isInstanceOf(IOException.class)
                    .hasCauseExactlyInstanceOf(StorageException.class)
                    .hasMessageContaining("ignored the Range header");
        }
    }

    /** A server answering the rest with no bytes would otherwise be asked for it again and again. */
    @Test
    @Timeout(30)
    void restAnsweredWithNoByteEndsTheRead() throws IOException {
        wm.stubFor(get(urlEqualTo(PATH))
                .withHeader("Range", equalTo("bytes=1000-1099"))
                .willReturn(rangeAnswer(1000, 50)));
        wm.stubFor(get(urlEqualTo(PATH))
                .withHeader("Range", equalTo("bytes=1050-1099"))
                .willReturn(aResponse().withStatus(206)));

        byte[] content = readAll(ReadOptions.range(1000, 100));

        assertThat(content).isEqualTo(Arrays.copyOfRange(DATA, 1000, 1050));
        wm.verify(1, getRequestedFor(urlEqualTo(PATH)).withHeader("Range", equalTo("bytes=1050-1099")));
    }

    @Test
    void restAnsweredWithOtherBytesFailsTheRead() throws IOException {
        wm.stubFor(get(urlEqualTo(PATH))
                .withHeader("Range", equalTo("bytes=1000-1099"))
                .willReturn(rangeAnswer(1000, 50)));
        wm.stubFor(get(urlEqualTo(PATH))
                .withHeader("Range", equalTo("bytes=1050-1099"))
                .willReturn(rangeAnswer(0, 50)));

        try (ReadHandle handle = storage.read(KEY, ReadOptions.range(1000, 100))) {
            InputStream content = handle.content();

            assertThatThrownBy(content::readAllBytes)
                    .isInstanceOf(IOException.class)
                    .hasCauseInstanceOf(StorageException.class)
                    .hasMessageContaining("other bytes than requested");
        }
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
                        rangeAnswerWithObjectSize(1050, 50, 2 * DATA.length)),
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
    void restAnsweredFromAnotherVersionFailsTheRead(ResponseDefinitionBuilder first, ResponseDefinitionBuilder rest)
            throws IOException {
        stubRangeThenRest(first, rest);

        assertReadFailsOnAChangedObject(ReadOptions.range(1000, 100));
    }

    /** An answer without an ETag between two answers with different ETags hides no change of the object. */
    @Test
    void restAnsweredFromAnotherVersionAfterAnAnswerWithoutETagFailsTheRead() throws IOException {
        wm.stubFor(get(urlEqualTo(PATH))
                .withHeader("Range", equalTo("bytes=1000-1099"))
                .willReturn(rangeAnswer(1000, 30).withHeader("ETag", "\"A\"")));
        wm.stubFor(get(urlEqualTo(PATH))
                .withHeader("Range", equalTo("bytes=1030-1099"))
                .willReturn(rangeAnswer(1030, 40)));
        wm.stubFor(get(urlEqualTo(PATH))
                .withHeader("Range", equalTo("bytes=1070-1099"))
                .willReturn(rangeAnswer(1070, 30).withHeader("ETag", "\"B\"")));

        assertReadFailsOnAChangedObject(ReadOptions.range(1000, 100));
        wm.verify(3, getRequestedFor(urlEqualTo(PATH)));
    }

    private void assertReadFailsOnAChangedObject(ReadOptions options) throws IOException {
        try (ReadHandle handle = storage.read(KEY, options)) {
            InputStream content = handle.content();

            assertThatThrownBy(content::readAllBytes)
                    .isInstanceOf(IOException.class)
                    .hasCauseExactlyInstanceOf(TransientStorageException.class)
                    .hasMessageContaining("changed during the read");
        }
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
            ResponseDefinitionBuilder first, ResponseDefinitionBuilder rest) throws IOException {
        stubRangeThenRest(first, rest);

        byte[] content = readAll(ReadOptions.range(1000, 100));

        assertThat(content).isEqualTo(Arrays.copyOfRange(DATA, 1000, 1100));
        wm.verify(2, getRequestedFor(urlEqualTo(PATH)));
    }

    /**
     * Each row names an answer with a status other than 206 to the GET of the rest of the range {@code 1000-1099}, and
     * the options of the read.
     */
    static Stream<Arguments> answersToTheRestWithAWrongStatus() {
        ReadOptions range = ReadOptions.range(1000, 100);
        return Stream.of(
                Arguments.argumentSet("redirect", redirect(), range),
                Arguments.argumentSet("304 to a conditional read", aResponse().withStatus(304), modifiedSince(range)));
    }

    @ParameterizedTest
    @MethodSource("answersToTheRestWithAWrongStatus")
    void restAnsweredWithAWrongStatusFailsTheRead(ResponseDefinitionBuilder answer, ReadOptions options)
            throws IOException {
        stubRangeAnsweredInPartThenRestWith(answer);
        int status = answer.build().getStatus();

        try (ReadHandle handle = storage.read(KEY, options)) {
            InputStream content = handle.content();

            assertThatThrownBy(content::readAllBytes)
                    .isInstanceOf(IOException.class)
                    .hasCauseExactlyInstanceOf(StorageException.class)
                    .hasMessageEndingWith(": " + status);
        }
    }

    @Test
    void restAnsweredWithARedirectFailsASingleByteRead() {
        stubRangeAnsweredInPartThenRestWith(redirect());
        ReadOptions range = ReadOptions.range(1000, 100);

        assertThatThrownBy(() -> readByteByByte(range))
                .isInstanceOf(IOException.class)
                .hasCauseExactlyInstanceOf(StorageException.class)
                .hasMessageEndingWith(": 302");
    }

    /**
     * The JDK client reads the close of a body without Content-Length as its end; only the Content-Range shows the
     * missing bytes.
     */
    @Test
    @Timeout(30)
    void rangeCutShortByAClosedConnectionFailsTheRead() throws IOException {
        try (CutShortServer server = CutShortServer.start();
                HttpStorage cutShortStorage = storageAt(server.baseUri());
                ReadHandle handle = cutShortStorage.read(KEY, ReadOptions.range(1000, 100))) {
            InputStream content = handle.content();

            assertThatThrownBy(content::readAllBytes)
                    .isInstanceOf(IOException.class)
                    .hasCauseExactlyInstanceOf(TransientStorageException.class)
                    .hasMessageContaining("Response body and Content-Range disagree");
        }
    }

    /**
     * Each row names the answer to a read of the range {@code 1000-1099} and the answer to the GET of its rest from
     * {@code 1050}, one of them with a body other than the bytes named by its Content-Range. The GET of the rest goes
     * out only after a first answer of the bytes {@code 1000-1049}.
     */
    static Stream<Arguments> answersWithABodyOtherThanTheirContentRange() {
        Arguments restWithNoBody = Arguments.argumentSet(
                "rest with no body", rangeAnswer(1000, 50), rangeAnswerWithBodyLength(1050, 50, 0));
        return Stream.concat(Stream.of(restWithNoBody), answersWithABodyLongerThanTheirContentRange());
    }

    /** The rows of {@link #answersWithABodyOtherThanTheirContentRange()} with a body longer than its Content-Range. */
    static Stream<Arguments> answersWithABodyLongerThanTheirContentRange() {
        return Stream.of(
                Arguments.argumentSet(
                        "whole range with a longer body",
                        rangeAnswerWithBodyLength(1000, 100, 150),
                        rangeAnswer(1050, 50)),
                Arguments.argumentSet(
                        "part of the range with a longer body",
                        rangeAnswerWithBodyLength(1000, 30, 150),
                        rangeAnswer(1050, 50)),
                Arguments.argumentSet(
                        "rest with a longer body", rangeAnswer(1000, 50), rangeAnswerWithBodyLength(1050, 50, 100)));
    }

    @ParameterizedTest
    @MethodSource("answersWithABodyOtherThanTheirContentRange")
    void bodyOtherThanItsContentRangeFailsTheRead(ResponseDefinitionBuilder first, ResponseDefinitionBuilder rest)
            throws IOException {
        stubRangeThenRest(first, rest);

        try (ReadHandle handle = storage.read(KEY, ReadOptions.range(1000, 100))) {
            InputStream content = handle.content();

            assertThatThrownBy(content::readAllBytes)
                    .isInstanceOf(IOException.class)
                    .hasCauseExactlyInstanceOf(TransientStorageException.class)
                    .hasMessageContaining("Response body and Content-Range disagree");
        }
    }

    @ParameterizedTest
    @MethodSource("answersWithABodyOtherThanTheirContentRange")
    void bodyOtherThanItsContentRangeFailsSingleByteReads(
            ResponseDefinitionBuilder first, ResponseDefinitionBuilder rest) {
        stubRangeThenRest(first, rest);
        ReadOptions range = ReadOptions.range(1000, 100);

        assertThatThrownBy(() -> readByteByByte(range))
                .isInstanceOf(IOException.class)
                .hasCauseExactlyInstanceOf(TransientStorageException.class)
                .hasMessageContaining("Response body and Content-Range disagree");
    }

    /** A caller reading exactly the length of the range never reads past it: the read reaching its end fails. */
    @ParameterizedTest
    @MethodSource("answersWithABodyLongerThanTheirContentRange")
    void bodyLongerThanItsContentRangeFailsAReadOfTheRangeLength(
            ResponseDefinitionBuilder first, ResponseDefinitionBuilder rest) throws IOException {
        stubRangeThenRest(first, rest);

        try (ReadHandle handle = storage.read(KEY, ReadOptions.range(1000, 100))) {
            InputStream content = handle.content();

            assertThatThrownBy(() -> content.readNBytes(100))
                    .isInstanceOf(IOException.class)
                    .hasCauseExactlyInstanceOf(TransientStorageException.class)
                    .hasMessageContaining("Response body and Content-Range disagree");
        }
    }

    /** The bytes past the Content-Range never reach the buffer of the caller, even before the read fails. */
    @Test
    void readOfALongerBodyStopsAtTheLastByteOfItsContentRange() throws IOException {
        wm.stubFor(get(urlEqualTo(PATH))
                .withHeader("Range", equalTo("bytes=1000-1099"))
                .willReturn(rangeAnswerWithBodyLength(1000, 100, 150)));
        byte[] buffer = new byte[200];

        try (ReadHandle handle = storage.read(KEY, ReadOptions.range(1000, 100))) {
            InputStream content = handle.content();

            assertThatThrownBy(() -> content.readNBytes(buffer, 0, buffer.length))
                    .isInstanceOf(IOException.class)
                    .hasCauseExactlyInstanceOf(TransientStorageException.class);
        }
        assertThat(Arrays.copyOfRange(buffer, 0, 100)).isEqualTo(Arrays.copyOfRange(DATA, 1000, 1100));
        assertThat(Arrays.copyOfRange(buffer, 100, 200)).isEqualTo(new byte[100]);
    }

    /** Like {@link HttpRangeReader}, the read fails rather than completing a short body with a GET of the rest. */
    @Test
    void bodyShorterThanItsContentRangeIsNotCompletedWithAGetOfTheRest() throws IOException {
        stubRangeThenRest(rangeAnswerWithBodyLength(1000, 50, 30), rangeAnswer(1050, 50));
        wm.stubFor(get(urlEqualTo(PATH))
                .withHeader("Range", equalTo("bytes=1030-1099"))
                .willReturn(rangeAnswer(1030, 70)));

        try (ReadHandle handle = storage.read(KEY, ReadOptions.range(1000, 100))) {
            InputStream content = handle.content();

            assertThatThrownBy(content::readAllBytes)
                    .isInstanceOf(IOException.class)
                    .hasCauseExactlyInstanceOf(TransientStorageException.class)
                    .hasMessageContaining("Response body and Content-Range disagree");
        }
        wm.verify(1, getRequestedFor(urlEqualTo(PATH)));
    }

    /**
     * Each row names the answer to a read of the range {@code 1000-1099} and the answer to the GET of its rest from
     * {@code 1050}, one of them without Content-Range and with a body going on past the requested bytes.
     */
    static Stream<Arguments> answersWithoutContentRangeLongerThanRequested() {
        return Stream.of(
                Arguments.argumentSet(
                        "whole range", answerWithoutContentRange(1000, 150), answerWithoutContentRange(1050, 50)),
                Arguments.argumentSet("rest", rangeAnswer(1000, 50), answerWithoutContentRange(1050, 500)));
    }

    /** Like {@link HttpRangeReader}, a body without Content-Range stands for at most the requested bytes. */
    @ParameterizedTest
    @MethodSource("answersWithoutContentRangeLongerThanRequested")
    void bodyWithoutContentRangeLongerThanRequestedFailsTheRead(
            ResponseDefinitionBuilder first, ResponseDefinitionBuilder rest) throws IOException {
        stubRangeThenRest(first, rest);

        try (ReadHandle handle = storage.read(KEY, ReadOptions.range(1000, 100))) {
            InputStream content = handle.content();

            assertThatThrownBy(content::readAllBytes)
                    .isInstanceOf(IOException.class)
                    .hasCauseExactlyInstanceOf(StorageException.class)
                    .hasMessageContaining("more data than requested");
        }
    }

    @ParameterizedTest
    @MethodSource("answersWithoutContentRangeLongerThanRequested")
    void bodyWithoutContentRangeLongerThanRequestedFailsAReadOfTheRangeLength(
            ResponseDefinitionBuilder first, ResponseDefinitionBuilder rest) throws IOException {
        stubRangeThenRest(first, rest);

        try (ReadHandle handle = storage.read(KEY, ReadOptions.range(1000, 100))) {
            InputStream content = handle.content();

            assertThatThrownBy(() -> content.readNBytes(100))
                    .isInstanceOf(IOException.class)
                    .hasCauseExactlyInstanceOf(StorageException.class)
                    .hasMessageContaining("more data than requested");
        }
    }

    /** The bytes past the range never reach the buffer of the caller, even before the read fails. */
    @Test
    void readOfALongerBodyWithoutContentRangeStopsAtTheEndOfTheRange() throws IOException {
        wm.stubFor(get(urlEqualTo(PATH))
                .withHeader("Range", equalTo("bytes=1000-1099"))
                .willReturn(answerWithoutContentRange(1000, 150)));
        byte[] buffer = new byte[200];

        try (ReadHandle handle = storage.read(KEY, ReadOptions.range(1000, 100))) {
            InputStream content = handle.content();

            assertThatThrownBy(() -> content.readNBytes(buffer, 0, buffer.length))
                    .isInstanceOf(IOException.class)
                    .hasCauseExactlyInstanceOf(StorageException.class);
        }
        assertThat(Arrays.copyOfRange(buffer, 0, 100)).isEqualTo(Arrays.copyOfRange(DATA, 1000, 1100));
        assertThat(Arrays.copyOfRange(buffer, 100, 200)).isEqualTo(new byte[100]);
    }

    /**
     * Each row names the answer to a read of the range {@code 1000-1099} and the answer to the GET of its rest from
     * {@code 1050}, one of them without Content-Range and with a body of the requested bytes.
     */
    static Stream<Arguments> answersWithoutContentRange() {
        return Stream.of(
                Arguments.argumentSet(
                        "whole range", answerWithoutContentRange(1000, 100), answerWithoutContentRange(1050, 50)),
                Arguments.argumentSet("rest", rangeAnswer(1000, 50), answerWithoutContentRange(1050, 50)));
    }

    @ParameterizedTest
    @MethodSource("answersWithoutContentRange")
    void bodyWithoutContentRangeIsReadAsTheRequestedBytes(
            ResponseDefinitionBuilder first, ResponseDefinitionBuilder rest) throws IOException {
        stubRangeThenRest(first, rest);

        byte[] content = readAll(ReadOptions.range(1000, 100));

        assertThat(content).isEqualTo(Arrays.copyOfRange(DATA, 1000, 1100));
    }

    /** Without a Content-Range, a body shorter than the rest tells that the object ends before the end of the range. */
    @Test
    void restWithoutContentRangeShorterThanRequestedEndsTheRead() throws IOException {
        stubRangeThenRest(rangeAnswer(1000, 50), answerWithoutContentRange(1050, 20));

        byte[] content = readAll(ReadOptions.range(1000, 100));

        assertThat(content).isEqualTo(Arrays.copyOfRange(DATA, 1000, 1070));
        wm.verify(2, getRequestedFor(urlEqualTo(PATH)));
    }

    @Test
    void unrangedReadAnsweredWithTheWholeObjectReturnsIt() throws IOException {
        wm.stubFor(get(urlEqualTo(PATH))
                .withHeader("Range", absent())
                .willReturn(aResponse().withStatus(200).withBody(DATA)));

        byte[] content = readAll(ReadOptions.defaults());

        assertThat(content).isEqualTo(DATA);
    }

    /** Stubs the range {@code 1000-1099} answered in part, and the GET of its rest answered with {@code rest}. */
    private void stubRangeAnsweredInPartThenRestWith(ResponseDefinitionBuilder rest) {
        stubRangeThenRest(rangeAnswer(1000, 50), rest);
    }

    /**
     * Stubs the range {@code 1000-1099} answered with {@code first}, and the GET of its rest from 1050 with
     * {@code rest}.
     */
    private void stubRangeThenRest(ResponseDefinitionBuilder first, ResponseDefinitionBuilder rest) {
        wm.stubFor(get(urlEqualTo(PATH))
                .withHeader("Range", equalTo("bytes=1000-1099"))
                .willReturn(first));
        wm.stubFor(get(urlEqualTo(PATH))
                .withHeader("Range", equalTo("bytes=1050-1099"))
                .willReturn(rest));
    }

    /**
     * Stubs the GET of {@code range} answered with the last 50 bytes of {@link #DATA} and an unknown object size, and
     * the GET of {@code restRange} with {@code rest}.
     */
    private void stubEndOfAnObjectOfUnknownSize(String range, String restRange, ResponseDefinitionBuilder rest) {
        wm.stubFor(get(urlEqualTo(PATH))
                .withHeader("Range", equalTo(range))
                .willReturn(rangeAnswerOfAnUnknownSize(99_950, 50)));
        wm.stubFor(get(urlEqualTo(PATH)).withHeader("Range", equalTo(restRange)).willReturn(rest));
    }

    private byte[] readAll(ReadOptions options) throws IOException {
        return readWith(InputStream::readAllBytes, options);
    }

    private byte[] readByteByByte(ReadOptions options) throws IOException {
        return readWith(HttpStorageRangedReadTest::readEachByte, options);
    }

    private byte[] readWith(ContentRead read, ReadOptions options) throws IOException {
        try (ReadHandle handle = storage.read(KEY, options)) {
            return read.readFrom(handle.content());
        }
    }

    private static byte[] readEachByte(InputStream in) throws IOException {
        ByteArrayOutputStream content = new ByteArrayOutputStream();
        int value = in.read();
        while (value != -1) {
            content.write(value);
            value = in.read();
        }
        return content.toByteArray();
    }

    /** Reads bytes from the content of a read. */
    @FunctionalInterface
    private interface ContentRead {
        byte[] readFrom(InputStream content) throws IOException;
    }

    /** A 206 answering {@code length} bytes of {@link #DATA} from {@code offset}, as told by its Content-Range. */
    private static ResponseDefinitionBuilder rangeAnswer(int offset, int length) {
        return rangeAnswerWithBodyLength(offset, length, length);
    }

    /**
     * A 206 naming {@code length} bytes of {@link #DATA} from {@code offset} in its Content-Range, with a body of
     * {@code bodyLength} bytes from {@code offset}.
     */
    private static ResponseDefinitionBuilder rangeAnswerWithBodyLength(int offset, int length, int bodyLength) {
        int last = offset + length - 1;
        return aResponse()
                .withStatus(206)
                .withHeader("Content-Range", "bytes " + offset + "-" + last + "/" + DATA.length)
                .withBody(Arrays.copyOfRange(DATA, offset, offset + bodyLength));
    }

    /**
     * A 206 answering {@code length} bytes of {@link #DATA} from {@code offset}, with {@code objectSize} as the total
     * of its Content-Range.
     */
    private static ResponseDefinitionBuilder rangeAnswerWithObjectSize(int offset, int length, int objectSize) {
        int last = offset + length - 1;
        return aResponse()
                .withStatus(206)
                .withHeader("Content-Range", "bytes " + offset + "-" + last + "/" + objectSize)
                .withBody(Arrays.copyOfRange(DATA, offset, offset + length));
    }

    /**
     * A 206 answering {@code length} bytes of {@link #DATA} from {@code offset}, with {@code *} as the total of its
     * Content-Range.
     */
    private static ResponseDefinitionBuilder rangeAnswerOfAnUnknownSize(int offset, int length) {
        int last = offset + length - 1;
        return aResponse()
                .withStatus(206)
                .withHeader("Content-Range", "bytes " + offset + "-" + last + "/*")
                .withBody(Arrays.copyOfRange(DATA, offset, offset + length));
    }

    /** A 206 without Content-Range, with a body of {@code bodyLength} bytes of {@link #DATA} from {@code offset}. */
    private static ResponseDefinitionBuilder answerWithoutContentRange(int offset, int bodyLength) {
        return aResponse().withStatus(206).withBody(Arrays.copyOfRange(DATA, offset, offset + bodyLength));
    }

    /** Returns {@code options} made conditional on a change since {@link #LAST_SEEN}: only such a read gets a 304. */
    private static ReadOptions modifiedSince(ReadOptions options) {
        Optional<Instant> lastSeen = Optional.of(LAST_SEEN);
        return new ReadOptions(
                options.offset(), options.length(), options.ifMatchEtag(), options.versionId(), lastSeen);
    }

    /** A 200 answering the whole of {@link #DATA}, as sent by a server ignoring the range of a GET. */
    private static ResponseDefinitionBuilder wholeObjectAnswer() {
        return aResponse().withStatus(200).withBody(DATA);
    }

    /** A {@link #wholeObjectAnswer()} declaring the length of {@link #DATA} in its Content-Length. */
    private static ResponseDefinitionBuilder wholeObjectAnswerWithItsLength() {
        return wholeObjectAnswer().withHeader("Content-Length", String.valueOf(DATA.length));
    }

    /** A 302 to another location, with the page sent by a server for clients not following it. */
    private static ResponseDefinitionBuilder redirect() {
        return aResponse()
                .withStatus(302)
                .withHeader("Location", MOVED_PATH)
                .withHeader("Content-Type", "text/html")
                .withBody(REDIRECT_PAGE);
    }

    /**
     * Answers one GET with a 206 naming the bytes {@code 1000-1099}, delimited by the close of the connection (no
     * Content-Length, not chunked), and closes it after {@link #BYTES_SENT} of them.
     */
    private static final class CutShortServer implements AutoCloseable {

        private static final int BYTES_SENT = 50;
        private static final int END_OF_REQUEST_HEAD = 0x0D0A0D0A;
        private static final String ANSWER_HEAD = "HTTP/1.1 206 Partial Content\r\n"
                + "Content-Range: bytes 1000-1099/" + DATA.length + "\r\n"
                + "Connection: close\r\n"
                + "\r\n";

        private final ServerSocket listener;

        private CutShortServer(ServerSocket listener) {
            this.listener = listener;
        }

        /** Listens on the loopback and answers the first GET in the background. */
        static CutShortServer start() throws IOException {
            ServerSocket listener = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
            CutShortServer server = new CutShortServer(listener);
            Thread answering = new Thread(server::answerOneGet, "cut-short-server");
            answering.setDaemon(true);
            answering.start();
            return server;
        }

        URI baseUri() {
            return URI.create("http://127.0.0.1:" + listener.getLocalPort() + "/");
        }

        @Override
        public void close() throws IOException {
            listener.close();
        }

        private void answerOneGet() {
            try (Socket connection = listener.accept()) {
                skipRequestHead(connection.getInputStream());
                OutputStream out = connection.getOutputStream();
                out.write(ANSWER_HEAD.getBytes(StandardCharsets.US_ASCII));
                out.write(DATA, 1000, BYTES_SENT);
                out.flush();
            } catch (IOException closedByTheTest) {
                // the test is over
            }
        }

        /** Reads the head of a GET through its closing blank line; a GET has no body. */
        private static void skipRequestHead(InputStream in) throws IOException {
            int lastFourBytes = 0;
            while (lastFourBytes != END_OF_REQUEST_HEAD) {
                int value = in.read();
                if (value == -1) {
                    return;
                }
                lastFourBytes = (lastFourBytes << 8) | value;
            }
        }
    }
}
