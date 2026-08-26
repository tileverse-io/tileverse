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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tileverse.storage.ContentRange;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class MultipartByteRangesParserTest {

    private static final String BOUNDARY = "PART_SEPARATOR";

    private static byte[] body(String... chunks) {
        StringBuilder joined = new StringBuilder();
        for (String chunk : chunks) {
            joined.append(chunk);
        }
        return joined.toString().getBytes(StandardCharsets.ISO_8859_1);
    }

    private static String part(long first, long last, long total, String data) {
        return "--" + BOUNDARY + "\r\n"
                + "Content-Type: application/octet-stream\r\n"
                + "Content-Range: bytes " + first + "-" + last + "/" + total + "\r\n"
                + "\r\n"
                + data + "\r\n";
    }

    private static String closing() {
        return "--" + BOUNDARY + "--\r\n";
    }

    private static String readAll(MultipartByteRangesParser parser, int length) throws IOException {
        ByteBuffer scratch = ByteBuffer.allocate(length);
        int read = parser.readBody(scratch, length);
        return new String(scratch.array(), 0, read, StandardCharsets.ISO_8859_1);
    }

    @Test
    void parsesPartsInOrder() throws IOException {
        InputStream in = new ByteArrayInputStream(body(part(0, 4, 100, "AAAAA"), part(50, 52, 100, "BBB"), closing()));
        MultipartByteRangesParser parser = MultipartByteRangesParser.multipart(in, BOUNDARY);

        ContentRange.Bytes first = parser.nextPart();
        assertThat(first.firstPos()).isZero();
        assertThat(readAll(parser, 5)).isEqualTo("AAAAA");

        ContentRange.Bytes second = parser.nextPart();
        assertThat(second.firstPos()).isEqualTo(50);
        assertThat(readAll(parser, 3)).isEqualTo("BBB");

        assertThat(parser.nextPart()).isNull();
        assertThat(parser.nextPart()).isNull();
    }

    @Test
    void skipsUnconsumedBodyBytesOnAdvance() throws IOException {
        InputStream in = new ByteArrayInputStream(body(part(0, 4, 100, "AAAAA"), part(50, 52, 100, "BBB"), closing()));
        MultipartByteRangesParser parser = MultipartByteRangesParser.multipart(in, BOUNDARY);

        parser.nextPart();
        ContentRange.Bytes second = parser.nextPart();

        assertThat(second.firstPos()).isEqualTo(50);
        assertThat(readAll(parser, 3)).isEqualTo("BBB");
    }

    @Test
    void skipAndPartialReadsRouteWithinAPart() throws IOException {
        InputStream in = new ByteArrayInputStream(body(part(10, 19, 100, "0123456789"), closing()));
        MultipartByteRangesParser parser = MultipartByteRangesParser.multipart(in, BOUNDARY);

        parser.nextPart();
        parser.skipBody(2);
        assertThat(readAll(parser, 3)).isEqualTo("234");
        parser.skipBody(2);
        assertThat(readAll(parser, 3)).isEqualTo("789");
        assertThat(parser.nextPart()).isNull();
    }

    @Test
    void toleratesLowerCaseHeadersAndBareLineFeeds() throws IOException {
        String lenientPart = "--" + BOUNDARY + "\n" + "content-range: bytes 5-9/50\n" + "\n" + "HELLO\n";
        InputStream in = new ByteArrayInputStream(body(lenientPart, "--" + BOUNDARY + "--\n"));
        MultipartByteRangesParser parser = MultipartByteRangesParser.multipart(in, BOUNDARY);

        ContentRange.Bytes part = parser.nextPart();
        assertThat(part.firstPos()).isEqualTo(5);
        assertThat(readAll(parser, 5)).isEqualTo("HELLO");
        assertThat(parser.nextPart()).isNull();
    }

    @Test
    void partWithoutContentRangeFailsTheParse() {
        String broken = "--" + BOUNDARY + "\r\n" + "Content-Type: application/octet-stream\r\n" + "\r\n" + "AAAAA\r\n";
        InputStream in = new ByteArrayInputStream(body(broken, closing()));
        MultipartByteRangesParser parser = MultipartByteRangesParser.multipart(in, BOUNDARY);

        assertThatThrownBy(parser::nextPart).isInstanceOf(IOException.class);
    }

    @Test
    void truncatedBodyYieldsShortReads() throws IOException {
        byte[] truncated = body("--" + BOUNDARY + "\r\n" + "Content-Range: bytes 0-9/100\r\n" + "\r\n" + "ABC");
        MultipartByteRangesParser parser =
                MultipartByteRangesParser.multipart(new ByteArrayInputStream(truncated), BOUNDARY);

        parser.nextPart();
        ByteBuffer scratch = ByteBuffer.allocate(10);
        assertThat(parser.readBody(scratch, 10)).isEqualTo(3);
    }

    @Test
    void singlePartFactoryYieldsExactlyOnePart() throws IOException {
        InputStream in = new ByteArrayInputStream("HELLOWORLD".getBytes(StandardCharsets.ISO_8859_1));
        ContentRange.Bytes range = ContentRange.bytesOf("bytes 20-29/100").orElseThrow();
        MultipartByteRangesParser parser = MultipartByteRangesParser.singlePart(in, range);

        ContentRange.Bytes part = parser.nextPart();
        assertThat(part).isSameAs(range);
        parser.skipBody(5);
        assertThat(readAll(parser, 5)).isEqualTo("WORLD");
        assertThat(parser.nextPart()).isNull();
    }

    @Test
    void readBodyStopsAtThePartEnd() throws IOException {
        InputStream in = new ByteArrayInputStream(body(part(0, 4, 100, "AAAAA"), part(50, 52, 100, "BBB"), closing()));
        MultipartByteRangesParser parser = MultipartByteRangesParser.multipart(in, BOUNDARY);

        parser.nextPart();
        ByteBuffer greedy = ByteBuffer.allocate(64);
        assertThat(parser.readBody(greedy, 64)).isEqualTo(5);
        assertThat(parser.nextPart().firstPos()).isEqualTo(50);
    }
}
