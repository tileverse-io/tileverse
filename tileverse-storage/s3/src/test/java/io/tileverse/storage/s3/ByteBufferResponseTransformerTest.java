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
package io.tileverse.storage.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tileverse.storage.StorageException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.exception.RetryableException;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/** Unit tests for {@link ByteBufferResponseTransformer}, driven the way the SDK's retry loop drives it. */
class ByteBufferResponseTransformerTest {

    private static final GetObjectResponse RESPONSE =
            GetObjectResponse.builder().contentRange("bytes 0-15/100").build();

    private static byte[] bytes(int count) {
        byte[] out = new byte[count];
        for (int i = 0; i < count; i++) {
            out[i] = (byte) (i * 7 + 1);
        }
        return out;
    }

    private static byte[] contents(ByteBuffer buffer, int from, int length) {
        byte[] out = new byte[length];
        buffer.get(from, out);
        return out;
    }

    private static AbortableInputStream bodyOf(byte[] body) {
        return AbortableInputStream.create(new ByteArrayInputStream(body));
    }

    /** Serves the first {@code available} bytes of {@code body}, then fails like a dropped connection. */
    private static InputStream droppingAfter(byte[] body, int available) {
        return new InputStream() {
            private int position;

            @Override
            public int read() throws IOException {
                byte[] one = new byte[1];
                return read(one, 0, 1) == -1 ? -1 : one[0] & 0xFF;
            }

            @Override
            public int read(byte[] into, int offset, int length) throws IOException {
                if (position >= available) {
                    throw new IOException("connection reset");
                }
                int count = Math.min(length, available - position);
                System.arraycopy(body, position, into, offset, count);
                position += count;
                return count;
            }
        };
    }

    @Test
    void streamsTheBodyIntoTheTargetAtItsPosition() {
        ByteBuffer target = ByteBuffer.allocate(32);
        target.position(8);
        ByteBufferResponseTransformer transformer = new ByteBufferResponseTransformer(target, 16);
        byte[] body = bytes(16);

        GetObjectResponse returned = transformer.transform(RESPONSE, bodyOf(body));

        assertThat(returned).isSameAs(RESPONSE);
        assertThat(transformer.bytesWritten()).isEqualTo(16);
        assertThat(target.position()).isEqualTo(24);
        assertThat(target.limit()).isEqualTo(32);
        assertThat(contents(target, 8, 16)).containsExactly(body);
    }

    @Test
    void streamsIntoADirectTarget() {
        ByteBuffer target = ByteBuffer.allocateDirect(32);
        target.position(3);
        ByteBufferResponseTransformer transformer = new ByteBufferResponseTransformer(target, 16);
        byte[] body = bytes(16);

        transformer.transform(RESPONSE, bodyOf(body));

        assertThat(target.position()).isEqualTo(19);
        assertThat(contents(target, 3, 16)).containsExactly(body);
    }

    @Test
    void aShortBodyReportsTheShortCount() {
        ByteBuffer target = ByteBuffer.allocate(32);
        target.position(8);
        ByteBufferResponseTransformer transformer = new ByteBufferResponseTransformer(target, 16);
        byte[] body = bytes(10);

        transformer.transform(RESPONSE, bodyOf(body));

        assertThat(transformer.bytesWritten()).isEqualTo(10);
        assertThat(target.position()).isEqualTo(18);
        assertThat(contents(target, 8, 10)).containsExactly(body);
    }

    @Test
    void aDroppedBodyIsRetryableAndTheNextAttemptRestartsAtTheInitialPosition() {
        ByteBuffer target = ByteBuffer.allocate(32);
        target.position(8);
        ByteBufferResponseTransformer transformer = new ByteBufferResponseTransformer(target, 16);
        byte[] body = bytes(16);
        AbortableInputStream dropped = AbortableInputStream.create(droppingAfter(body, 6));

        assertThatThrownBy(() -> transformer.transform(RESPONSE, dropped))
                .isInstanceOf(RetryableException.class)
                .hasCauseInstanceOf(IOException.class);
        assertThat(target.position()).isEqualTo(14);

        transformer.transform(RESPONSE, bodyOf(body));

        assertThat(transformer.bytesWritten()).isEqualTo(16);
        assertThat(target.position()).isEqualTo(24);
        assertThat(contents(target, 8, 16)).containsExactly(body);
    }

    @Test
    void rejectsACountOutsideTheTargetCapacity() {
        ByteBuffer target = ByteBuffer.allocate(8);

        assertThatThrownBy(() -> new ByteBufferResponseTransformer(target, 9))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ByteBufferResponseTransformer(target, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aBodyLongerThanRequestedAbortsTheConnection() {
        ByteBuffer target = ByteBuffer.allocate(32);
        target.position(8);
        ByteBufferResponseTransformer transformer = new ByteBufferResponseTransformer(target, 16);
        AtomicBoolean aborted = new AtomicBoolean();
        AbortableInputStream body =
                AbortableInputStream.create(new ByteArrayInputStream(bytes(20)), () -> aborted.set(true));

        assertThatThrownBy(() -> transformer.transform(RESPONSE, body))
                .isInstanceOf(StorageException.class)
                .hasMessageContaining("more data than requested");
        assertThat(aborted).isTrue();
    }

    @Test
    void aTargetRefusingTheWriteAbortsTheConnectionAndFailsTheRead() {
        ByteBuffer target = ByteBuffer.allocate(32).asReadOnlyBuffer();
        ByteBufferResponseTransformer transformer = new ByteBufferResponseTransformer(target, 16);
        AtomicBoolean aborted = new AtomicBoolean();
        AbortableInputStream body =
                AbortableInputStream.create(new ByteArrayInputStream(bytes(16)), () -> aborted.set(true));

        assertThatThrownBy(() -> transformer.transform(RESPONSE, body))
                .isInstanceOf(StorageException.class)
                .hasMessageContaining("refused the write");
        assertThat(aborted).isTrue();
    }
}
