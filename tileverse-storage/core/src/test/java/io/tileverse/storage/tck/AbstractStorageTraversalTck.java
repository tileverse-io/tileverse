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
package io.tileverse.storage.tck;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.tileverse.storage.CopyOptions;
import io.tileverse.storage.PresignWriteOptions;
import io.tileverse.storage.ReadOptions;
import io.tileverse.storage.Storage;
import io.tileverse.storage.WriteOptions;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * TCK that asserts every key-accepting method on a {@link Storage} implementation rejects path-traversal attempts and
 * other malformed keys at the validation layer, before any backend code runs. Per-backend ITs extend this and provide a
 * configured Storage from their existing fixture.
 *
 * <p>Subclasses can override {@link #maliciousKeys()} and {@link #maliciousPatterns()} to add backend-specific cases
 * (e.g. FileStorage adds Windows-style backslash separators). The base method-sources cover the universal lexical rules
 * from {@link Storage#requireSafeKey(String)} and {@link Storage#requireSafePattern(String)}.
 *
 * <p>Tests use {@code assumeTrue} to skip operations the backend doesn't support: write-method tests skip on read-only
 * backends (HTTP), presign-method tests skip on backends without presigned-URL support (File, Azure DataLake).
 *
 * <p>{@link TestInstance.Lifecycle#PER_CLASS} is required so subclasses can override the non-static method-source.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SuppressWarnings("java:S5778")
public abstract class AbstractStorageTraversalTck {

    /**
     * The Storage instance under test. Subclasses provide one from their fixture (e.g. backed by a TestContainer or a
     * temp directory) and own its lifecycle. Tests never touch the backend - validation rejects the call before any I/O
     * - so subclasses can return the same shared instance for every call without worrying about per-test reset.
     */
    protected abstract Storage storage();

    /**
     * Universal malicious-key cases that every backend MUST reject. Subclasses override (calling super and adding to
     * the stream) to include backend-specific cases.
     */
    protected Stream<String> maliciousKeys() {
        return Stream.of(
                "..", // bare ..
                "../escape", // .. as first segment
                "foo/../escape", // .. mid-key after a real segment
                "data/sub/../../escape", // multiple .. resolving outside
                ".", // bare .
                "./foo", // . as first segment
                "foo/./bar", // . mid-key
                "/absolute", // leading slash
                "/", // bare slash
                "with\0null", // NUL byte
                "" // empty key (rejected unless capability says otherwise)
                );
    }

    /**
     * Malicious patterns that every backend's {@code list} MUST reject. Empty pattern is the documented root-listing
     * contract and is NOT included here.
     */
    protected Stream<String> maliciousPatterns() {
        return Stream.of(
                "../**",
                "data/../**",
                "data/sub/../../**/*.parquet",
                "./**",
                "data/./**",
                "/absolute/**",
                "with\0null/**");
    }

    // ---------- read-side methods (every backend) ----------

    @ParameterizedTest(name = "stat rejects {0}")
    @MethodSource("maliciousKeys")
    void statRejectsMaliciousKey(String key) {
        assertThatThrownBy(() -> storage().stat(key)).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest(name = "exists rejects {0}")
    @MethodSource("maliciousKeys")
    void existsRejectsMaliciousKey(String key) {
        // exists(key) defaults to stat(key).isPresent(); covered by statRejectsMaliciousKey but kept for explicit
        // coverage in case a backend overrides exists().
        assertThatThrownBy(() -> storage().exists(key)).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest(name = "openRangeReader(String) rejects {0}")
    @MethodSource("maliciousKeys")
    void openRangeReaderRejectsMaliciousKey(String key) {
        assertThatThrownBy(() -> storage().openRangeReader(key)).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest(name = "read rejects {0}")
    @MethodSource("maliciousKeys")
    void readRejectsMaliciousKey(String key) {
        assertThatThrownBy(() -> storage().read(key, ReadOptions.defaults()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest(name = "list rejects {0}")
    @MethodSource("maliciousPatterns")
    void listRejectsMaliciousPattern(String pattern) {
        assumeTrue(storage().capabilities().list(), "backend does not support list");
        assertThatThrownBy(() -> storage().list(pattern)).isInstanceOf(IllegalArgumentException.class);
    }

    // ---------- write-side methods (skipped on read-only backends) ----------

    @ParameterizedTest(name = "put(byte[]) rejects {0}")
    @MethodSource("maliciousKeys")
    void putBytesRejectsMaliciousKey(String key) {
        assumeTrue(storage().capabilities().writes(), "backend does not support writes");
        assertThatThrownBy(() -> storage().put(key, new byte[] {1, 2, 3}, WriteOptions.defaults()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest(name = "put(Path) rejects {0}")
    @MethodSource("maliciousKeys")
    void putPathRejectsMaliciousKey(String key, @org.junit.jupiter.api.io.TempDir Path tmp) throws Exception {
        assumeTrue(storage().capabilities().writes(), "backend does not support writes");
        Path source = Files.createTempFile(tmp, "src", ".bin");
        try {
            Files.write(source, new byte[] {1, 2, 3});
            assertThatThrownBy(() -> storage().put(key, source, WriteOptions.defaults()))
                    .isInstanceOf(IllegalArgumentException.class);
        } finally {
            Files.delete(source);
        }
    }

    @ParameterizedTest(name = "openOutputStream rejects {0}")
    @MethodSource("maliciousKeys")
    void openOutputStreamRejectsMaliciousKey(String key) {
        assumeTrue(storage().capabilities().writes(), "backend does not support writes");
        assertThatThrownBy(() -> storage().openOutputStream(key, WriteOptions.defaults()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest(name = "delete rejects {0}")
    @MethodSource("maliciousKeys")
    void deleteRejectsMaliciousKey(String key) {
        assumeTrue(storage().capabilities().writes(), "backend does not support writes");
        assertThatThrownBy(() -> storage().delete(key)).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest(name = "deleteAll rejects {0}")
    @MethodSource("maliciousKeys")
    void deleteAllRejectsMaliciousKey(String key) {
        assumeTrue(storage().capabilities().writes(), "backend does not support writes");
        assertThatThrownBy(() -> storage().deleteAll(List.of(key))).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest(name = "copy rejects malicious src {0}")
    @MethodSource("maliciousKeys")
    void copyRejectsMaliciousSrcKey(String key) {
        assumeTrue(storage().capabilities().writes(), "backend does not support writes");
        assertThatThrownBy(() -> storage().copy(key, "dst.bin", CopyOptions.defaults()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest(name = "copy rejects malicious dst {0}")
    @MethodSource("maliciousKeys")
    void copyRejectsMaliciousDstKey(String key) {
        assumeTrue(storage().capabilities().writes(), "backend does not support writes");
        assertThatThrownBy(() -> storage().copy("src.bin", key, CopyOptions.defaults()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest(name = "move rejects malicious src {0}")
    @MethodSource("maliciousKeys")
    void moveRejectsMaliciousSrcKey(String key) {
        assumeTrue(storage().capabilities().writes(), "backend does not support writes");
        assertThatThrownBy(() -> storage().move(key, "dst.bin", CopyOptions.defaults()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest(name = "move rejects malicious dst {0}")
    @MethodSource("maliciousKeys")
    void moveRejectsMaliciousDstKey(String key) {
        assumeTrue(storage().capabilities().writes(), "backend does not support writes");
        assertThatThrownBy(() -> storage().move("src.bin", key, CopyOptions.defaults()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ---------- presign methods (skipped on backends without presign support) ----------

    @ParameterizedTest(name = "presignGet rejects {0}")
    @MethodSource("maliciousKeys")
    void presignGetRejectsMaliciousKey(String key) {
        assumeTrue(storage().capabilities().presignedUrls(), "backend does not support presigned URLs");
        assertThatThrownBy(() -> storage().presignGet(key, Duration.ofMinutes(1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest(name = "presignPut rejects {0}")
    @MethodSource("maliciousKeys")
    void presignPutRejectsMaliciousKey(String key) {
        assumeTrue(storage().capabilities().presignedUrls(), "backend does not support presigned URLs");
        assertThatThrownBy(() -> storage().presignPut(key, Duration.ofMinutes(1), PresignWriteOptions.defaults()))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
