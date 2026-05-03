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
package io.tileverse.storage;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Unit tests for {@link Storage#requireSafeKey(String)} and {@link Storage#requireSafePattern(String)}. These exercise
 * the lexical rules in isolation, before any backend gets involved. Per-backend wiring is verified by
 * {@link AbstractStorageTraversalTck} and its per-backend extensions.
 */
class StorageRequireSafeKeyTest {

    // ---------- requireSafeKey: rejection cases ----------

    @Test
    void rejectsNullKey() {
        assertThatThrownBy(() -> Storage.requireSafeKey(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("key");
    }

    @Test
    void rejectsEmptyKey() {
        assertThatThrownBy(() -> Storage.requireSafeKey(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty");
    }

    @ParameterizedTest
    @ValueSource(strings = {"/abs", "/", "/etc/passwd", "/data/file.bin"})
    void rejectsLeadingSlashKey(String key) {
        assertThatThrownBy(() -> Storage.requireSafeKey(key))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("/");
    }

    @ParameterizedTest
    @ValueSource(strings = {"with\0null", "\0", "prefix\0/foo"})
    void rejectsNulInKey(String key) {
        assertThatThrownBy(() -> Storage.requireSafeKey(key))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("NUL");
    }

    @ParameterizedTest
    @ValueSource(strings = {"..", "../etc/passwd", "foo/../escape", "data/sub/../../escape", "a/b/.."})
    void rejectsDotDotSegment(String key) {
        assertThatThrownBy(() -> Storage.requireSafeKey(key))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("..");
    }

    @ParameterizedTest
    @ValueSource(strings = {".", "./foo", "foo/./bar", "a/b/."})
    void rejectsSingleDotSegment(String key) {
        assertThatThrownBy(() -> Storage.requireSafeKey(key))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(".");
    }

    // ---------- requireSafeKey: happy-path cases ----------

    @ParameterizedTest
    @ValueSource(
            strings = {
                "file.bin",
                "data/file.bin",
                "deeply/nested/path/to/object.parquet",
                ".hidden", // leading-dot filename, NOT a '.' segment
                "..hidden", // leading-dotdot filename, NOT a '..' segment
                "foo..bar", // contains '..' but not as a whole segment
                "spaces in name.txt",
                "unicode-éñ-ü.bin",
                "with-trailing-slash/", // legal-ish: empty trailing segment after split is not '..' or '.'
                "name?with=query", // not validated here; backend's openRangeReader(URI) handles URI-ness
            })
    void allowsLegitimateKeys(String key) {
        assertThatCode(() -> Storage.requireSafeKey(key)).doesNotThrowAnyException();
    }

    // ---------- requireSafePattern: rejection cases ----------

    @Test
    void patternRejectsNull() {
        assertThatThrownBy(() -> Storage.requireSafePattern(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("pattern");
    }

    @ParameterizedTest
    @ValueSource(strings = {"/abs/glob", "/", "/data/**"})
    void patternRejectsLeadingSlash(String pattern) {
        assertThatThrownBy(() -> Storage.requireSafePattern(pattern))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("/");
    }

    @ParameterizedTest
    @ValueSource(strings = {"../*", "data/../*", "data/sub/../../escape/**", "../**/*.parquet"})
    void patternRejectsDotDotSegment(String pattern) {
        assertThatThrownBy(() -> Storage.requireSafePattern(pattern))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("..");
    }

    @ParameterizedTest
    @ValueSource(strings = {"./*", "data/./*"})
    void patternRejectsDotSegment(String pattern) {
        assertThatThrownBy(() -> Storage.requireSafePattern(pattern))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(".");
    }

    @ParameterizedTest
    @ValueSource(strings = {"foo\0bar", "\0**"})
    void patternRejectsNul(String pattern) {
        assertThatThrownBy(() -> Storage.requireSafePattern(pattern))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("NUL");
    }

    // ---------- requireSafePattern: happy-path cases ----------

    @Test
    void patternAllowsEmpty() {
        // Empty pattern is the documented root-listing contract.
        assertThatCode(() -> Storage.requireSafePattern("")).doesNotThrowAnyException();
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "data/",
                "data/**",
                "data/**/*.parquet",
                "data/*.parquet",
                "data/file?.bin",
                "data/{foo,bar}/*.bin",
                "data/file[0-9].bin",
                "theme=addresses/**/*.parquet", // Hive-style partition with '='
            })
    void patternAllowsLegitimateGlobs(String pattern) {
        assertThatCode(() -> Storage.requireSafePattern(pattern)).doesNotThrowAnyException();
    }
}
