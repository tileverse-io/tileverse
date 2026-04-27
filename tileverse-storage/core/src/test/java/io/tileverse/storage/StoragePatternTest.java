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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import org.junit.jupiter.api.Test;

class StoragePatternTest {

    @Test
    void emptyPatternListsRoot() {
        StoragePattern p = StoragePattern.parse("");
        assertThat(p.prefix()).isEmpty();
        assertThat(p.walkDescendants()).isFalse();
        assertThat(p.matcher()).isEmpty();
    }

    @Test
    void plainPrefixWithTrailingSlash() {
        StoragePattern p = StoragePattern.parse("data/year=2024/");
        assertThat(p.prefix()).isEqualTo("data/year=2024/");
        assertThat(p.walkDescendants()).isFalse();
        assertThat(p.matcher()).isEmpty();
    }

    @Test
    void plainPrefixWithoutSlashIsLiteralPrefix() {
        StoragePattern p = StoragePattern.parse("data");
        assertThat(p.prefix()).isEqualTo("data");
        assertThat(p.walkDescendants()).isFalse();
        assertThat(p.matcher()).isEmpty();
    }

    @Test
    void shallowGlobOnImmediateChildren() {
        StoragePattern p = StoragePattern.parse("data/*.parquet");
        assertThat(p.prefix()).isEqualTo("data/");
        assertThat(p.walkDescendants()).isFalse();
        assertThat(p.matcher()).isPresent();
        assertThat(p.matcher().get().test("data/file.parquet")).isTrue();
        assertThat(p.matcher().get().test("data/sub/file.parquet")).isFalse();
    }

    @Test
    void doubleStarWalksDescendants() {
        StoragePattern p = StoragePattern.parse("data/**");
        assertThat(p.prefix()).isEqualTo("data/");
        assertThat(p.walkDescendants()).isTrue();
        assertThat(p.matcher()).isPresent();
        assertThat(p.matcher().get().test("data/file.parquet")).isTrue();
        assertThat(p.matcher().get().test("data/sub/file.parquet")).isTrue();
    }

    @Test
    void recursiveGlobMatchesAtAnyDepth() {
        StoragePattern p = StoragePattern.parse("data/**/*.parquet");
        assertThat(p.prefix()).isEqualTo("data/");
        assertThat(p.walkDescendants()).isTrue();
        assertThat(p.matcher().get().test("data/x/y.parquet")).isTrue();
        assertThat(p.matcher().get().test("data/x/y/z.parquet")).isTrue();
    }

    @Test
    void recursiveGlobWithLiteralComponentAfterDoubleStar() {
        // Hive-style partition layout: walk anywhere under data/, then require a literal
        // theme=buildings/ segment, then any *.parquet leaf.
        StoragePattern p = StoragePattern.parse("/data/**/theme=buildings/*.parquet");
        assertThat(p.prefix()).isEqualTo("data/");
        assertThat(p.walkDescendants()).isTrue();
        assertThat(p.matcher()).isPresent();
        assertThat(p.matcher().get().test("data/release=2024-01/theme=buildings/file.parquet"))
                .isTrue();
        assertThat(p.matcher().get().test("data/release=2024-01/region=us/theme=buildings/file.parquet"))
                .isTrue();
        assertThat(p.matcher().get().test("data/release=2024-01/theme=transportation/file.parquet"))
                .isFalse();
        assertThat(p.matcher().get().test("data/theme=buildings/file.parquet")).isFalse();
        assertThat(p.matcher().get().test("data/release=2024-01/theme=buildings/sub/file.parquet"))
                .isFalse();
    }

    @Test
    void deepGlobWithoutDoubleStarStillWalks() {
        // The glob portion contains '/', so we must walk descendants to reach the matched depth.
        StoragePattern p = StoragePattern.parse("data/{a,b}/file.txt");
        assertThat(p.prefix()).isEqualTo("data/");
        assertThat(p.walkDescendants()).isTrue();
        assertThat(p.matcher().get().test("data/a/file.txt")).isTrue();
        assertThat(p.matcher().get().test("data/b/file.txt")).isTrue();
        assertThat(p.matcher().get().test("data/c/file.txt")).isFalse();
    }

    @Test
    void leadingSlashIsStripped() {
        StoragePattern p = StoragePattern.parse("/data/");
        assertThat(p.prefix()).isEqualTo("data/");
    }

    @Test
    void multiComponentPrefixBeforeGlob() {
        // The prefix is everything up to the last '/' before the first glob char, not just one segment.
        StoragePattern p = StoragePattern.parse("data/year=2024/region=us/*.parquet");
        assertThat(p.prefix()).isEqualTo("data/year=2024/region=us/");
        assertThat(p.walkDescendants()).isFalse();
        assertThat(p.matcher()).isPresent();
        assertThat(p.matcher().get().test("data/year=2024/region=us/file.parquet"))
                .isTrue();
        assertThat(p.matcher().get().test("data/year=2024/region=eu/file.parquet"))
                .isFalse();
        assertThat(p.matcher().get().test("data/year=2024/region=us/sub/file.parquet"))
                .isFalse();
    }

    @Test
    void multiComponentPrefixWithRecursiveGlob() {
        StoragePattern p = StoragePattern.parse("a/b/c/**/leaf.bin");
        assertThat(p.prefix()).isEqualTo("a/b/c/");
        assertThat(p.walkDescendants()).isTrue();
        assertThat(p.matcher().get().test("a/b/c/x/leaf.bin")).isTrue();
        assertThat(p.matcher().get().test("a/b/c/x/y/leaf.bin")).isTrue();
        assertThat(p.matcher().get().test("a/b/leaf.bin")).isFalse();
    }

    @Test
    void rootGlobMatchesAtAnyDepth() {
        StoragePattern p = StoragePattern.parse("**.parquet");
        assertThat(p.prefix()).isEmpty();
        assertThat(p.walkDescendants()).isTrue();
        assertThat(p.matcher().get().test("file.parquet")).isTrue();
        assertThat(p.matcher().get().test("a/b/file.parquet")).isTrue();
    }

    @Test
    void nullPatternIsRejected() {
        assertThatNullPointerException().isThrownBy(() -> StoragePattern.parse(null));
    }
}
