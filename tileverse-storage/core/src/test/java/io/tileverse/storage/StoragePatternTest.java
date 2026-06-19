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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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
        // Ant/DuckDB semantics: **/ matches zero directories, so the literal segment may sit directly under data/.
        assertThat(p.matcher().get().test("data/theme=buildings/file.parquet")).isTrue();
        assertThat(p.matcher().get().test("data/release=2024-01/theme=buildings/sub/file.parquet"))
                .isFalse();
    }

    @Test
    void leadingDoubleStarSlashMatchesTopLevelAndAnyDepth() {
        // The conventional "every .parquet anywhere, including top level" pattern.
        StoragePattern p = StoragePattern.parse("**/*.parquet");
        assertThat(p.prefix()).isEmpty();
        assertThat(p.walkDescendants()).isTrue();
        assertThat(p.matcher().get().test("data.parquet")).isTrue();
        assertThat(p.matcher().get().test("a/data.parquet")).isTrue();
        assertThat(p.matcher().get().test("a/b/data.parquet")).isTrue();
        assertThat(p.matcher().get().test("data.txt")).isFalse();
    }

    @Test
    void prefixedDoubleStarSlashMatchesZeroOrMoreDirs() {
        StoragePattern p = StoragePattern.parse("data/**/*.parquet");
        assertThat(p.prefix()).isEqualTo("data/");
        assertThat(p.walkDescendants()).isTrue();
        assertThat(p.matcher().get().test("data/x.parquet")).isTrue(); // zero intermediate dirs
        assertThat(p.matcher().get().test("data/a/b/x.parquet")).isTrue();
        assertThat(p.matcher().get().test("other/x.parquet")).isFalse();
    }

    @Test
    void braceMatchesSingleFileExactly() {
        // parquetry emits {name.parquet} for a single-file URI to force exact matching instead of prefix matching.
        StoragePattern p = StoragePattern.parse("{example.parquet}");
        assertThat(p.prefix()).isEmpty();
        assertThat(p.matcher()).isPresent();
        assertThat(p.matcher().get().test("example.parquet")).isTrue();
        assertThat(p.matcher().get().test("example.parquet.bak")).isFalse();
        assertThat(p.matcher().get().test("sub/example.parquet")).isFalse();
    }

    @Test
    void braceAlternationRecursesGlobRules() {
        StoragePattern p = StoragePattern.parse("{*.parquet,**/*.parquet}");
        assertThat(p.matcher()).isPresent();
        assertThat(p.matcher().get().test("data.parquet")).isTrue();
        assertThat(p.matcher().get().test("year=2024/data.parquet")).isTrue();
        assertThat(p.matcher().get().test("data.csv")).isFalse();
    }

    @Test
    void characterClassMatchesOneOfTheSet() {
        StoragePattern p = StoragePattern.parse("part[12].txt");
        assertThat(p.matcher()).isPresent();
        assertThat(p.matcher().get().test("part1.txt")).isTrue();
        assertThat(p.matcher().get().test("part2.txt")).isTrue();
        assertThat(p.matcher().get().test("partX.txt")).isFalse();
    }

    @Test
    void characterClassRange() {
        StoragePattern p = StoragePattern.parse("f[A-C].txt");
        assertThat(p.matcher().get().test("fB.txt")).isTrue();
        assertThat(p.matcher().get().test("fD.txt")).isFalse();
    }

    @Test
    void negatedCharacterClassUsesBangAndNeverCrossesSlash() {
        StoragePattern p = StoragePattern.parse("a[!z]b.txt");
        assertThat(p.matcher().get().test("axb.txt")).isTrue();
        assertThat(p.matcher().get().test("azb.txt")).isFalse();
        // DuckDB: a negated class never matches the path separator.
        assertThat(p.matcher().get().test("a/b.txt")).isFalse();
    }

    @Test
    void caretInsideClassIsLiteralNotNegation() {
        // DuckDB spells negation with '!' only; '^' is an ordinary set member.
        StoragePattern p = StoragePattern.parse("x[^y].txt");
        assertThat(p.matcher().get().test("x^.txt")).isTrue();
        assertThat(p.matcher().get().test("xy.txt")).isTrue();
        assertThat(p.matcher().get().test("xz.txt")).isFalse();
    }

    @Test
    void unclosedCharacterClassIsTreatedAsLiteral() {
        // DuckDB treats an unclosed '[' as a literal character (verified against the duckdb binary).
        StoragePattern p = StoragePattern.parse("lt[1.txt");
        assertThat(p.matcher().get().test("lt[1.txt")).isTrue();
        assertThat(p.matcher().get().test("lt1.txt")).isFalse();
    }

    @Test
    void loneBraceIsTreatedAsLiteral() {
        // Braces are a tileverse extension; a '{' that opens no balanced group falls back to DuckDB's literal
        // treatment.
        StoragePattern p = StoragePattern.parse("br{a.txt");
        assertThat(p.matcher().get().test("br{a.txt")).isTrue();
        assertThat(p.matcher().get().test("bra.txt")).isFalse();
    }

    @Test
    void trailingCommaInBraceDoesNotMatchEmpty() {
        StoragePattern p = StoragePattern.parse("file{a,b,}.txt");
        assertThat(p.matcher().get().test("filea.txt")).isTrue();
        assertThat(p.matcher().get().test("fileb.txt")).isTrue();
        assertThat(p.matcher().get().test("file.txt")).isFalse();
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

    /**
     * Replays the shared golden table ({@code glob-cases.tsv}) that pins the matcher to DuckDB glob semantics. The
     * {@code duckdb}-sourced rows are reproducible with {@code verify-glob-cases.sh}; keep the table identical in
     * parquetry's copy of the translator.
     */
    @Nested
    class GlobToRegex {

        @ParameterizedTest(name = "[{3}] {0} ~ {1} -> {2}")
        @MethodSource("io.tileverse.storage.StoragePatternTest#goldenCases")
        void matchesGoldenTable(String glob, String key, boolean expected, String source) {
            boolean actual = Pattern.compile(StoragePattern.globToRegex(glob))
                    .matcher(key)
                    .matches();
            assertThat(actual).isEqualTo(expected);
        }
    }

    static Stream<Arguments> goldenCases() {
        return readGoldenTable().stream();
    }

    private static List<Arguments> readGoldenTable() {
        String resource = "glob-cases.tsv";
        try (InputStream in = StoragePatternTest.class.getResourceAsStream(resource);
                BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            List<Arguments> rows = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank() || line.startsWith("#")) {
                    continue;
                }
                String[] columns = line.split("\t");
                String glob = columns[0];
                String key = columns[1];
                boolean expected = Boolean.parseBoolean(columns[2]);
                String source = columns[3];
                rows.add(Arguments.of(glob, key, expected, source));
            }
            return rows;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
