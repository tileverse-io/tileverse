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

import java.util.Optional;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import lombok.NonNull;

/**
 * Parsed representation of a {@link Storage#list(String) list pattern}.
 *
 * <p>A pattern is a shell-style glob (or plain prefix) that combines two pieces of intent:
 *
 * <ul>
 *   <li><b>What to traverse</b>: the longest non-glob path prefix and whether to walk descendants or only immediate
 *       children.
 *   <li><b>What to match</b>: an optional predicate compiled from the glob portion that filters keys client-side after
 *       listing.
 * </ul>
 *
 * <p>Examples (showing parsed prefix + walkDescendants + has-matcher):
 *
 * <pre>{@code
 * ""                      -> ("", false, no matcher)
 * "data/"                 -> ("data/", false, no matcher)
 * "data/*.parquet"        -> ("data/", false, matcher)        // immediate children
 * "data/**"               -> ("data/", true,  matcher)        // walk all
 * "data/**\/*.parquet"    -> ("data/", true,  matcher)        // walk + filter, any depth incl. top level
 * "**\/*.parquet"         -> ("",      true,  matcher)        // every .parquet anywhere, top level included
 * "data/{a,b}/file.txt"   -> ("data/", true,  matcher)        // depth-2; walk required to reach
 * "**.parquet"            -> ("",      true,  matcher)        // walk root
 * }</pre>
 *
 * <p>Rule for {@code walkDescendants}: true if the glob portion contains either {@code **} (cross-directory wildcard)
 * or {@code /} (any path separator implies the match targets nested levels).
 *
 * <p>The matcher is built from the full pattern (not just the glob suffix) and runs as a regex over the canonical
 * {@code /}-separated key, so matching is OS-independent. The glob syntax follows DuckDB's convention rather than JDK
 * glob:
 *
 * <ul>
 *   <li>{@code *} matches any run of characters within a single segment (does not cross {@code /}).
 *   <li>{@code ?} matches exactly one character within a segment.
 *   <li>{@code **} matches any run of characters including {@code /}.
 *   <li>{@code **}{@code /} matches <b>zero or more</b> leading directories, so {@code **}{@code /*.parquet} also
 *       matches a top-level {@code data.parquet} (this is where it diverges from JDK glob, which requires at least one
 *       directory).
 *   <li>{@code [abc]} and {@code [a-z]} match a single character in the set or range; a leading {@code !}
 *       ({@code [!abc]}) negates it and never matches {@code /}. As in DuckDB, {@code ^} is an ordinary member, not a
 *       negation marker.
 *   <li>{@code /} is a literal separator.
 * </ul>
 *
 * <p>{@code &#123;a,b&#125;} brace alternation is a tileverse/parquetry extension beyond DuckDB glob (parquetry emits
 * {@code &#123;name.parquet&#125;} to force exact, prefix-free matching of a single file); the glob rules apply within
 * each alternative. As in DuckDB, a metacharacter that cannot form a valid construct is treated as a literal: an
 * unclosed {@code [} or a {@code &#123;} that opens no balanced group matches that character literally.
 *
 * @param prefix the literal storage-key prefix to seed listing from; never null, may be empty
 * @param walkDescendants when true, list all descendants of {@code prefix}; when false, list only immediate children
 *     (with a backend-appropriate {@code "/"} delimiter where supported)
 * @param matcher optional client-side glob filter over full keys; empty when the pattern is a pure prefix (no glob
 *     characters), in which case the listing is unfiltered
 */
public record StoragePattern(String prefix, boolean walkDescendants, Optional<Predicate<String>> matcher) {

    public static StoragePattern parse(@NonNull final String pattern) {
        String p = pattern;
        while (p.startsWith("/")) {
            p = p.substring(1);
        }

        int firstGlob = -1;
        for (int i = 0; i < p.length(); i++) {
            char c = p.charAt(i);
            if (c == '*' || c == '?' || c == '[' || c == '{') {
                firstGlob = i;
                break;
            }
        }

        if (firstGlob < 0) {
            // No glob characters: pure prefix listing of immediate children.
            return new StoragePattern(p, false, Optional.empty());
        }

        int lastSlashBeforeGlob = p.lastIndexOf('/', firstGlob);
        String prefix = lastSlashBeforeGlob >= 0 ? p.substring(0, lastSlashBeforeGlob + 1) : "";
        String globSuffix = p.substring(prefix.length());
        boolean walkDescendants = globSuffix.contains("**") || globSuffix.contains("/");
        Predicate<String> matcher = globMatcher(p);
        return new StoragePattern(prefix, walkDescendants, Optional.of(matcher));
    }

    private static Predicate<String> globMatcher(String pattern) {
        Pattern regex = Pattern.compile(globToRegex(pattern));
        return key -> key != null && regex.matcher(key).matches();
    }

    /**
     * Translates a DuckDB-style glob into a regex anchored over a canonical {@code /}-separated storage key. The
     * defining difference from JDK glob is that {@code **}{@code /} matches zero or more directories, so
     * {@code **}{@code /*.parquet} also matches a top-level {@code data.parquet}.
     *
     * <ul>
     *   <li>{@code *} matches any run of characters within a single segment (does not cross {@code /}).
     *   <li>{@code ?} matches exactly one character within a segment.
     *   <li>{@code **} matches any run of characters including {@code /}.
     *   <li>{@code **}{@code /} matches zero or more leading directories.
     *   <li>{@code [abc]}/{@code [a-z]} is a character class; a leading {@code !} negates it and excludes {@code /}.
     *   <li>{@code &#123;a,b&#125;} is alternation (a tileverse extension beyond DuckDB), with the glob rules applied
     *       within each alternative.
     * </ul>
     *
     * <p>An unclosed {@code [} or {@code &#123;} is treated as a literal character, matching DuckDB.
     */
    static String globToRegex(String glob) {
        StringBuilder regex = new StringBuilder("^");
        appendGlob(glob, 0, glob.length(), regex);
        return regex.append('$').toString();
    }

    private static void appendGlob(String glob, int from, int to, StringBuilder regex) {
        int i = from;
        while (i < to) {
            char c = glob.charAt(i);
            switch (c) {
                case '*' -> i = appendStar(glob, i, to, regex);
                case '[' -> i = appendCharClass(glob, i, to, regex);
                case '{' -> i = appendBrace(glob, i, regex);
                case '?' -> {
                    regex.append("[^/]");
                    i++;
                }
                default -> {
                    appendLiteral(c, regex);
                    i++;
                }
            }
        }
    }

    private static int appendStar(String glob, int start, int to, StringBuilder regex) {
        boolean crossesDirectories = start + 1 < to && glob.charAt(start + 1) == '*';
        if (!crossesDirectories) {
            regex.append("[^/]*");
            return start + 1;
        }
        boolean matchesLeadingDirectories = start + 2 < to && glob.charAt(start + 2) == '/';
        if (matchesLeadingDirectories) {
            regex.append("(?:.*/)?");
            return start + 3;
        }
        regex.append(".*");
        return start + 2;
    }

    /**
     * Translates a {@code [...]} character class. DuckDB spells negation with a leading {@code !} only ({@code ^} is an
     * ordinary member), and a negated class never matches the path separator.
     */
    private static int appendCharClass(String glob, int start, int to, StringBuilder regex) {
        int contentStart = start + 1;
        boolean negated = contentStart < to && glob.charAt(contentStart) == '!';
        int firstMember = negated ? contentStart + 1 : contentStart;
        int scanFrom = firstMember < to && glob.charAt(firstMember) == ']' ? firstMember + 1 : firstMember;
        int close = glob.indexOf(']', scanFrom);
        if (close < 0) {
            appendLiteral('[', regex); // unclosed class: a lone '[' is a literal, as in DuckDB
            return start + 1;
        }
        regex.append('[');
        if (negated) {
            regex.append('^');
        }
        for (int i = firstMember; i < close; i++) {
            appendClassChar(glob.charAt(i), regex);
        }
        if (negated) {
            regex.append('/');
        }
        regex.append(']');
        return close + 1;
    }

    private static void appendClassChar(char c, StringBuilder regex) {
        if (c == '\\' || c == ']' || c == '[' || c == '^') {
            regex.append('\\');
        }
        regex.append(c);
    }

    private static int appendBrace(String glob, int start, StringBuilder regex) {
        int close = glob.indexOf('}', start);
        if (close < 0) {
            appendLiteral('{', regex); // no closing '}': a lone '{' is a literal, as in DuckDB
            return start + 1;
        }
        String[] alternatives = glob.substring(start + 1, close).split(",");
        regex.append("(?:");
        for (int k = 0; k < alternatives.length; k++) {
            if (k > 0) {
                regex.append('|');
            }
            String alternative = alternatives[k];
            appendGlob(alternative, 0, alternative.length(), regex);
        }
        regex.append(')');
        return close + 1;
    }

    private static void appendLiteral(char c, StringBuilder regex) {
        if (REGEX_METACHARACTERS.indexOf(c) >= 0) {
            regex.append('\\');
        }
        regex.append(c);
    }

    private static final String REGEX_METACHARACTERS = "\\.[]{}()+-^$|";
}
