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

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.Optional;
import java.util.function.Predicate;
import lombok.NonNull;

/**
 * Parsed representation of a {@link Storage#list(String) list pattern}.
 *
 * <p>A pattern is a shell-style glob (or plain prefix) that combines two pieces of intent:
 *
 * <ul>
 *   <li><b>What to traverse</b>: the longest non-glob path prefix and whether to walk descendants or only immediate
 *       children.
 *   <li><b>What to match</b>: an optional {@link java.nio.file.PathMatcher} compiled from the glob portion that filters
 *       keys client-side after listing.
 * </ul>
 *
 * <p>Examples (showing parsed prefix + walkDescendants + has-matcher):
 *
 * <pre>{@code
 * ""                      -> ("", false, no matcher)
 * "data/"                 -> ("data/", false, no matcher)
 * "data/*.parquet"        -> ("data/", false, matcher)        // immediate children
 * "data/**"               -> ("data/", true,  matcher)        // walk all
 * "data/**\/*.parquet"    -> ("data/", true,  matcher)        // walk + filter
 * "data/{a,b}/file.txt"   -> ("data/", true,  matcher)        // depth-2; walk required to reach
 * "**.parquet"            -> ("",      true,  matcher)        // walk root
 * }</pre>
 *
 * <p>Rule for {@code walkDescendants}: true if the glob portion contains either {@code **} (cross-directory wildcard)
 * or {@code /} (any path separator implies the match targets nested levels).
 *
 * <p>The matcher is built from the full pattern (not just the glob suffix) using JDK glob syntax (see
 * {@link java.nio.file.FileSystem#getPathMatcher}), so it operates on the canonical full key form. On Windows the key
 * is normalized to the platform separator before matching.
 *
 * <p><b>Cross-platform glob portability:</b> glob compilation delegates to
 * {@code FileSystems.getDefault().getPathMatcher("glob:...")}, with {@code /} translated to the platform separator
 * before matching. The basic syntax ({@code *}, {@code **}, {@code ?}, character classes) is portable, but Windows'
 * glob compiler may differ in edge cases - brace expansion ({@code &#123;a,b&#125;}), negated character classes
 * ({@code [!abc]}), and escaping can behave differently from POSIX. If you need bit-identical cross-OS matching, stick
 * to {@code *}, {@code **}, {@code ?}, and unbracketed literals.
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
        PathMatcher pm = FileSystems.getDefault().getPathMatcher("glob:" + pattern);
        return key -> {
            if (key == null) {
                return false;
            }
            Path p = Path.of(key.replace('/', File.separatorChar));
            return pm.matches(p);
        };
    }
}
