#!/usr/bin/env bash
#
# Verify the source=duckdb rows of glob-cases.tsv against the real duckdb CLI.
#
# This is the provenance tool for the golden table: it proves that every
# duckdb-sourced expected value reflects actual DuckDB glob behavior. Run it when
# adding duckdb rows or after a DuckDB upgrade; it is not part of the build (the
# replay test in StoragePatternTest is hermetic and needs no duckdb).
#
# For each row it materializes ONLY that key in a fresh temp dir, asks duckdb, and
# compares. Per-row isolation avoids one row's files polluting another's glob.
# Exits non-zero if any duckdb row disagrees with the binary.
#
# Usage: ./verify-glob-cases.sh
set -u

here="$(cd "$(dirname "$0")" && pwd)"
table="$here/glob-cases.tsv"

command -v duckdb >/dev/null 2>&1 || {
    echo "duckdb CLI not found on PATH" >&2
    exit 2
}

fail=0
while IFS=$'\t' read -r glob key expected source; do
    [ -z "${glob:-}" ] && continue
    case "$glob" in \#*) continue ;; esac
    [ "${source:-}" = "duckdb" ] || continue

    dir="$(mktemp -d)"
    mkdir -p "$dir/$(dirname "$key")" 2>/dev/null
    : > "$dir/$key"
    count="$(duckdb -noheader -list -c "SELECT count(*) FROM glob('$dir/$glob') WHERE file = '$dir/$key';" 2>/dev/null)"
    actual=$([ "$count" = "1" ] && echo true || echo false)
    rm -rf "$dir"

    if [ "$actual" = "$expected" ]; then
        flag=OK
    else
        flag="**MISMATCH**"
        fail=1
    fi
    printf '%-40s %-38s expected=%-5s duckdb=%-5s %s\n' "$glob" "$key" "$expected" "$actual" "$flag"
done < "$table"

exit $fail
