#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
manual_supporters_file="$repo_root/docs/resources/additional-individual-supporters.txt"
aliases_file="$repo_root/docs/resources/contributor-aliases.txt"
ignored_contributors_file="$repo_root/docs/resources/ignored-contributors.txt"
output_file="$repo_root/SUPPORTERS.md"

tmp_contributors="$(mktemp)"
tmp_manual="$(mktemp)"
tmp_combined="$(mktemp)"
trap 'rm -f "$tmp_contributors" "$tmp_manual" "$tmp_combined"' EXIT

git -C "$repo_root" shortlog -sne --all | sed -E 's/^[[:space:]]*[0-9]+[[:space:]]+([^<]+)[[:space:]]+<.*$/\1/' > "$tmp_contributors"

if [[ -f "$aliases_file" ]]; then
  while IFS='=' read -r alias canonical; do
    [[ -z "${alias}" || "${alias}" == \#* ]] && continue
    sed -i "s/^${alias//\//\\/}$/$(printf '%s' "$canonical" | sed 's/[&/\]/\\&/g')/" "$tmp_contributors"
  done < "$aliases_file"
fi

sort -fu "$tmp_contributors" -o "$tmp_contributors"

if [[ -f "$ignored_contributors_file" ]]; then
  grep -v '^[[:space:]]*#' "$ignored_contributors_file" | sed '/^[[:space:]]*$/d' | while IFS= read -r ignored; do
    grep -F -x -v "$ignored" "$tmp_contributors" > "$tmp_contributors.filtered" || true
    mv "$tmp_contributors.filtered" "$tmp_contributors"
  done
fi

if [[ -f "$manual_supporters_file" ]]; then
  grep -v '^[[:space:]]*#' "$manual_supporters_file" | sed '/^[[:space:]]*$/d' > "$tmp_manual" || true
else
  : > "$tmp_manual"
fi

cat "$tmp_contributors" "$tmp_manual" | awk 'NF && !seen[tolower($0)]++' > "$tmp_combined"

{
  cat <<'EOF'
# Supporters

## Individual Supporters

EOF

  while IFS= read -r supporter; do
    [[ -n "$supporter" ]] && printf -- "- %s\n" "$supporter"
  done < "$tmp_combined"

  cat <<'EOF'

## Organizations

- [Multivers.io](https://www.multivers.io/)
EOF
} > "$output_file"
