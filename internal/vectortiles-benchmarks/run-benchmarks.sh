#!/bin/bash
set -e
# A failed benchmark run fails the script, although its output goes through tee
set -o pipefail

# Runs the decode benchmarks of this module on 7 tiles of mapbox/mvt-fixtures and maplibre/maplibre-tile-spec, then
# measures the heap held by a tile decoded by each library.
# The script downloads the missing tiles into tiles/, checks their SHA-256, builds the benchmarks jar, and runs the
# benchmarks once on the tiles. Each run writes the JSON and the text output of JMH into results/, and the table of
# the decoded tile sizes next to them.
# Arguments replace the default JMH options, -f 2 -prof gc -foe true. A smoke run:
#   internal/vectortiles-benchmarks/run-benchmarks.sh -f 0 -wi 0 -i 1 -r 100ms -foe true

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TILES_DIR="${SCRIPT_DIR}/tiles"
RESULTS_DIR="${SCRIPT_DIR}/results"
BENCHMARKS_JAR="${SCRIPT_DIR}/target/benchmarks.jar"
DEFAULT_JMH_OPTIONS=(-f 2 -prof gc -foe true)

# The URLs are pinned to a commit of each repository, as the checksums hold for these commits only
MVT_FIXTURES_URL="https://raw.githubusercontent.com/mapbox/mvt-fixtures/54e216ce6bb52c8ceee97b2b9f65438d8eb67b34/real-world"
MAPLIBRE_FIXTURES_URL="https://raw.githubusercontent.com/maplibre/maplibre-tile-spec/38e245a3e93a4f374969d597bfd8b7a05ca9ccdf/test/fixtures"

# Each tile: the base URL of its repository, its set and its name, and its SHA-256
TILES=(
    "${MVT_FIXTURES_URL} osm-qa-montevideo/12-1408-2471.mvt 3c95f081cf0df71bc73d3e22eedb91d2b2e93b8868a26715a33f86893f44b071"
    "${MVT_FIXTURES_URL} chicago/13-2101-3044.mvt af4d3286350665f2c9301b8d410b32aa10c0e8f5f1921dac57a19dbafab022b8"
    "${MAPLIBRE_FIXTURES_URL} bing/4-12-6.mvt b056a3da02b708cd0cfa98d82c1ac449e23121ee8c3397b4af172a67f2b07944"
    "${MAPLIBRE_FIXTURES_URL} amazon/5_6_21.mvt f5755035794106e62ad9f94ae4a25ac92af8a0486593fcdd9417b8b5a587e880"
    "${MAPLIBRE_FIXTURES_URL} amazon_here/4_9_4.mvt c249d240b7fec976901bac047572d687208eba315d789306fd3a7c397f4c4199"
    "${MAPLIBRE_FIXTURES_URL} omt/4_8_10.mvt 2a1b9c73986483d74cce9c58660d2d9bca3434573ce36c2871d13eaf967515fd"
    "${MAPLIBRE_FIXTURES_URL} omt/14_8298_10748.mvt b407269c205a943278e40fa4cade600e8d2f41d1739095b0f3f2d35453a949fe"
)

main() {
    check_required_commands
    download_tiles
    build_benchmarks_jar
    local results
    results="${RESULTS_DIR}/$(date +%Y%m%d-%H%M%S)"
    mkdir -p "${RESULTS_DIR}"
    run_benchmarks "${results}" "$@"
    measure_decoded_tile_sizes "${results}"
}

check_required_commands() {
    require_command curl "to download the tiles"
    require_command java "to run the benchmarks"
    if ! command_exists shasum && ! command_exists sha256sum; then
        echo "shasum or sha256sum is required to check the tiles, but neither is installed" >&2
        exit 1
    fi
}

require_command() {
    if ! command_exists "$1"; then
        echo "$1 is required $2, but not installed" >&2
        exit 1
    fi
}

command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# The tiles of earlier runs are checked too: a file may have changed since its download
download_tiles() {
    local entry base_url tile sha256
    for entry in "${TILES[@]}"; do
        read -r base_url tile sha256 <<< "${entry}"
        download_if_missing "${base_url}/${tile}" "${TILES_DIR}/${tile}"
        check_sha256 "${TILES_DIR}/${tile}" "${sha256}"
    done
}

# A tile gets its name once its download is complete: an interrupted download leaves a .part file, and the next run
# downloads the tile again
download_if_missing() {
    local url="$1"
    local file="$2"
    if [ ! -f "${file}" ]; then
        echo "Downloading ${url}"
        mkdir -p "$(dirname "${file}")"
        curl --fail --silent --show-error --location --retry 3 --max-time 120 --output "${file}.part" "${url}"
        mv "${file}.part" "${file}"
    fi
}

# Deletes a file with a wrong checksum, for the next run to download it again
check_sha256() {
    local file="$1"
    local expected="$2"
    local actual
    actual="$(sha256_of "${file}")"
    if [ "${actual}" != "${expected}" ]; then
        rm -f "${file}"
        echo "Wrong SHA-256 for ${file}: ${actual} instead of ${expected}. The file is deleted." >&2
        exit 1
    fi
}

sha256_of() {
    if command_exists shasum; then
        shasum -a 256 "$1" | cut -d ' ' -f 1
    else
        sha256sum "$1" | cut -d ' ' -f 1
    fi
}

build_benchmarks_jar() {
    echo "Building ${BENCHMARKS_JAR}"
    (cd "${REPO_DIR}" && ./mvnw -Pbenchmarks -pl :tileverse-vectortiles-benchmarks -am package -DskipTests)
}

# The benchmarks run in the tiles directory, where JMH names each tile by its set and its name
run_benchmarks() {
    local results="$1"
    shift
    local jmh_options=("$@")
    if [ ${#jmh_options[@]} -eq 0 ]; then
        jmh_options=("${DEFAULT_JMH_OPTIONS[@]}")
    fi
    echo "Running the benchmarks on ${#TILES[@]} tiles"
    (cd "${TILES_DIR}" && java -jar "${BENCHMARKS_JAR}" "${jmh_options[@]}" -p tile="$(tile_list)" \
        -rf json -rff "${results}.json") | tee "${results}.txt"
    echo "Results: ${results}.json and ${results}.txt"
}

# The tiles as the value of the JMH parameter, separated by commas
tile_list() {
    tile_paths | paste -s -d , -
}

# The measurement runs in the tiles directory too, with a fixed heap: a growing heap would blur the sizes
measure_decoded_tile_sizes() {
    local results="$1"
    echo "Measuring the heap held by the decoded tiles"
    (cd "${TILES_DIR}" && tile_paths | xargs java -Xms4g -Xmx4g -cp "${BENCHMARKS_JAR}" \
        io.tileverse.vectortile.benchmarks.DecodedTileSize) | tee "${results}-sizes.md"
    echo "Decoded tile sizes: ${results}-sizes.md"
}

# The tiles as their paths in the tiles directory, one per line
tile_paths() {
    local entry tile
    for entry in "${TILES[@]}"; do
        read -r _ tile _ <<< "${entry}"
        echo "${tile}"
    done
}

main "$@"
