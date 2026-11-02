# Vector tile decode benchmarks

JMH benchmarks of vector tile decoding by tileverse and by [java-vector-tile](https://github.com/ElectronicChartCentre/java-vector-tile) 1.4.2. This module is never published.

## What the benchmarks measure

The `decoder` parameter selects the library: `TILEVERSE` or `JAVA_VECTOR_TILE`.

- `decode` measures the decoding of the tile bytes: `VectorTileCodec.decode(byte[])` for tileverse, and `VectorTileDecoder.decode(byte[])` for java-vector-tile. java-vector-tile parses the whole tile with protobuf in this call.
- `readFeatures` decodes the tile, then reads the id, the geometry and the attributes of each feature. java-vector-tile builds the geometry and the attributes of a feature while iterating over the features. A tileverse feature builds them when asked.
- `readDecodedFeatures` reads the id, the geometry and the attributes of each feature of a tile decoded once, in the setup of the trial. java-vector-tile builds the geometry and the attributes of each feature again at each iteration over the features. A tileverse feature builds them when asked.

`readFeatures` is the cost of a tile met for the first time: decoded, then read once. `readDecodedFeatures` is the cost of each later read of a tile kept decoded in memory. The PMTiles and VersaTiles vector tile stores keep decoded tiles in a cache, and a map drawing several layers of the same tiles, such as a GeoServer layer group styled with a Mapbox style, decodes each tile once and reads it once per layer. The read of a decoded tile is the repeated cost.

java-vector-tile runs with `setAutoScale(false)`. It returns coordinates in the extent space of the tile, as tileverse does.

## Tiles

The benchmarks run on the 7 tiles below, from two public repositories: two real-world tiles of [mapbox/mvt-fixtures](https://github.com/mapbox/mvt-fixtures), from its directory `real-world`, and five fixtures of [maplibre/maplibre-tile-spec](https://github.com/maplibre/maplibre-tile-spec), from its directory `test/fixtures`. `run-benchmarks.sh` downloads them into `internal/vectortiles-benchmarks/tiles/`, from commit `54e216ce` of mvt-fixtures and commit `38e245a3` of maplibre-tile-spec, and checks their SHA-256. They are not copied into this module: the Bing and HERE fixtures of maplibre-tile-spec state no license, and the OSM QA tile of Montevideo weighs 7.0 MB.

The tiles differ in size, in their number of features and layers, in the size and the type of their geometries, and in their attributes. Each one stands for a kind of map data. The two libraries build the same geometries from each tile, and the last column counts their coordinates.

| Tile | Profile | Size | Features | Coordinates |
|---|---|---:|---:|---:|
| `osm-qa-montevideo/12-1408-2471.mvt` | Raw OpenStreetMap data with all its tags: a very large tile of mostly points, with 13 attributes per feature on average and 242,799 distinct values | 7.0 MB | 80,605 | 119,723 |
| `chicago/13-2101-3044.mvt` | A street map of a city at zoom 13: roads, land use and labels in 13 layers, with points, lines and polygons | 73 KB | 1,366 | 10,555 |
| `bing/4-12-6.mvt` | A road map at zoom 4: few features, with lines of up to 61,363 points | 287 KB | 73 | 99,861 |
| `amazon/5_6_21.mvt` | A base map at zoom 5: large land cover polygons of up to 35,402 points, with 36 attributes per feature on average, in 21 layers | 412 KB | 169 | 119,038 |
| `amazon_here/4_9_4.mvt` | A base map at zoom 4: a few large polygons of up to 29,307 points next to 825 points of interest, with 15 attributes per feature on average | 747 KB | 870 | 54,048 |
| `omt/4_8_10.mvt` | OpenMapTiles at zoom 4: a dense network of 60,026 short lines in its transportation layer | 1.5 MB | 61,476 | 141,772 |
| `omt/14_8298_10748.mvt` | OpenMapTiles at zoom 14, its deepest level, read by maps at larger scales: buildings, points of interest and house numbers | 910 KB | 23,435 | 74,517 |

## Why java-vector-tile runs on its own protobuf runtime

java-vector-tile 1.4.2 declares protobuf-java 4.29.3, and the benchmarks measure it on that version. tileverse uses protobuf-javalite. The two runtimes share the package `com.google.protobuf`, and one classpath cannot hold both.

The module `java-vector-tile-shaded` packages java-vector-tile with protobuf-java 4.29.3, and relocates protobuf-java to the package `io.tileverse.internal.protobuf`. The benchmarks jar holds this runtime next to the protobuf-javalite runtime of tileverse.

## Build and run

`run-benchmarks.sh` downloads the tiles, builds the benchmarks jar, and runs the benchmarks on them, with two forks and the GC profiler. It then measures the heap held by the decoded tiles, as described in [Decoded tile size](#decoded-tile-size). The first benchmark error stops the run, and the script fails. It works from any directory:

```bash
internal/vectortiles-benchmarks/run-benchmarks.sh
```

Arguments replace the default JMH options, `-f 2 -prof gc -foe true`. A smoke run checks that the benchmarks run. Its numbers mean nothing:

```bash
internal/vectortiles-benchmarks/run-benchmarks.sh -f 0 -wi 0 -i 1 -r 100ms -foe true
```

Each run writes the JSON and the text output of JMH into `internal/vectortiles-benchmarks/results/`, in files named after the time of the run, and the table of the decoded tile sizes next to them, in a file ending with `-sizes.md`. The JMH options leave the size measurement unchanged: the sizes of a smoke run hold.

The script builds the runnable jar, `internal/vectortiles-benchmarks/target/benchmarks.jar`, with the `benchmarks` profile:

```bash
./mvnw -Pbenchmarks -pl :tileverse-vectortiles-benchmarks -am package -DskipTests
```

Maven prints a checksum warning when it downloads java-vector-tile 1.4.2, because the OSGeo repository publishes no checksum for this jar.

The `tile` parameter takes the paths of tile files, and has no default value. Run the jar on other tiles with `-p tile=<file>`, separated by commas:

```bash
java -jar internal/vectortiles-benchmarks/target/benchmarks.jar -f 2 -prof gc -p tile=<file>,<file>
```

## Decoded tile size

A cache of decoded tiles pays for each tile the heap held by the decoded tile. `DecodedTileSize`, a program of this module run by `run-benchmarks.sh` after the benchmarks, measures this heap for each tile and library. It decodes copies of the tile, reads the id, the geometry and the attributes of each feature of each copy once, and divides the growth of the used heap after a full garbage collection by the number of copies. It reads each copy before measuring, because java-vector-tile turns some bytes of its protobuf objects into strings at the first read, and keeps them. The program checks that both libraries read the same coordinates.
