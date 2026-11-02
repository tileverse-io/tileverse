# Vector tile decode benchmarks

JMH benchmarks of vector tile decoding by tileverse and by [java-vector-tile](https://github.com/ElectronicChartCentre/java-vector-tile) 1.4.2. This module is never published.

## What the benchmarks measure

The `decoder` parameter selects the library: `TILEVERSE` or `JAVA_VECTOR_TILE`.

- `decode` measures the decoding of the tile bytes: `VectorTileCodec.decode(byte[])` for tileverse, and `VectorTileDecoder.decode(byte[])` for java-vector-tile. java-vector-tile parses the whole tile with protobuf in this call. tileverse copies the bytes in this call, and indexes the layers of the copy: their keys and values, the start and end of each feature, and the geometry of each feature, decoded into packed coordinates. Indexing checks the whole tile as protobuf's parser does.
- `readFeatures` decodes the tile, then reads the id, the geometry and the attributes of each feature. java-vector-tile builds the geometry and the attributes of a feature while iterating over the features. A tileverse feature builds them when asked.
- `readDecodedFeatures` reads the id, the geometry and the attributes of each feature of a tile decoded once, in the setup of the trial. java-vector-tile builds the geometry and the attributes of each feature again at each iteration over the features. A tileverse feature builds its geometry from the packed coordinates of its layer, and reads its attributes from the tile bytes, when asked.

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

## The protobuf runtime of java-vector-tile

java-vector-tile 1.4.2 declares protobuf-java 4.29.3, and the benchmarks run it on that version. The root pom manages protobuf-java to another version for the cloud SDKs. The pom of this module sets it back to 4.29.3. tileverse needs no protobuf runtime.

## Build and run

`run-benchmarks.sh` downloads the tiles, builds the benchmarks jar, and runs the benchmarks on them, with two forks and the GC profiler, as for the results below. It then measures the heap held by the decoded tiles, as described in [Decoded tile size](#decoded-tile-size). The first benchmark error stops the run, and the script fails. It works from any directory:

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

## Results

Average time per operation (`avgt`) and allocation per operation (`gc.alloc.rate.norm`), measured with OpenJDK 25.0.5 (Temurin 25.0.5+8-LTS), JMH 1.37 and the default JMH options of `run-benchmarks.sh`, `-f 2 -prof gc -foe true`, on these machines:

- `rei`: an Apple M4 Max (12 performance and 4 efficiency cores) with 48 GB, under macOS 15.8.
- `gendo`: an AMD Ryzen 9 7950X (16 cores, 32 threads, up to 5.9 GHz) with 187 GB of DDR5 at 3600 MT/s, under Ubuntu 24.04 with Linux 6.8.
- `eva00`: two Intel Xeon Gold 6138 at 2.0 GHz (2 x 20 cores, 80 threads), measured in a KVM virtual machine with 64 vCPUs and 94 GB, under Ubuntu 24.04 with Linux 6.8.

Allocation differs by less than 5% between the machines, and the table gives the allocation measured on `rei`.

The gain of tileverse over java-vector-tile depends on the tile and on the processor. `readDecodedFeatures` is 1.6 to 10.3 times as fast with tileverse on `rei`, 1.5 to 7.6 times on `gendo`, and 1.7 to 4.9 times on `eva00`. `decode` of the tiles of long lines and large polygons, `bing` and `amazon`, takes 3% longer with tileverse on `gendo`. On `eva00`, tileverse decodes `bing` in 4% less time and `amazon` in 15% less.

| Tile | Benchmark | Decoder | Time on `rei` (us/op) | Time on `gendo` (us/op) | Time on `eva00` (us/op) | Allocation (KiB/op) |
|---|---|---|---:|---:|---:|---:|
| `osm-qa-montevideo/12-1408-2471.mvt` | `decode` | java-vector-tile | 14,609 | 18,093 | 53,075 | 98,779 |
| `osm-qa-montevideo/12-1408-2471.mvt` | `decode` | tileverse | 6,573 | 8,376 | 20,194 | 20,886 |
| `osm-qa-montevideo/12-1408-2471.mvt` | `readFeatures` | java-vector-tile | 27,003 | 37,688 | 100,109 | 199,483 |
| `osm-qa-montevideo/12-1408-2471.mvt` | `readFeatures` | tileverse | 14,194 | 20,937 | 52,185 | 89,366 |
| `osm-qa-montevideo/12-1408-2471.mvt` | `readDecodedFeatures` | java-vector-tile | 12,078 | 21,304 | 53,088 | 96,086 |
| `osm-qa-montevideo/12-1408-2471.mvt` | `readDecodedFeatures` | tileverse | 7,375 | 12,692 | 28,353 | 68,480 |
| `chicago/13-2101-3044.mvt` | `decode` | java-vector-tile | 185 | 167 | 450 | 854 |
| `chicago/13-2101-3044.mvt` | `decode` | tileverse | 80 | 121 | 336 | 282 |
| `chicago/13-2101-3044.mvt` | `readFeatures` | java-vector-tile | 470 | 468 | 1,273 | 2,720 |
| `chicago/13-2101-3044.mvt` | `readFeatures` | tileverse | 186 | 264 | 674 | 1,276 |
| `chicago/13-2101-3044.mvt` | `readDecodedFeatures` | java-vector-tile | 269 | 283 | 741 | 1,831 |
| `chicago/13-2101-3044.mvt` | `readDecodedFeatures` | tileverse | 134 | 129 | 345 | 1,015 |
| `bing/4-12-6.mvt` | `decode` | java-vector-tile | 709 | 677 | 1,705 | 3,870 |
| `bing/4-12-6.mvt` | `decode` | tileverse | 393 | 696 | 1,639 | 2,454 |
| `bing/4-12-6.mvt` | `readFeatures` | java-vector-tile | 2,209 | 2,355 | 6,338 | 15,450 |
| `bing/4-12-6.mvt` | `readFeatures` | tileverse | 617 | 1,100 | 2,904 | 6,816 |
| `bing/4-12-6.mvt` | `readDecodedFeatures` | java-vector-tile | 1,589 | 1,738 | 4,341 | 11,571 |
| `bing/4-12-6.mvt` | `readDecodedFeatures` | tileverse | 216 | 412 | 1,191 | 4,362 |
| `amazon/5_6_21.mvt` | `decode` | java-vector-tile | 699 | 713 | 2,141 | 4,896 |
| `amazon/5_6_21.mvt` | `decode` | tileverse | 471 | 732 | 1,818 | 2,715 |
| `amazon/5_6_21.mvt` | `readFeatures` | java-vector-tile | 2,077 | 2,200 | 5,630 | 12,945 |
| `amazon/5_6_21.mvt` | `readFeatures` | tileverse | 599 | 914 | 2,423 | 5,001 |
| `amazon/5_6_21.mvt` | `readDecodedFeatures` | java-vector-tile | 1,142 | 1,205 | 2,909 | 7,372 |
| `amazon/5_6_21.mvt` | `readDecodedFeatures` | tileverse | 111 | 158 | 589 | 2,248 |
| `amazon_here/4_9_4.mvt` | `decode` | java-vector-tile | 434 | 465 | 1,372 | 3,512 |
| `amazon_here/4_9_4.mvt` | `decode` | tileverse | 240 | 384 | 1,022 | 1,887 |
| `amazon_here/4_9_4.mvt` | `readFeatures` | java-vector-tile | 1,176 | 1,285 | 3,508 | 8,453 |
| `amazon_here/4_9_4.mvt` | `readFeatures` | tileverse | 385 | 619 | 1,675 | 3,669 |
| `amazon_here/4_9_4.mvt` | `readDecodedFeatures` | java-vector-tile | 592 | 660 | 1,786 | 4,286 |
| `amazon_here/4_9_4.mvt` | `readDecodedFeatures` | tileverse | 137 | 232 | 582 | 1,782 |
| `omt/4_8_10.mvt` | `decode` | java-vector-tile | 3,686 | 3,470 | 9,392 | 20,694 |
| `omt/4_8_10.mvt` | `decode` | tileverse | 2,170 | 2,974 | 6,603 | 5,887 |
| `omt/4_8_10.mvt` | `readFeatures` | java-vector-tile | 8,546 | 8,796 | 24,750 | 60,240 |
| `omt/4_8_10.mvt` | `readFeatures` | tileverse | 4,512 | 5,745 | 14,169 | 29,319 |
| `omt/4_8_10.mvt` | `readDecodedFeatures` | java-vector-tile | 4,454 | 4,841 | 14,894 | 38,931 |
| `omt/4_8_10.mvt` | `readDecodedFeatures` | tileverse | 2,284 | 2,751 | 7,353 | 23,432 |
| `omt/14_8298_10748.mvt` | `decode` | java-vector-tile | 2,321 | 1,929 | 5,210 | 10,836 |
| `omt/14_8298_10748.mvt` | `decode` | tileverse | 1,090 | 1,464 | 3,335 | 2,771 |
| `omt/14_8298_10748.mvt` | `readFeatures` | java-vector-tile | 4,857 | 5,009 | 13,533 | 30,963 |
| `omt/14_8298_10748.mvt` | `readFeatures` | tileverse | 2,317 | 3,252 | 7,051 | 13,667 |
| `omt/14_8298_10748.mvt` | `readDecodedFeatures` | java-vector-tile | 2,285 | 2,716 | 7,152 | 19,649 |
| `omt/14_8298_10748.mvt` | `readDecodedFeatures` | tileverse | 1,188 | 1,770 | 4,215 | 10,738 |

## Decoded tile size

A cache of decoded tiles pays for each tile the heap held by the decoded tile. `DecodedTileSize`, a program of this module run by `run-benchmarks.sh` after the benchmarks, measures this heap for each tile and library. It decodes copies of the tile, reads the id, the geometry and the attributes of each feature of each copy once, and divides the growth of the used heap after a full garbage collection by the number of copies. It reads each copy before measuring, because java-vector-tile turns some bytes of its protobuf objects into strings at the first read, and keeps them. java-vector-tile keeps the protobuf objects of the tile. tileverse keeps a copy of the tile bytes, an index of its layers and features, and the packed coordinates of its geometries. The program checks that both libraries read the same coordinates.

A tile decoded by tileverse holds 2.2 to 3.1 times its encoded size, and 8% to 75% less heap than the same tile decoded by java-vector-tile. Measured on `rei` with the JDK of the results above, with `-Xms4g -Xmx4g` and the G1 collector, which picks regions of 2 MiB for this heap. Two runs differ by up to 0.2%, the sizes measured on `gendo` by up to 0.3%, and the sizes measured on `eva00` by up to 0.7%. The heap delta compares the heap of tileverse with the heap of java-vector-tile: -60% means 60% less.

| Tile | Encoded | java-vector-tile | tileverse | Heap delta |
|---|---:|---:|---:|---:|
| `osm-qa-montevideo/12-1408-2471.mvt` | 6,822 KiB | 53,392 KiB | 19,354 KiB | -64% |
| `chicago/13-2101-3044.mvt` | 71 KiB | 520 KiB | 177 KiB | -66% |
| `bing/4-12-6.mvt` | 281 KiB | 1,312 KiB | 834 KiB | -36% |
| `amazon/5_6_21.mvt` | 403 KiB | 1,991 KiB | 1,236 KiB | -38% |
| `amazon_here/4_9_4.mvt` | 729 KiB | 1,769 KiB | 1,624 KiB | -8% |
| `omt/4_8_10.mvt` | 1,505 KiB | 15,891 KiB | 3,933 KiB | -75% |
| `omt/14_8298_10748.mvt` | 888 KiB | 7,268 KiB | 1,998 KiB | -73% |
