# Vector tile benchmarks

JMH benchmarks of vector tile decoding and writing by tileverse and by [java-vector-tile](https://github.com/ElectronicChartCentre/java-vector-tile) 1.4.2, and a test comparing the tiles written by the two libraries. This module is never published.

## What the decode benchmarks measure

The `decoder` parameter selects the library: `TILEVERSE` or `JAVA_VECTOR_TILE`.

- `decode` measures the decoding of the tile bytes: `VectorTileCodec.decode(byte[])` for tileverse, and `VectorTileDecoder.decode(byte[])` for java-vector-tile. java-vector-tile parses the whole tile with protobuf in this call. tileverse copies the bytes in this call, and indexes the layers of the copy: their keys and values, the start and end of each feature, and the geometry of each feature, decoded into packed coordinates. Indexing checks the whole tile as protobuf's parser does.
- `readFeatures` decodes the tile, then reads the id, the geometry and the attributes of each feature. java-vector-tile builds the geometry and the attributes of a feature while iterating over the features. A tileverse feature builds them when asked.
- `readDecodedFeatures` reads the id, the geometry and the attributes of each feature of a tile decoded once, in the setup of the trial. java-vector-tile builds the geometry and the attributes of each feature again at each iteration over the features. A tileverse feature builds its geometry from the packed coordinates of its layer, and reads its attributes from the tile bytes, when asked.

`readFeatures` is the cost of a tile met for the first time: decoded, then read once. `readDecodedFeatures` is the cost of each later read of a tile kept decoded in memory. The PMTiles and VersaTiles vector tile stores keep decoded tiles in a cache. A map drawing several layers of the same tiles decodes each tile once and reads it once per layer. The read of a decoded tile is the repeated cost.

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

`run-benchmarks.sh` downloads the tiles, builds the benchmarks jar, and runs the benchmarks on them, with two forks and the GC profiler, as for the results below. It then measures the heap held by the decoded tiles and by the tiles under construction, as described in [Decoded tile size](#decoded-tile-size) and [Tile under construction size](#tile-under-construction-size). The first benchmark error stops the run, and the script fails. It works from any directory:

```bash
internal/vectortiles-benchmarks/run-benchmarks.sh
```

Arguments replace the default JMH options, `-f 2 -prof gc -foe true`. A smoke run checks that the benchmarks run. Its numbers mean nothing:

```bash
internal/vectortiles-benchmarks/run-benchmarks.sh -f 0 -wi 0 -i 1 -r 100ms -foe true
```

Each run writes the JSON and the text output of JMH into `internal/vectortiles-benchmarks/results/`, in files named after the time of the run, and the tables of the sizes next to them: the decoded tile sizes in `<time>-sizes.md`, and the tile under construction sizes in `<time>-construction-sizes.md`. The JMH options leave the size measurements unchanged: the sizes of a smoke run hold.

The script builds the runnable jar, `internal/vectortiles-benchmarks/target/benchmarks.jar`, with the `benchmarks` profile:

```bash
./mvnw -Pbenchmarks -pl :tileverse-vectortiles-benchmarks -am package -DskipTests
```

Maven prints a checksum warning when it downloads java-vector-tile 1.4.2, because the OSGeo repository publishes no checksum for this jar.

The `tile` parameter takes the paths of tile files, and has no default value. Run the jar on other tiles with `-p tile=<file>`, separated by commas:

```bash
java -jar internal/vectortiles-benchmarks/target/benchmarks.jar -f 2 -prof gc -p tile=<file>,<file>
```

## Decode results

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

A tile decoded by tileverse holds 2.2 to 3.1 times its encoded size, and 8% to 75% less heap than the same tile decoded by java-vector-tile. Measured on `rei` with the JDK of the results above, with `-Xms4g -Xmx4g` and the G1 collector, which picks regions of 2 MiB for this heap. Two runs differ by up to 0.5%, the sizes measured on `gendo` by up to 0.3%, and the sizes measured on `eva00` by up to 0.7%. The heap delta compares the heap of tileverse with the heap of java-vector-tile: -60% means 60% less.

| Tile | Encoded | java-vector-tile | tileverse | Heap delta |
|---|---:|---:|---:|---:|
| `osm-qa-montevideo/12-1408-2471.mvt` | 6,822 KiB | 53,392 KiB | 19,354 KiB | -64% |
| `chicago/13-2101-3044.mvt` | 71 KiB | 520 KiB | 177 KiB | -66% |
| `bing/4-12-6.mvt` | 281 KiB | 1,312 KiB | 834 KiB | -36% |
| `amazon/5_6_21.mvt` | 403 KiB | 1,991 KiB | 1,236 KiB | -38% |
| `amazon_here/4_9_4.mvt` | 729 KiB | 1,769 KiB | 1,624 KiB | -8% |
| `omt/4_8_10.mvt` | 1,505 KiB | 15,891 KiB | 3,933 KiB | -75% |
| `omt/14_8298_10748.mvt` | 888 KiB | 7,268 KiB | 1,998 KiB | -73% |

## Writer benchmarks

`VectorTileWriteBenchmark` compares the writing of vector tiles by tileverse and by java-vector-tile 1.4.2. Its `write` benchmark writes a tile from its features with a new writer, up to the bytes of the tile: `VectorTileBuilder.toByteArray()` for tileverse, and `VectorTileEncoder.encode()` for java-vector-tile. The benchmark adds the features to tileverse one layer at a time, and to java-vector-tile with `addFeature`, in the same order. The setup of the trial decodes the features of the tile file once, with tileverse, and the operations share them: the writers only read them. A feature gets its position in its layer plus one as id. A layer of another extent than 4096 is scaled to 4096 first: the layer of `osm-qa-montevideo/12-1408-2471.mvt` from 1,048,576, and the layers of `bigtile.vector.pbf`, a tile of the parity test, from 512. The parity test, the validity benchmark and the measurement of the tiles under construction scale them the same way.

The `sequence` parameter selects the coordinate sequences of the geometries given to the writers:

- `ARRAY`: the sequences of a default JTS geometry factory, `CoordinateArraySequence`. java-vector-tile reads the coordinates of a geometry with `getCoordinates()`, and this sequence returns its array without copying it.
- `PACKED`: copies of the features with `PackedCoordinateSequence.Double`, made with `new GeometryFactory(PackedCoordinateSequenceFactory.DOUBLE_FACTORY).createGeometry(geometry)` once the features are in the extent and the units of the writers. A packed sequence holds the ordinates in one array of doubles. Its `getCoordinates()` creates a `Coordinate` per point at the first call, and keeps them until an ordinate is set. Before each operation, the benchmark sets the first ordinate of the packed sequences to its own value, and the operation pays for the creation of the `Coordinate` objects, as a writer of fresh geometries does.

The `writer` parameter selects the library: `TILEVERSE` or `JAVA_VECTOR_TILE`. The `configuration` parameter sets both writers to write the same tile from the same features. In each configuration, both writers first drop the lines shorter than 1 and the polygons of area below 1, in tile units. They write the ids given with the features, without automatic ids. The writers write tiles of extent 4096, and the features are in the coordinates of such a tile, except in `AUTO_SCALE`. The configurations:

- `ENCODING_ONLY`: the size filter alone. Both writers round and encode the coordinates, and clip nothing and simplify nothing. java-vector-tile runs as `NoClipEncoder`, a subclass of its encoder overriding `clipGeometry` and `clipCovers` to keep the geometries as given. tileverse runs with a pipeline holding a `SizeFilterStep` alone.
- `DEFAULTS`: the geometries are clipped to the tile and a buffer of 8 tile units around it, the default buffer of java-vector-tile, without simplification. java-vector-tile runs as `new VectorTileEncoder(4096, 8, false, false, -1)`, and tileverse with `setClipBuffer(8)`: the builder derives a pipeline of a `SizeFilterStep` and a `ClipStep`.
- `CLIP_NO_BUFFER`: as `DEFAULTS`, with a buffer of 0. The geometries are clipped at the edges of the tile.
- `CLIP_SIMPLIFY`: the lines and polygons are simplified with a distance tolerance of 2 tile units, then clipped as in `DEFAULTS`. java-vector-tile runs as `new VectorTileEncoder(4096, 8, false, false, 2.0)`, and tileverse with `setClipBuffer(8)` and `setSimplificationDistanceTolerance(2.0)`. Both writers repair the polygons invalid once rounded: java-vector-tile once it has encoded a polygon, and tileverse with the `PolygonRepairStep` at the end of its pipeline.
- `AUTO_SCALE`: the features are in the coordinates of a tile of size 256, and the writers scale them to the extent, without simplification. java-vector-tile runs as `new VectorTileEncoder(4096, 8, true, false, -1)`, and tileverse with `setInputScale(16)` and `setClipBuffer(128)`. The clip buffer is 8 units of the input, or 128 tile units. Both writers filter and clip the geometries in the units of the input, and multiply the coordinates by 16 when they round them: the builder derives a pipeline of a `SizeFilterStep(1 / 16.0, 1 / 256.0)` and a `ClipStep(256, 8)`, the minimum sizes and the clip area of java-vector-tile.

`PolygonValidityBenchmark` measures the check of a polygon once its coordinates are rounded, as run by both writers in `CLIP_SIMPLIFY` before they repair a polygon. Its `checkPolygons` benchmark runs the check on a fresh copy of each polygon and multipolygon of a tile. The three checks answer one question: whether the polygon is valid once its coordinates are rounded. The `check` parameter selects the check:

- `ROUNDED_VALIDITY`: `RoundedValidity.isValid`, the check of tileverse. It rounds the coordinates into reused `Coordinate` objects, and runs the validity check of JTS on the rounded copy.
- `PRECISION_REDUCER_IS_VALID`: `GeometryPrecisionReducer.reducePointwise(polygon, new PrecisionModel(1.0)).isValid()`. A copy of the polygon is rounded point by point, then checked by JTS, as in the check of java-vector-tile without its encoding and decoding.
- `IS_VALID`: `isValid()` on the fresh copy, once its coordinates are rounded in place with `Math.round` by a `CoordinateSequenceFilter`. The filter reports the change, and `Geometry.apply` then calls `geometryChanged()`: the copy drops its cached envelope. It is the validity check of JTS on the rounded polygon, without a second copy.
- `COPY_ONLY`: the copies alone, without a check. It is the baseline of the three checks: their times include the copies.

The setup of the trial decodes the polygons of the tile file once, with the default geometry reader of tileverse, scales them to the extent 4096 as for the write benchmark, and moves their coordinates by up to 0.49 tile units, as the parity test does. The moved polygons have packed coordinate sequences, and their coordinates are off the integers.

Each check runs on a fresh copy because a writer checks a polygon once. A packed coordinate sequence keeps the `Coordinate` objects created by its first `getCoordinates()`, as called by the validity check of JTS, and a second check of the same polygon would skip their creation. The copy of a packed sequence copies its ordinates into a new array, and creates no `Coordinate` object. The rounding of `IS_VALID` changes the copy, never the polygon of the setup.

A regular expression after the JMH options selects the benchmarks by name. The arguments replace the default options, and this command repeats them before the expression. It runs the write and validity benchmarks alone, then the size measurements:

```bash
internal/vectortiles-benchmarks/run-benchmarks.sh -f 2 -prof gc -foe true 'Write|Validity'
```

### Parity test

`WriterParityTest`, a JUnit test of this module, checks that both writers write the same features in the configurations. The build runs it on the four tiles committed in the test resources of `tileverse-vectortiles`, and on the seven tiles of `run-benchmarks.sh` once the script has downloaded them. The layers of a tile are scaled to the extent 4096 first, as for the write benchmark. In a configuration, a tile is written twice: with its features as decoded, and with their coordinates moved by up to 0.49 tile units. The offsets come from a random generator with a fixed seed. A moved integer coordinate rounds to its original value, and the writers receive coordinates off the integers, as from a caller.

The test decodes both tiles with tileverse, and matches the written features with their input features by layer and id. For an input feature, both writers must write the same features: the same attributes, and the same geometries, compared with `equalsExact` once their repeated consecutive points are removed. The comparison removes these points because java-vector-tile writes the last points of a ring when they round to its first point, and tileverse leaves them out.

The cases below are documented differences between the writers, and the comparison leaves out their input features:

- A polygon repaired by java-vector-tile once encoded. tileverse repairs polygons another way.
- A polygon with a ring of zero area once rounded, such as a sliver left by the clip along its edge. tileverse leaves the ring out, as required by the MVT specification, and java-vector-tile writes it: the decoder skips the ring. With simplification, tileverse also repairs such a polygon, as invalid once rounded, and the rings of the repaired polygon start at another vertex: the outputs are equal once normalized. java-vector-tile keeps the polygon, because its check decodes the polygon without its rings of zero area.
- A polygon with a ring changing orientation with rounding. java-vector-tile orients a ring by its unrounded coordinates, and the specification applies the winding rule to the written ones. Such a ring has an area of zero or close to it.
- With clipping, a feature written as several features by either writer, from a collection returned by the clip. java-vector-tile runs its steps before the clip again on the members of the collection, and tileverse does not.
- A feature written by java-vector-tile with a geometry decoded as empty: a line collapsed to a lone MoveTo, or a MultiPoint with repeated points, written with a wrong MoveTo count.

The rules on rings check the polygons handed to the encoders: the input polygons once filtered, simplified and clipped, as set by the configuration, in the units of the input. The test computes them with the pipeline of the tileverse builder of the configuration, without its `PolygonRepairStep`. java-vector-tile runs the same JTS operations, and repairs a polygon after encoding it. The rules multiply the coordinates by the input scale before rounding them, as both encoders do.

java-vector-tile repairs a polygon in `encode()`, only with simplification, when the commands of the polygon decode to an invalid polygon. In `CLIP_SIMPLIFY`, the test writes the tile of java-vector-tile with `RepairRecordingEncoder`, a subclass of its encoder. The repair method receives the clipped and simplified geometry of a feature, without its layer and its id. The subclass records the positions of the repaired features in the tile instead: `encode()` calls the method once per feature in tile order, and leaves out a feature when the method returns no commands. The test reads the layers and the ids of the features at these positions in the decoded tile.

The test prints the counts of the comparison per tile and configuration. It fails on a feature written differently by the two writers, and when it leaves out more than 10% of the input features.

### Tile under construction size

For each tile under construction, a tile server pays the heap held by its writer until the tile is encoded. `TileUnderConstructionSize`, a program of this module run by `run-benchmarks.sh` after `DecodedTileSize`, measures this heap per tile and writer, in the encoding-only configuration. It adds the features of the tile to copies of both writers, and divides the growth of the used heap after a full garbage collection by the number of copies, as `DecodedTileSize` does.

- tileverse writes a feature into the buffer of its layer when the feature is added, and writes the layer into the buffer of the tile at the build of the layer. The copies build a layer when the next one starts, and leave the last layer open, as after the last feature of a tile.
- java-vector-tile keeps the geometries of the features until it encodes the tile. Its copies receive copies of the geometries, as the tiles of a server receive geometries of their own.

The copies and the decoded features share the keys and values of the attributes, and the measured sizes leave them out. The table gives the heap reported by `VectorTileBuilder.heapSize()` next to the measured heap of tileverse. `heapSize()` counts the buffers of the builder by the length of their arrays, and estimates the tables of keys and values of the open layer, keys and values included. The measurement runs with the G1 collector and its regions of 2 MiB, as for the decoded tile sizes. G1 allocates an array of half a region or more in whole regions, and the used heap counts these regions whole. A buffer of 1 MiB or more can hold up to a region more than its length, and the builder more heap than `heapSize()` reports.

### Writer results

Average time per operation (`avgt`) and allocation per operation (`gc.alloc.rate.norm`) of `write`, measured with OpenJDK 25.0.5 (Temurin 25.0.5+8-LTS), JMH 1.37 and the default JMH options of `run-benchmarks.sh`, `-f 2 -prof gc -foe true`, on `rei`: an Apple M4 Max (12 performance and 4 efficiency cores) with 48 GB, under macOS 15.8. The configurations ran one at a time, with this command and the name of each configuration:

```bash
internal/vectortiles-benchmarks/run-benchmarks.sh -f 2 -prof gc -foe true -p configuration=ENCODING_ONLY VectorTileWriteBenchmark
```

JMH times each `write` operation on its own, because a setup clears the coordinates kept by the packed sequences before each operation. It times the decode benchmarks over batches of operations.

In `ENCODING_ONLY`, tileverse takes 26% to 58% less time than java-vector-tile and allocates 55% to 82% less, with array sequences. With packed sequences, it takes 34% to 57% less time and allocates 57% to 86% less. In `DEFAULTS`, `CLIP_NO_BUFFER` and `CLIP_SIMPLIFY`, both writers run the same JTS operations, and tileverse takes 1% to 52% less time and allocates 6% to 56% less, with either sequence. In `AUTO_SCALE`, tileverse takes 19% to 64% less time and allocates 55% to 86% less. In `ENCODING_ONLY`, java-vector-tile allocates 4% to 48% more with packed sequences than with array sequences, for the `Coordinate` objects created by `getCoordinates()`, and tileverse allocates the same. In the four other configurations, tileverse allocates up to 27% more with packed sequences, and java-vector-tile up to 48% more. The errors of JMH stay below 20% of the scores.

| Tile | Configuration | Writer | Time on `rei`, `ARRAY` (us/op) | Time on `rei`, `PACKED` (us/op) | Allocation, `ARRAY` (KiB/op) | Allocation, `PACKED` (KiB/op) |
|---|---|---|---:|---:|---:|---:|
| `osm-qa-montevideo/12-1408-2471.mvt` | `ENCODING_ONLY` | java-vector-tile | 64,286 | 62,953 | 144,774 | 149,949 |
| `osm-qa-montevideo/12-1408-2471.mvt` | `ENCODING_ONLY` | tileverse | 41,151 | 40,473 | 65,189 | 65,189 |
| `chicago/13-2101-3044.mvt` | `ENCODING_ONLY` | java-vector-tile | 608 | 657 | 1,996 | 2,613 |
| `chicago/13-2101-3044.mvt` | `ENCODING_ONLY` | tileverse | 325 | 323 | 482 | 482 |
| `bing/4-12-6.mvt` | `ENCODING_ONLY` | java-vector-tile | 3,396 | 3,822 | 13,338 | 19,776 |
| `bing/4-12-6.mvt` | `ENCODING_ONLY` | tileverse | 1,441 | 1,643 | 4,267 | 4,267 |
| `amazon/5_6_21.mvt` | `ENCODING_ONLY` | java-vector-tile | 3,043 | 3,319 | 14,977 | 20,134 |
| `amazon/5_6_21.mvt` | `ENCODING_ONLY` | tileverse | 2,245 | 2,022 | 3,688 | 3,688 |
| `amazon_here/4_9_4.mvt` | `ENCODING_ONLY` | java-vector-tile | 1,423 | 1,524 | 8,317 | 10,701 |
| `amazon_here/4_9_4.mvt` | `ENCODING_ONLY` | tileverse | 1,042 | 1,013 | 1,477 | 1,477 |
| `omt/4_8_10.mvt` | `ENCODING_ONLY` | java-vector-tile | 11,033 | 11,947 | 35,909 | 45,624 |
| `omt/4_8_10.mvt` | `ENCODING_ONLY` | tileverse | 6,501 | 6,344 | 10,891 | 10,891 |
| `omt/14_8298_10748.mvt` | `ENCODING_ONLY` | java-vector-tile | 7,043 | 7,338 | 20,363 | 23,959 |
| `omt/14_8298_10748.mvt` | `ENCODING_ONLY` | tileverse | 4,462 | 4,502 | 4,835 | 4,835 |
| `osm-qa-montevideo/12-1408-2471.mvt` | `DEFAULTS` | java-vector-tile | 63,770 | 60,895 | 144,774 | 149,949 |
| `osm-qa-montevideo/12-1408-2471.mvt` | `DEFAULTS` | tileverse | 41,293 | 41,446 | 65,459 | 65,459 |
| `chicago/13-2101-3044.mvt` | `DEFAULTS` | java-vector-tile | 6,558 | 6,703 | 14,785 | 15,930 |
| `chicago/13-2101-3044.mvt` | `DEFAULTS` | tileverse | 6,234 | 6,305 | 13,732 | 14,030 |
| `bing/4-12-6.mvt` | `DEFAULTS` | java-vector-tile | 235,621 | 233,655 | 222,074 | 226,077 |
| `bing/4-12-6.mvt` | `DEFAULTS` | tileverse | 226,493 | 228,262 | 208,138 | 213,060 |
| `amazon/5_6_21.mvt` | `DEFAULTS` | java-vector-tile | 100,189 | 99,950 | 71,197 | 76,359 |
| `amazon/5_6_21.mvt` | `DEFAULTS` | tileverse | 98,204 | 97,343 | 58,232 | 63,240 |
| `amazon_here/4_9_4.mvt` | `DEFAULTS` | java-vector-tile | 99,001 | 98,504 | 54,203 | 58,930 |
| `amazon_here/4_9_4.mvt` | `DEFAULTS` | tileverse | 97,148 | 97,586 | 47,148 | 51,935 |
| `omt/4_8_10.mvt` | `DEFAULTS` | java-vector-tile | 16,499 | 17,097 | 54,739 | 64,420 |
| `omt/4_8_10.mvt` | `DEFAULTS` | tileverse | 11,315 | 12,908 | 29,703 | 30,005 |
| `omt/14_8298_10748.mvt` | `DEFAULTS` | java-vector-tile | 11,658 | 11,732 | 32,837 | 36,883 |
| `omt/14_8298_10748.mvt` | `DEFAULTS` | tileverse | 10,019 | 10,232 | 21,121 | 21,471 |
| `osm-qa-montevideo/12-1408-2471.mvt` | `CLIP_NO_BUFFER` | java-vector-tile | 63,853 | 65,602 | 144,774 | 149,949 |
| `osm-qa-montevideo/12-1408-2471.mvt` | `CLIP_NO_BUFFER` | tileverse | 41,012 | 40,845 | 65,459 | 65,324 |
| `chicago/13-2101-3044.mvt` | `CLIP_NO_BUFFER` | java-vector-tile | 6,717 | 6,743 | 15,492 | 16,116 |
| `chicago/13-2101-3044.mvt` | `CLIP_NO_BUFFER` | tileverse | 6,287 | 6,379 | 13,907 | 14,205 |
| `bing/4-12-6.mvt` | `CLIP_NO_BUFFER` | java-vector-tile | 244,996 | 250,325 | 233,960 | 242,339 |
| `bing/4-12-6.mvt` | `CLIP_NO_BUFFER` | tileverse | 239,952 | 236,880 | 220,023 | 225,573 |
| `amazon/5_6_21.mvt` | `CLIP_NO_BUFFER` | java-vector-tile | 100,682 | 100,980 | 72,024 | 77,149 |
| `amazon/5_6_21.mvt` | `CLIP_NO_BUFFER` | tileverse | 98,064 | 97,445 | 58,584 | 63,594 |
| `amazon_here/4_9_4.mvt` | `CLIP_NO_BUFFER` | java-vector-tile | 123,671 | 125,778 | 54,524 | 59,226 |
| `amazon_here/4_9_4.mvt` | `CLIP_NO_BUFFER` | tileverse | 122,134 | 121,590 | 47,421 | 52,186 |
| `omt/4_8_10.mvt` | `CLIP_NO_BUFFER` | java-vector-tile | 16,622 | 18,040 | 56,448 | 66,413 |
| `omt/4_8_10.mvt` | `CLIP_NO_BUFFER` | tileverse | 11,651 | 13,192 | 31,378 | 31,834 |
| `omt/14_8298_10748.mvt` | `CLIP_NO_BUFFER` | java-vector-tile | 11,761 | 11,984 | 33,448 | 38,048 |
| `omt/14_8298_10748.mvt` | `CLIP_NO_BUFFER` | tileverse | 10,168 | 10,508 | 21,808 | 22,040 |
| `osm-qa-montevideo/12-1408-2471.mvt` | `CLIP_SIMPLIFY` | java-vector-tile | 65,074 | 70,336 | 158,091 | 165,405 |
| `osm-qa-montevideo/12-1408-2471.mvt` | `CLIP_SIMPLIFY` | tileverse | 46,473 | 49,631 | 76,433 | 88,708 |
| `chicago/13-2101-3044.mvt` | `CLIP_SIMPLIFY` | java-vector-tile | 7,913 | 8,315 | 19,298 | 21,621 |
| `chicago/13-2101-3044.mvt` | `CLIP_SIMPLIFY` | tileverse | 7,489 | 7,691 | 17,442 | 19,542 |
| `bing/4-12-6.mvt` | `CLIP_SIMPLIFY` | java-vector-tile | 227,244 | 228,580 | 237,882 | 253,792 |
| `bing/4-12-6.mvt` | `CLIP_SIMPLIFY` | tileverse | 218,036 | 218,005 | 213,042 | 228,925 |
| `amazon/5_6_21.mvt` | `CLIP_SIMPLIFY` | java-vector-tile | 203,627 | 202,989 | 153,469 | 167,751 |
| `amazon/5_6_21.mvt` | `CLIP_SIMPLIFY` | tileverse | 171,913 | 171,613 | 118,358 | 132,624 |
| `amazon_here/4_9_4.mvt` | `CLIP_SIMPLIFY` | java-vector-tile | 208,048 | 209,696 | 188,594 | 194,993 |
| `amazon_here/4_9_4.mvt` | `CLIP_SIMPLIFY` | tileverse | 100,754 | 100,263 | 90,343 | 97,001 |
| `omt/4_8_10.mvt` | `CLIP_SIMPLIFY` | java-vector-tile | 20,123 | 31,372 | 89,422 | 113,743 |
| `omt/4_8_10.mvt` | `CLIP_SIMPLIFY` | tileverse | 17,836 | 22,160 | 64,251 | 81,642 |
| `omt/14_8298_10748.mvt` | `CLIP_SIMPLIFY` | java-vector-tile | 28,995 | 31,009 | 77,865 | 86,732 |
| `omt/14_8298_10748.mvt` | `CLIP_SIMPLIFY` | tileverse | 27,060 | 28,376 | 62,959 | 72,303 |
| `osm-qa-montevideo/12-1408-2471.mvt` | `AUTO_SCALE` | java-vector-tile | 60,617 | 62,570 | 144,774 | 149,949 |
| `osm-qa-montevideo/12-1408-2471.mvt` | `AUTO_SCALE` | tileverse | 41,169 | 41,422 | 65,460 | 65,460 |
| `chicago/13-2101-3044.mvt` | `AUTO_SCALE` | java-vector-tile | 636 | 662 | 1,934 | 2,551 |
| `chicago/13-2101-3044.mvt` | `AUTO_SCALE` | tileverse | 335 | 320 | 482 | 482 |
| `bing/4-12-6.mvt` | `AUTO_SCALE` | java-vector-tile | 3,389 | 3,620 | 13,338 | 19,776 |
| `bing/4-12-6.mvt` | `AUTO_SCALE` | tileverse | 1,281 | 1,299 | 4,235 | 4,235 |
| `amazon/5_6_21.mvt` | `AUTO_SCALE` | java-vector-tile | 3,091 | 3,373 | 14,983 | 20,140 |
| `amazon/5_6_21.mvt` | `AUTO_SCALE` | tileverse | 2,489 | 2,368 | 3,689 | 3,689 |
| `amazon_here/4_9_4.mvt` | `AUTO_SCALE` | java-vector-tile | 1,449 | 1,551 | 8,317 | 10,701 |
| `amazon_here/4_9_4.mvt` | `AUTO_SCALE` | tileverse | 971 | 953 | 1,479 | 1,546 |
| `omt/4_8_10.mvt` | `AUTO_SCALE` | java-vector-tile | 10,409 | 12,718 | 33,945 | 43,647 |
| `omt/4_8_10.mvt` | `AUTO_SCALE` | tileverse | 6,827 | 6,537 | 10,848 | 10,848 |
| `omt/14_8298_10748.mvt` | `AUTO_SCALE` | java-vector-tile | 5,524 | 5,765 | 15,996 | 19,412 |
| `omt/14_8298_10748.mvt` | `AUTO_SCALE` | tileverse | 4,010 | 3,732 | 4,150 | 4,150 |

Heap held by a tile under construction, measured as described in [Tile under construction size](#tile-under-construction-size), with the same JDK. tileverse holds 39% to 84% less heap than java-vector-tile. Runs of the measurement differ by up to 12 KiB.

| Tile | java-vector-tile | tileverse measured | tileverse `heapSize()` | Heap delta |
|---|---:|---:|---:|---:|
| `osm-qa-montevideo/12-1408-2471.mvt` | 44,960 KiB | 27,403 KiB | 25,510 KiB | -39% |
| `chicago/13-2101-3044.mvt` | 948 KiB | 187 KiB | 185 KiB | -80% |
| `bing/4-12-6.mvt` | 7,246 KiB | 3,114 KiB | 2,089 KiB | -57% |
| `amazon/5_6_21.mvt` | 5,542 KiB | 1,395 KiB | 1,377 KiB | -75% |
| `amazon_here/4_9_4.mvt` | 2,823 KiB | 527 KiB | 553 KiB | -81% |
| `omt/4_8_10.mvt` | 18,547 KiB | 8,560 KiB | 5,312 KiB | -54% |
| `omt/14_8298_10748.mvt` | 8,896 KiB | 1,438 KiB | 1,621 KiB | -84% |

Time and allocation of `checkPolygons`, measured with the same JDK and options as `write`:

```bash
internal/vectortiles-benchmarks/run-benchmarks.sh -f 2 -prof gc -foe true PolygonValidityBenchmark
```

`ROUNDED_VALIDITY` allocates 28% to 46% less than `PRECISION_REDUCER_IS_VALID`, and 8% to 19% less than `IS_VALID`. Its time is between 18% less and 1% more than the time of `PRECISION_REDUCER_IS_VALID`, and between 10% less and 9% more than the time of `IS_VALID`. The copies of `COPY_ONLY` take up to 4% of the time of `ROUNDED_VALIDITY`.

| Tile | Check | Time on `rei` (us/op) | Allocation (KiB/op) |
|---|---|---:|---:|
| `osm-qa-montevideo/12-1408-2471.mvt` | `ROUNDED_VALIDITY` | 1,247 | 5,494 |
| `osm-qa-montevideo/12-1408-2471.mvt` | `PRECISION_REDUCER_IS_VALID` | 1,523 | 8,405 |
| `osm-qa-montevideo/12-1408-2471.mvt` | `IS_VALID` | 1,386 | 6,034 |
| `osm-qa-montevideo/12-1408-2471.mvt` | `COPY_ONLY` | 31 | 550 |
| `chicago/13-2101-3044.mvt` | `ROUNDED_VALIDITY` | 335 | 1,524 |
| `chicago/13-2101-3044.mvt` | `PRECISION_REDUCER_IS_VALID` | 332 | 2,108 |
| `chicago/13-2101-3044.mvt` | `IS_VALID` | 307 | 1,662 |
| `chicago/13-2101-3044.mvt` | `COPY_ONLY` | 10 | 129 |
| `bing/4-12-6.mvt` | `ROUNDED_VALIDITY` | 171 | 708 |
| `bing/4-12-6.mvt` | `PRECISION_REDUCER_IS_VALID` | 208 | 1,317 |
| `bing/4-12-6.mvt` | `IS_VALID` | 183 | 859 |
| `bing/4-12-6.mvt` | `COPY_ONLY` | 6 | 104 |
| `amazon/5_6_21.mvt` | `ROUNDED_VALIDITY` | 37,041 | 30,525 |
| `amazon/5_6_21.mvt` | `PRECISION_REDUCER_IS_VALID` | 37,856 | 46,126 |
| `amazon/5_6_21.mvt` | `IS_VALID` | 37,501 | 34,928 |
| `amazon/5_6_21.mvt` | `COPY_ONLY` | 53 | 1,850 |
| `amazon_here/4_9_4.mvt` | `ROUNDED_VALIDITY` | 5,483 | 8,667 |
| `amazon_here/4_9_4.mvt` | `PRECISION_REDUCER_IS_VALID` | 5,800 | 15,940 |
| `amazon_here/4_9_4.mvt` | `IS_VALID` | 5,553 | 10,638 |
| `amazon_here/4_9_4.mvt` | `COPY_ONLY` | 35 | 931 |
| `omt/4_8_10.mvt` | `ROUNDED_VALIDITY` | 364 | 1,108 |
| `omt/4_8_10.mvt` | `PRECISION_REDUCER_IS_VALID` | 395 | 1,673 |
| `omt/4_8_10.mvt` | `IS_VALID` | 363 | 1,246 |
| `omt/4_8_10.mvt` | `COPY_ONLY` | 4 | 88 |
| `omt/14_8298_10748.mvt` | `ROUNDED_VALIDITY` | 7,003 | 20,398 |
| `omt/14_8298_10748.mvt` | `PRECISION_REDUCER_IS_VALID` | 7,450 | 28,412 |
| `omt/14_8298_10748.mvt` | `IS_VALID` | 6,988 | 22,052 |
| `omt/14_8298_10748.mvt` | `COPY_ONLY` | 107 | 1,567 |
