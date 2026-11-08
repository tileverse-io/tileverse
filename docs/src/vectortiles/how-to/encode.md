# Encoding Vector Tiles

`VectorTileBuilder` writes Mapbox Vector Tiles from JTS `Geometry` instances, one layer at a time. The geometry of a feature goes through a pipeline of steps, such as a clip to the tile. The builder then rounds its coordinates to integers and writes the feature into its layer.

## Build a tile

```java
import io.tileverse.vectortile.mvt.VectorTileBuilder;
import java.util.Map;

VectorTileBuilder builder = new VectorTileBuilder();

builder.layer()
        .name("buildings")
        .feature()
            .id(123)
            .geometry(buildingPolygon)
            .attribute("height", 50)
            .attribute("type", "residential")
            .build()       // writes the feature, returns the LayerBuilder
        .build();          // writes the layer, returns the VectorTileBuilder

builder.layer()
        .name("roads")
        .feature(Map.of("highway", "primary"), road)
        .build();

byte[] tile = builder.toByteArray();
```

`LayerBuilder.feature()` opens a `FeatureBuilder`. Chain `.id(...)`, `.geometry(...)`, `.attribute(...)` and `.attributes(Map)`, then call `.build()`. It runs the geometry through the pipeline, writes the feature into the layer, and returns the `LayerBuilder` for the next feature. An attribute with a `null` value is left out.

The `LayerBuilder.feature(Map, Geometry)` and `feature(Map, Geometry, long id)` overloads skip the feature builder, for example for rows read from another source:

```java
import io.tileverse.vectortile.mvt.LayerBuilder;

LayerBuilder roads = builder.layer().name("roads");
rows.forEach(row -> roads.feature(row.attributes(), row.geometry()));
roads.build();
```

A tile is written one layer at a time:

- The features of a layer are added between `layer()` and `LayerBuilder.build()`, and the build writes the layer into the tile. A layer needs a name: `build()` fails with an `IllegalStateException` without one.
- A tile holds one layer per name, as required by the MVT specification. `name(...)` fails with an `IllegalArgumentException` for the name of a layer written before. It fails the same way on a name with an unpaired surrogate: UTF-8 has no encoding for it, and two such names could be written as one.
- A layer with features must be built before the next layer starts, and before the tile is written. Otherwise `layer()`, `toByteArray()`, `writeTo(...)` and `build()` fail with an `IllegalStateException`. A layer without features is left out of the tile, and leaves its name free.
- Once a layer is built or the next layer started, its layer builder and its feature builder fail with an `IllegalStateException`.
- A `GeometryCollection` gives one feature per member, with the id and the attributes of the feature.
- A feature is written whole or not at all. When a step or the encoding fails, for example on a NaN ordinate, the members of a collection written before the failure are removed from the layer, and the automatic ids stay as they were. The exception goes to the caller. The keys and values added by the feature stay in the tables of the layer: the layer stays valid, with these entries unused.

## Write the bytes

Three methods end a tile: they give the tile, then clear the builder for the next tile.

```java
import io.tileverse.vectortile.model.VectorTile;

byte[] bytes = builder.toByteArray();   // the encoded tile
builder.writeTo(outputStream);          // or: the encoded tile, written to the stream
VectorTile tile = builder.build();      // or: the tile as a VectorTile
```

- `toByteArray()` returns the encoded tile.
- `writeTo(OutputStream)` writes the encoded tile to the stream without copying it first, for example to an HTTP response. The builder is cleared also when the stream fails.
- `build()` returns the tile as a `VectorTile`. It reads the encoded tile as `VectorTileCodec.decode` does, without a copy of the bytes. Use it to read the features of the tile, or to keep the tile in a cache of decoded tiles. `toByteArray()` and `writeTo(...)` skip this reading.

The cleared builder keeps its settings, its pipeline and the capacity of its buffers. Its layers and their names are removed, and the automatic ids restart at the first id.

`VectorTileCodec` writes the bytes of a `VectorTile`:

```java
import io.tileverse.vectortile.mvt.VectorTileCodec;

VectorTileCodec codec = new VectorTileCodec();

byte[] bytes = codec.encode(tile);      // a copy of the bytes
codec.encode(tile, outputStream);       // streaming
codec.encode(tile, byteBuffer);         // into a buffer with room for codec.getSerializedSize(tile) bytes
```

A tile decoded by the codec or built by `VectorTileBuilder` holds its encoded bytes, and the codec writes them as they are. `encode(VectorTile, ByteBuffer)` fails with an `InsufficientBufferException` when the buffer has less room than the tile. The codec also encodes the tiles of other `VectorTile` implementations, as described in [Tiles from tiles](#tiles-from-tiles).

## Choose the steps

The builder runs the geometry of a feature through its pipeline before writing it. A `GeometryPipeline` runs its `GeometryStep`s in order. The package `io.tileverse.vectortile.pipeline` has these steps:

| Step | What it does |
|---|---|
| `SizeFilterStep(minimumLength, minimumArea)` | Drops a `Polygon` or a `MultiPolygon` of area below the minimum area, and a `LineString` shorter than the minimum length. Points and `MultiLineString`s pass, as in java-vector-tile. |
| `SimplificationStep(distanceTolerance)` | Simplifies the geometries as java-vector-tile does. Lines and polygons go through the Douglas-Peucker simplifier of JTS. A polygon goes through the topology-preserving simplifier of JTS instead when its Douglas-Peucker result is not polygonal. A `Point` is returned as is. Other geometries, such as a `MultiPoint`, go through the topology-preserving simplifier. |
| `ClipStep(extent, buffer)` | Clips the geometries to the area from `-buffer` to `extent + buffer` on both axes, and drops the features outside it. |
| `GridSnapStep()` | Snaps the coordinates to the integer grid with the precision reducer of JTS, and removes the parts collapsed by the snapping. A valid polygon stays valid. |
| `PolygonRepairStep()` | Repairs the polygons invalid once their coordinates are rounded. It snaps such a polygon to the integer grid, then fixes it with the `GeometryFixer` of JTS if it is still invalid. |

`RoundedValidity`, the check of the repair step, tells whether a polygon is valid once its coordinates are rounded.

The builder derives its pipeline from its [settings](#settings). With the default settings, the pipeline holds a `SizeFilterStep(1, 1)` and a `ClipStep(4096, 32)`. It drops the lines shorter than one tile unit and the polygons of area below one square tile unit. It then clips the geometries to the tile and a band of 32 tile units around it.

`setPipeline` replaces the derived pipeline. The encoding-only configuration keeps the size filter alone, and clips and simplifies nothing:

```java
import io.tileverse.vectortile.pipeline.GeometryPipeline;
import io.tileverse.vectortile.pipeline.SizeFilterStep;

builder.setPipeline(GeometryPipeline.of(new SizeFilterStep(1, 1)));
```

A step of your own is a lambda, or a class implementing `GeometryStep`. It returns the geometry for the remaining steps, or `null` to drop the feature:

```java
import io.tileverse.vectortile.pipeline.ClipStep;
import io.tileverse.vectortile.pipeline.GeometryStep;
import org.locationtech.jts.geom.Puntal;

GeometryStep withoutPoints = geometry -> geometry instanceof Puntal ? null : geometry;
builder.setPipeline(GeometryPipeline.of(new SizeFilterStep(1, 1), withoutPoints, new ClipStep(4096, 32)));
```

- A step never receives `null`, an empty geometry, or a `GeometryCollection` of that exact class. An empty result drops the feature, as `null` does.
- A `GeometryCollection` returned by a step is split, and its members go through the remaining steps one by one.
- The steps receive the geometries in the coordinates given to the builder: tile coordinates, or the units of the input with an [input scale](#geometries-at-another-scale). Their parameters are in the same units.
- A pipeline is immutable, and its steps must be safe to share between threads. The builders of several threads can share one pipeline, and a builder belongs to one thread.
- A step cannot change the tile under construction. A call from a step to the builder, or to a layer or feature builder of the tile, fails with an `IllegalStateException` when it would change the tile, the pipeline or a setting.
- `GeometryPipeline.empty()` has no steps, and the builder writes the geometries as given, small ones included.
- A later call to `setExtent`, `setClipBuffer`, `setInputScale`, `setSimplificationDistanceTolerance` or `setUsePrecisionModelSnapping` derives the pipeline from the settings again: the last call wins. Call `setPipeline` after the settings. `init` takes the extent of a tile and keeps the given pipeline.
- `pipeline()` returns the pipeline of the builder.

## Settings

A feature goes through the pipeline when its `FeatureBuilder.build()` runs, with the settings in effect at that moment. Set them before adding features:

```java
VectorTileBuilder builder = new VectorTileBuilder()
        .setExtent(4096)                            // the extent of the layers (default 4096)
        .setClipBuffer(64)                          // the band kept around the tile, in tile units (default 32)
        .setSimplificationDistanceTolerance(1.0)    // Douglas-Peucker tolerance in tile units (default -1, none)
        .setUsePrecisionModelSnapping(false);       // round the coordinates (default), or snap them
```

| Setting | Default | Steps |
|---|---|---|
| `setExtent(int)` | 4096 | The extent of the layers and of the `ClipStep` |
| `setClipBuffer(int)` | 32 | The buffer of the `ClipStep`, in tile units: 0 clips at the edges of the tile |
| `setSimplificationDistanceTolerance(double)` | -1 | A positive tolerance adds a `SimplificationStep` after the size filter, and a `PolygonRepairStep` at the end |
| `setUsePrecisionModelSnapping(boolean)` | false | True adds a `GridSnapStep` after the clip |
| `setInputScale(double)` | 1 | The steps take the other settings divided by the scale, as described in [Geometries at another scale](#geometries-at-another-scale) |

A layer is written with one extent, when it is built. While the open layer has features, `setExtent` with another extent fails with an `IllegalStateException`: build the layer first.

At the input scale 1, the derived pipeline runs in this order: `SizeFilterStep(1, 1)`, a `SimplificationStep` when the tolerance is positive, `ClipStep(extent, clipBuffer)`, a `GridSnapStep` when snapping is on, and a `PolygonRepairStep` when the tolerance is positive.

With snapping off, the builder rounds the coordinates when it writes them. Rounding can make a polygon invalid, for example when a hole touches its shell once rounded. As in java-vector-tile, the polygons are repaired only when they are simplified: a positive tolerance adds the repair step. With snapping on, the `GridSnapStep` snaps the polygons to the integer grid: a valid polygon stays valid, and polygons closer than one tile unit can merge.

`setExtent(int)` fails with an `IllegalArgumentException` below 1, and `setClipBuffer(int)` below 0. `setInputScale(double)` fails the same way unless the scale is positive and finite, and `setSimplificationDistanceTolerance(double)` unless the tolerance is finite. A setting also fails when a step rejects it once divided by the input scale, for example an extent that becomes infinite with a scale close to 0. A failed setting leaves the settings and the pipeline as they were.

## Feature ids

```java
builder.layer()
        .name("pois")
        .feature(Map.of("name", "a"), pointA, 50)   // id 50
        .feature(Map.of("name", "b"), pointB)       // automatic id 51
        .feature(Map.of("name", "c"), pointC, -1)   // a negative id means none: automatic id 52
        .build();
```

- Automatic ids are on by default. A feature without an id takes the next automatic id.
- The automatic ids of a tile start at the first id, 0 by default, set by `setFirstFeatureId(long)`. They continue after the largest id of the tile, given or automatic: after a feature with id 50, the next automatic id is 51. They restart at the first id for the next tile.
- A negative id means no id. With `setAutoIncrementIds(false)`, a feature without an id is written without one. `setFirstFeatureId` fails with an `IllegalArgumentException` on a negative id.
- No automatic id comes after the id 2^63 - 1, the largest long. Once the tile has it, a feature without an id fails with an `IllegalStateException` until the next tile.
- A feature takes its id before the pipeline runs: a feature dropped by a step consumes its id. The members of a `GeometryCollection` share the id of their feature. The specification recommends unique ids in a layer. The members are the parts of one feature, and a client selecting the feature by its id selects them all.
- MVT ids are unsigned 64-bit integers. `FeatureBuilder.id(long)` writes the ids from 0 to `Long.MAX_VALUE`. A decoded feature with an id of 2^63 or above returns it from `getId()` as a negative long. `LayerBuilder.merge` writes such an id as it is, and leaves the automatic ids as they are: they stay below 2^63.

## Geometries at another scale

`setInputScale(double)` gives the number of tile units in one unit of the geometries, 1 by default. The builder multiplies the coordinates by the scale when it rounds them, and makes no scaled copy of the geometries. A scale of 16 writes geometries from a grid of 256 units into a tile of extent 4096:

```java
VectorTileBuilder builder = new VectorTileBuilder()
        .setInputScale(16)
        .setClipBuffer(8 * 16);   // a band of 8 units of the input, given in tile units
```

The geometries go through the pipeline as given, in units of the input. The extent, the clip buffer and the simplification tolerance stay in tile units, and the derived steps take them divided by the scale. The builder above derives a `SizeFilterStep(1 / 16.0, 1 / 256.0)` and a `ClipStep(256, 8)`.

java-vector-tile's `autoScale` at extent 4096 is `setInputScale(16)`. java-vector-tile takes its clip buffer and its simplification tolerance in units of the input, and the builder in tile units: multiply them by 16, as `setClipBuffer(8 * 16)` does above.

The steps of a pipeline given with `setPipeline` also receive the geometries in units of the input. Give their parameters in these units, and give the scale to the steps working on the integer grid of the tile: `GridSnapStep(16)` and `PolygonRepairStep(16)`. Set the scale before the pipeline, because `setInputScale` derives the pipeline again:

```java
import io.tileverse.vectortile.pipeline.GridSnapStep;

builder.setInputScale(16)
        .setPipeline(GeometryPipeline.of(
                new SizeFilterStep(1 / 16.0, 1 / 256.0),   // one tile unit, in units of the input
                new ClipStep(256, 2),                      // the tile and a band of 2 units of the input
                new GridSnapStep(16)));                    // the integer grid of the tile
```

The layers merged from other tiles are scaled by their extent, not by the input scale.

## Tiles from tiles

`init` starts a tile from an existing one, and `merge` adds the layers of another tile:

```java
VectorTileCodec codec = new VectorTileCodec();
VectorTile base = codec.decode(baseBytes);
VectorTile overlay = codec.decode(overlayBytes);

byte[] combined = new VectorTileBuilder()
        .init(base)          // the extent and the layers of base
        .merge(overlay)      // the layers of overlay
        .toByteArray();
```

`LayerBuilder.merge` adds the features of a layer of another tile to the open layer, next to features of your own:

```java
builder.layer()
        .name("pois")
        .merge(tileA.getLayer("pois").orElseThrow())
        .merge(tileB.getLayer("pois").orElseThrow())
        .feature(Map.of("name", "added"), point)
        .build();
```

- `LayerBuilder.merge(Layer)` adds the features of the layer in their order, without the steps of the pipeline. The features keep their ids and their attributes. A feature without an id stays without one, also with automatic ids on. A merged id moves the automatic ids of the tile past it, also when its feature is left out, as the id of a feature dropped by a step does. A feature without geometry commands, or with a geometry read as empty, such as a malformed one, is left out.
- The builder writes the geometries in its own extent: a layer of another extent has its coordinates scaled, then rounded. For a layer of a decoded or built tile with the extent of the builder and version 2 of the specification, the builder copies the geometry bytes of the features as they are. A polygon starting with a ring of negative area is encoded again, because the specification wants a positive area for an exterior ring.
- The builder reads a layer of another `VectorTile` implementation through the model. A feature read through the model with the id 0 is written without an id, because the model returns 0 for a feature without an id. Another id is written as its unsigned value: a negative id stands for an id of 2^63 or above, as for a decoded feature. A feature of a decoded or built tile keeps the id written in its tile, the id 0 included, also when such a layer hands it out.
- A layer is merged whole or not at all. When a feature fails, the features merged by the call are removed and the automatic ids stay as they were. The exception goes to the caller. The keys and values added by the merged features stay in the tables of the layer, unused.
- `merge(VectorTile)` adds the layers of the tile as layers of the same names. It checks the names first, and fails with an `IllegalArgumentException` on a layer without a name, on two layers of one name, on the name of a layer written in the builder, or on a name with an unpaired surrogate. A failed check leaves the builder as it was.
- `init(VectorTile)` merges the layers of the tile as `merge` does. When the builder has no layer written yet and the layers of the tile agree on an extent, `init` first takes this extent, and derives the pipeline for it as `setExtent` does. A pipeline given with `setPipeline` stays as given: its steps are yours, a `ClipStep` with its own extent included.
- `merge` and `init` fail with an `IllegalStateException` while a layer with features is open, as `layer()` does.

`VectorTileCodec.encode` accepts the tiles of other `VectorTile` implementations. It writes their layers one at a time, in their own extents, as `LayerBuilder.merge` writes the features: without the steps of a pipeline, with the coordinates rounded, and without an id for a feature of id 0 read through the model. The codec fails with an `IllegalArgumentException` on a layer without a name, on two layers of one name, also when one of them has no features, and on a name with an unpaired surrogate.

## Settings of java-vector-tile

The table gives encoders of [java-vector-tile](https://github.com/ElectronicChartCentre/java-vector-tile) 1.4.2 and the builders with the same settings:

| java-vector-tile | VectorTileBuilder |
|---|---|
| `new VectorTileEncoder(4096, 8, false)` | `new VectorTileBuilder().setClipBuffer(8).setAutoIncrementIds(false)` |
| `new VectorTileEncoder(4096, 8, false, true)` | `new VectorTileBuilder().setClipBuffer(8).setAutoIncrementIds(true).setFirstFeatureId(1)` |
| `new VectorTileEncoder(4096, 8, false, false, 2.0)` | `new VectorTileBuilder().setClipBuffer(8).setAutoIncrementIds(false).setSimplificationDistanceTolerance(2.0)` |
| `new VectorTileEncoder()` | `new VectorTileBuilder().setInputScale(16).setClipBuffer(8 * 16).setAutoIncrementIds(false)` |

- java-vector-tile numbers its automatic ids from 1, and `setFirstFeatureId(1)` does the same. java-vector-tile moves its automatic ids past a given id only when it writes the feature, and the builder also when a step drops the feature. When the first feature of a tile has the id 50 and the clip drops it, java-vector-tile gives the next feature without an id the id 1, and the builder the id 51.
- `new VectorTileEncoder()` has `autoScale` on, for geometries in a grid of 256 units, as described in [Geometries at another scale](#geometries-at-another-scale).
- java-vector-tile takes the features of its layers in any order, and the builder one layer at a time: group the features by layer first.

Both libraries then run the same size filter, simplification and clip on the geometries. The [parity test](https://github.com/tileverse-io/tileverse/blob/main/internal/vectortiles-benchmarks/README.md#parity-test) of the benchmarks compares the written tiles, and lists their differences, such as the polygons repaired once rounded. The [Performance](../index.md#performance) section of the overview sums up the time and the memory of both writers.

## Memory guard

`heapSize()` returns the heap held by the tile under construction, in bytes. A memory guard can read it instead of estimating the size of the features:

```java
roads.build();
if (builder.heapSize() > budget) {
    // stop adding layers, or fail the request
}
```

The size counts the capacity of the buffers, not their content: the buffer of the tile, the buffers of the open layer, and the arrays kept to write a feature or to merge a layer. It adds an estimate of the keys and values of the open layer, and leaves out the builder objects. The buffers keep their capacity for the next tile.

The builder counts a key or a value when it joins the tables of the layer. A call costs no walk of the tables, and a guard can read the size after each feature.

With the G1 collector, an array of half a region or more takes whole regions. A buffer of that size can hold up to a region more than its length, and the builder more heap than `heapSize()` reports. The [benchmarks README](https://github.com/tileverse-io/tileverse/blob/main/internal/vectortiles-benchmarks/README.md#tile-under-construction-size) compares `heapSize()` with the heap measured on its tiles.

## Coordinates and units

The geometries given to the builder are in **tile coordinates**: origin at the tile's top-left, y growing downward, extent given by `setExtent(...)` (typically 4096). Geometries in another grid take an [input scale](#geometries-at-another-scale). If your source is in a CRS (EPSG:3857, etc.), transform it to tile coordinates first, with the matching `TileMatrixSet`. See [Tile Matrix Set](../../tilematrixset/index.md).

The builder multiplies the coordinates by the input scale, then rounds them to integers with `Math.round`, when it writes them. It writes the geometry commands by the rules of the [MVT specification 2.1](https://github.com/mapbox/vector-tile-spec/tree/master/2.1):

- An exterior ring has a positive area by the surveyor's formula on the rounded coordinates, and a hole a negative one.
- A ring of zero area once rounded is left out, with the holes of a left-out exterior ring.

A feature left without geometry commands once rounded is not written, for example a feature with a line collapsed to one point. A NaN ordinate, an ordinate rounding outside the range of an int, or a point more than 2^31 - 1 tile units from the point before it on one axis fails the feature with an `IllegalArgumentException`. The specification supports no larger move between two points. The builder checks the geometries returned by the steps: the clip step of the default pipeline drops a point with a NaN ordinate before the check, as it drops a point outside the tile.

## See also

- [Decoding Vector Tiles](decode.md) for the reverse direction.
- [MVT v2.1 spec](https://github.com/mapbox/vector-tile-spec/tree/master/2.1) for the binary format.
