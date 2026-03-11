# Tileverse Parquet

Hadoop-free Apache Parquet reader powered by [tileverse-rangereader](../src/core). Read Parquet and GeoParquet files from local disk, HTTP, S3, Azure Blob Storage, or Google Cloud Storage — without pulling in the Hadoop I/O stack.

## Features

- **No Hadoop I/O at runtime** — replaces Hadoop's `InputFile` with a `RangeReader`-backed adapter
- **Read from anywhere** — local files, HTTP/HTTPS, S3, Azure Blob, GCS via RangeReader plugins
- **GeoParquet support** — native handling of `GEOMETRY` and `GEOGRAPHY` logical types, CRS metadata passthrough
- **Four-tier filter pushdown** — row group pruning (statistics + dictionary), column-index page-level skipping, record-level filtering
- **Column projection** — read only the columns you need
- **Pluggable materializers** — Avro `GenericRecord` (default), Parquet `Group`, or bring your own
- **Compression** — Snappy, GZIP, ZSTD, LZ4_RAW, LZ4 (legacy), Brotli, and uncompressed
- **Stateless facade** — `ParquetDataset` caches metadata; each `read()` opens a fresh reader for safe concurrent use

## Quick Start

### Maven

```xml
<dependency>
  <groupId>io.tileverse.parquet</groupId>
  <artifactId>tileverse-parquet</artifactId>
  <version>${tileverse.version}</version>
</dependency>
```

You'll also need a RangeReader implementation on the classpath (e.g., `tileverse-rangereader-core` for local/HTTP, `tileverse-rangereader-s3` for S3).

### Read all records

```java
RangeReader reader = FileRangeReader.of(Path.of("data.parquet"));
InputFile inputFile = new RangeReaderInputFile(reader);

ParquetDataset dataset = ParquetDataset.open(inputFile);

try (CloseableIterator<GenericRecord> records = dataset.read()) {
    while (records.hasNext()) {
        GenericRecord record = records.next();
        System.out.println(record);
    }
}
```

### Read from S3

```java
RangeReader reader = S3RangeReader.builder()
    .uri(URI.create("s3://bucket/path/to/file.parquet"))
    .withRegion(Region.US_WEST_2)
    .build();

InputFile inputFile = new RangeReaderInputFile(reader);
ParquetDataset dataset = ParquetDataset.open(inputFile);
```

### With filter pushdown and column projection

```java
FilterPredicate filter = FilterApi.and(
    FilterApi.gtEq(FilterApi.intColumn("year"), 2020),
    FilterApi.eq(FilterApi.binaryColumn("country"), Binary.fromString("AR"))
);

try (CloseableIterator<GenericRecord> records =
        dataset.read(filter, Set.of("year", "country", "geometry"))) {
    // only matching records, only requested columns
}
```

## Architecture

```
┌───────────────────────────────────────────────────────────┐
│                     ParquetDataset                        │  High-level API
│            (stateless facade, caches metadata)            │
├──────────────┬────────────────────────┬───────────────────┤
│ Materializer │   ParquetRecordIterator│  CloseableIterator│
│   Provider   │   (lazy multi-row-group│  (Iterator +      │ 
│   (pluggable)│    iteration + filter) │   Closeable)      │
├──────────────┴────────────────────────┴───────────────────┤
│                   reader package                          │  Low-level engine
│  ┌─────────────────────┐  ┌──────────────────────────┐    │
│  │CoreParquetFooter    │  │CoreParquetRowGroupReader │    │
│  │  Reader             │  │  (decompress, 4-tier     │    │
│  │  (Thrift footer,    │  │   filter pushdown)       │    │
│  │   schema parsing)   │  │                          │    │
│  └─────────────────────┘  └──────────────────────────┘    │
├───────────────────────────────────────────────────────────┤
│              RangeReaderInputFile                         │  I/O adapter
│        (RangeReader → Parquet InputFile)                  │
├───────────────────────────────────────────────────────────┤
│                  RangeReader                              │  tileverse-rangereader
│        (File, HTTP, S3, Azure, GCS)                       │
└───────────────────────────────────────────────────────────┘
```

### Package structure

| Package | Role |
|---------|------|
| `io.tileverse.parquet` | High-level API: `ParquetDataset`, materializer providers, I/O adapters |
| `io.tileverse.parquet.reader` | Low-level engine: footer reading, row group reading, decompression, filter visitors |

### Key classes

| Class | Description |
|-------|-------------|
| `ParquetDataset` | Main entry point. Opens a file, caches schema and metadata, returns lazy record iterators. Thread-safe for concurrent reads. |
| `RangeReaderInputFile` | Adapts a `RangeReader` to Parquet's `InputFile` interface. Does not take ownership of the reader. |
| `ParquetMaterializerProvider<T>` | Functional interface for pluggable record type conversion. |
| `AvroMaterializerProvider` | Default materializer producing Avro `GenericRecord`s. Custom implementation that avoids `AvroReadSupport` and its `hadoop-common` runtime dependency. Handles nested schemas, arrays, maps. |
| `GroupMaterializerProvider` | Backward-compatible materializer producing Parquet `Group` objects. |
| `ParquetRecordIterator<T>` | Lazy iterator that transparently advances through row groups with record-level filtering. |
| `CloseableIterator<T>` | `Iterator<T> + Closeable` — use with try-with-resources. |
| `CoreParquetFooterReader` | Reads the Parquet footer directly from an `InputFile`, parses the Thrift `FileMetaData`, and reconstructs the `MessageType` schema. Supports all Parquet logical types including `GEOMETRY` and `GEOGRAPHY`. |
| `CoreParquetRowGroupReader` | Reads and decompresses column chunks. Implements four-tier filter pushdown: stats-based and dictionary-based row group pruning, column-index page-level filtering, and record-level filtering. |
| `CoreColumnIndexReader` | Reads column index and offset index structures from a Parquet file for a given row group. |
| `CoreColumnIndexStore` | Implements Parquet's `ColumnIndexStore` interface, providing page-level min/max stats to the column-index filter. |
| `CoreParquetReadOptions` | Immutable configuration for read behavior. Three filter toggles (`useStatsFilter`, `useDictionaryFilter`, `useColumnIndexFilter`) all default to true. |

## GeoParquet Support

The library provides first-class support for [GeoParquet](https://geoparquet.org/) files:

- **Logical types:** `GEOMETRY` and `GEOGRAPHY` logical type annotations are parsed from the Parquet schema, including CRS metadata and edge interpolation algorithms.
- **Metadata passthrough:** The `"geo"` key-value metadata (GeoParquet spec) is available via `dataset.getKeyValueMetadata().get("geo")` for consumers to parse.
- **Geometry encoding:** Geometry data is typically stored as WKB (Well-Known Binary) in `BINARY` columns. The core library delivers the raw bytes; decoding WKB to geometry objects (e.g., JTS `Geometry`) is left to the consumer.

```java
ParquetDataset dataset = ParquetDataset.open(inputFile);

// Access GeoParquet metadata
String geoMetadata = dataset.getKeyValueMetadata().get("geo");

// Read geometry as raw bytes (WKB)
try (CloseableIterator<GenericRecord> records = dataset.read(Set.of("geometry"))) {
    while (records.hasNext()) {
        GenericRecord record = records.next();
        ByteBuffer wkb = (ByteBuffer) record.get("geometry");
        // decode WKB with your geometry library of choice
    }
}
```

## Filter Pushdown

`ParquetDataset` supports Parquet's native `FilterPredicate` for four levels of filtering:

### 1. Row group pruning (statistics-based)

Uses column min/max statistics stored in the row group metadata to skip entire row groups that can't contain matching records.

### 2. Row group pruning (dictionary-based)

Loads dictionary pages and checks if the filter value exists in the dictionary. Skips row groups where the dictionary proves no match is possible.

### 3. Column-index pushdown (page-level)

Uses page-level min/max statistics (ColumnIndex) and page offsets (OffsetIndex) to compute matching `RowRanges`, then reads only the pages that may contain matching records. This is particularly effective for large row groups — for example, a 1 GB file with a single row group and a 1% selectivity filter reads ~10 MB instead of the full 1 GB.

### 4. Record-level filtering

Parquet's built-in column reader applies the filter predicate during record assembly, skipping individual records that don't match.

All four levels are enabled by default and can be toggled via `CoreParquetReadOptions`:

```java
CoreParquetReadOptions options = CoreParquetReadOptions.builder()
    .useStatsFilter(true)           // row group pruning via statistics (default: true)
    .useDictionaryFilter(true)      // row group pruning via dictionaries (default: true)
    .useColumnIndexFilter(true)     // page-level skipping via column index (default: true)
    .build();

ParquetDataset dataset = ParquetDataset.open(inputFile, options);
```

## Compression

Supported decompression codecs:

| Codec | Library |
|-------|---------|
| Uncompressed | — |
| Snappy | `org.xerial.snappy:snappy-java` |
| GZIP | `java.util.zip` (JDK) |
| ZSTD | `com.github.luben:zstd-jni` |
| LZ4_RAW | `net.jpountz.lz4:lz4-java` |
| LZ4 (legacy Hadoop) | `net.jpountz.lz4:lz4-java` |
| Brotli | `org.apache.commons:commons-compress` + `org.brotli:dec` |

Files using unsupported codecs (LZO) will throw `IOException` at read time.

## Custom Materializers

Implement `ParquetMaterializerProvider<T>` to convert Parquet column data to any record type:

```java
ParquetMaterializerProvider<MyRecord> provider = (fileSchema, requestedSchema, metadata) -> {
    // return a RecordMaterializer<MyRecord> that assembles your type
    return new MyRecordMaterializer(requestedSchema);
};

try (CloseableIterator<MyRecord> records = dataset.read(provider, Set.of("id", "name"))) {
    while (records.hasNext()) {
        MyRecord record = records.next();
    }
}
```

Built-in providers:
- `AvroMaterializerProvider.INSTANCE` — Avro `GenericRecord` (default)
- `GroupMaterializerProvider.INSTANCE` — Parquet `Group`

## Hadoop-Free Design

This library's primary design goal is to read Parquet files **without requiring Hadoop at runtime**.

### Why not use parquet-avro's `AvroReadSupport`?

Parquet-avro's `AvroReadSupport` directly imports `org.apache.hadoop.conf.Configuration` and `org.apache.hadoop.util.ReflectionUtils`. Although parquet-avro declares `hadoop-common` with `<scope>provided</scope>`, any code that instantiates `AvroReadSupport` will fail with `NoClassDefFoundError` unless `hadoop-common` is on the classpath.

[`hadoop-common:3.4.3`](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common/3.4.3/dependencies) brings **47 compile-scoped transitive dependencies** (Guava, Protobuf, Jetty, Jersey, `commons-*`, Curator, etc.) — a 100 MB+ dependency tree that conflicts with most modern application stacks.

Instead, `AvroMaterializerProvider` reimplements Avro materialization using only:
- **Avro APIs** — `Schema`, `GenericData.Record`
- **Parquet column APIs** — `GroupConverter`, `PrimitiveConverter`, `RecordMaterializer`
- **`AvroSchemaConverter`** from parquet-avro (which itself has no Hadoop imports)

This gives full Avro `GenericRecord` support (nested records, arrays, maps, all primitives, nullable unions) with zero Hadoop classes at runtime.

### What about `parquet-hadoop`?

`parquet-hadoop` is still a compile dependency — it provides core Parquet classes like `InputFile`, `ColumnDescriptor`, `FilterCompat`, and the column reader infrastructure. However, all Hadoop I/O classes (`HadoopInputFile`, `Configuration`, etc.) are avoided at runtime by using `RangeReaderInputFile` as the `InputFile` implementation.

## Ecosystem

The companion module [`tileverse-parquet-datastore`](../tileverse-parquet-datastore) builds on this library to provide a GeoTools `DataStore` implementation, enabling seamless integration with GeoServer and other GeoTools-based applications.

## Limitations

- **Read-only** — no write support yet
- **Single file** — reads one Parquet file at a time; multi-file/partitioned datasets must be handled by the consumer
- **Missing codecs** — LZO is not yet supported
- **GeoParquet metadata not parsed** — the `"geo"` JSON is passed through as a raw string; consumers parse it themselves
- **`parquet-hadoop` dependency** — still required at compile time for some core Parquet classes, though Hadoop I/O is not used at runtime
