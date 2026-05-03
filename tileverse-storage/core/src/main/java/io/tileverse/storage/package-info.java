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
/**
 * Tileverse Storage - a small, backend-agnostic API for object storage.
 *
 * <h2>Overview</h2>
 *
 * Two complementary entry points cover most use cases:
 *
 * <ul>
 *   <li>{@link io.tileverse.storage.Storage Storage} - a {@link java.io.Closeable} handle to a container (an S3
 *       bucket+prefix, an Azure container, a GCS bucket, a local directory, or an HTTP base URL). Supports
 *       {@code stat}, {@code list}, {@code openRangeReader}, {@code read}, {@code put}, {@code delete}, {@code copy},
 *       {@code move}, and presigning - subject to the backend's {@link io.tileverse.storage.StorageCapabilities
 *       capabilities}.
 *   <li>{@link io.tileverse.storage.RangeReader RangeReader} - a thread-safe {@code readRange(offset, length)}
 *       abstraction obtained from {@link io.tileverse.storage.Storage#openRangeReader(String) Storage.openRangeReader}.
 *       Suited to single-object byte-range readers (PMTiles, COG, single-file Parquet) where the {@code Storage}
 *       surface is more than the caller needs.
 * </ul>
 *
 * <h2>Opening a Storage</h2>
 *
 * {@link io.tileverse.storage.StorageFactory StorageFactory} resolves a
 * {@link io.tileverse.storage.spi.StorageProvider} for a URI (or {@link java.util.Properties}) via
 * {@link java.util.ServiceLoader} and returns a {@code Storage} rooted at that URI. All keys passed to the
 * {@code Storage} are relative to its root.
 *
 * <pre>{@code
 * try (Storage storage = StorageFactory.open(URI.create("s3://my-bucket/datasets/v3/"))) {
 *     storage.list("year=2024/").forEach(System.out::println);
 *     try (RangeReader reader = storage.openRangeReader("file.parquet")) {
 *         ByteBuffer header = reader.readRange(0, 8);
 *     }
 * }
 * }</pre>
 *
 * <h2>Optional surface</h2>
 *
 * Backends differ. Inspect {@link io.tileverse.storage.Storage#capabilities() storage.capabilities()} before invoking
 * optional methods, or use the {@code requireXxx} helpers to fail fast with
 * {@link io.tileverse.storage.UnsupportedCapabilityException}.
 *
 * <h2>Glob patterns</h2>
 *
 * {@link io.tileverse.storage.Storage#list(String) Storage.list(String)} accepts a shell-style pattern.
 * {@code "data/*.parquet"} matches direct children; {@code "data/**\/*.parquet"} walks descendants;
 * {@code "data/{a,b}/*.txt"} expands brace alternatives. See {@link io.tileverse.storage.StoragePattern
 * StoragePattern}.
 *
 * <h2>Resource ownership</h2>
 *
 * The SDK client (S3, Azure, GCS) is owned by the {@code Storage} and survives the
 * {@code RangeReader}/{@code InputStream}/{@code OutputStream} instances obtained from it. Closing the {@code Storage}
 * releases the SDK client; a {@code RangeReader} obtained from a closed {@code Storage} throws
 * {@link java.io.IOException} on the next read.
 *
 * <h2>Companion packages</h2>
 *
 * <ul>
 *   <li>{@link io.tileverse.storage.spi} - {@link io.tileverse.storage.spi.StorageProvider} SPI for adding new
 *       backends; configuration types ({@link io.tileverse.storage.StorageConfig},
 *       {@link io.tileverse.storage.StorageParameter}).
 * </ul>
 */
@org.jspecify.annotations.NullMarked
package io.tileverse.storage;
