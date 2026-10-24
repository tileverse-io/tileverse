/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.pmtiles;

import static java.util.Objects.requireNonNull;

import io.tileverse.io.ByteRange;
import io.tileverse.storage.BatchReadResult;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.RangeRequest;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;
import io.tileverse.tiling.pyramid.TileIndex;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import javax.imageio.ImageIO;
import javax.imageio.stream.ImageOutputStream;
import javax.imageio.stream.MemoryCacheImageOutputStream;

public class PMTilesTestData {

    public static Path andorra(Path tmpFolder) throws IOException {
        Path andorraPmTiles = tmpFolder.resolve("andorra.pmtiles");
        if (!Files.isRegularFile(andorraPmTiles)) {
            try (InputStream in = requireNonNull(
                    PMTilesReaderTest.class.getResourceAsStream("/io/tileverse/pmtiles/andorra.pmtiles"))) {
                Files.copy(in, andorraPmTiles);
            }
        }
        return andorraPmTiles;
    }

    /**
     * Materialize {@code flowers.pmtiles} into {@code tmpFolder} and return its path. The fixture is a small
     * WebP-encoded raster pyramid (zoom 0-21, ~7 MB) sourced from https://air.mtn.tw/flowers.pmtiles.
     */
    public static Path flowers(Path tmpFolder) throws IOException {
        Path target = tmpFolder.resolve("flowers.pmtiles");
        if (!Files.isRegularFile(target)) {
            try (InputStream in = requireNonNull(
                    PMTilesReaderTest.class.getResourceAsStream("/io/tileverse/pmtiles/flowers.pmtiles"))) {
                Files.copy(in, target);
            }
        }
        return target;
    }

    /** Encodes a black PNG image of {@code width} x {@code height} pixels. */
    public static byte[] png(int width, int height) throws IOException {
        BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        // ImageIO.write(image, "png", bytes) would write a temporary file
        try (ImageOutputStream out = new MemoryCacheImageOutputStream(bytes)) {
            ImageIO.write(image, "png", out);
        }
        return bytes.toByteArray();
    }

    public static RangeReader andorraFileRangeReader(Path tmpFolder) throws IOException {
        Path file = andorra(tmpFolder);
        URI leaf = file.toUri();
        URI parent = file.getParent().toUri();
        Storage storage = StorageFactory.open(parent);
        try {
            return bundle(storage.openRangeReader(leaf), storage);
        } catch (RuntimeException e) {
            try {
                storage.close();
            } catch (IOException ignored) {
                // best-effort cleanup
            }
            throw e;
        }
    }

    private static RangeReader bundle(RangeReader delegate, Storage owner) {
        return new RangeReader() {
            @Override
            public int readRange(long offset, int length, ByteBuffer target) {
                return delegate.readRange(offset, length, target);
            }

            @Override
            public BatchReadResult readRanges(List<RangeRequest> requests) {
                return delegate.readRanges(requests);
            }

            @Override
            public OptionalLong size() {
                return delegate.size();
            }

            @Override
            public String getSourceIdentifier() {
                return delegate.getSourceIdentifier();
            }

            @Override
            public void close() throws IOException {
                try {
                    delegate.close();
                } finally {
                    owner.close();
                }
            }
        };
    }

    public static List<String> andorraLayerNames() {
        return Arrays.asList(
                "addresses",
                "aerialways",
                "boundaries",
                "boundary_labels",
                "bridges",
                "buildings",
                "dam_lines",
                "dam_polygons",
                "land",
                "place_labels",
                "pois",
                "public_transport",
                "sites",
                "street_labels",
                "street_polygons",
                "streets",
                "streets_polygons_labels",
                "water_lines",
                "water_lines_labels",
                "water_polygons",
                "water_polygons_labels");
    }

    /**
     * Delegating reader that records the shape of every read reaching the backend, for I/O-shape assertions.
     * Thread-safe.
     */
    public static class RecordingRangeReader implements RangeReader {
        private final RangeReader delegate;
        private final List<ByteRange> reads = Collections.synchronizedList(new ArrayList<>());
        private final AtomicInteger batchCalls = new AtomicInteger();

        RecordingRangeReader(RangeReader delegate) {
            this.delegate = requireNonNull(delegate);
        }

        @Override
        public int readRange(long offset, int length, ByteBuffer target) {
            reads.add(ByteRange.of(offset, length));
            return delegate.readRange(offset, length, target);
        }

        @Override
        public BatchReadResult readRanges(List<RangeRequest> requests) {
            batchCalls.incrementAndGet();
            requests.forEach(request -> reads.add(request.range()));
            return delegate.readRanges(requests);
        }

        @Override
        public OptionalLong size() {
            return delegate.size();
        }

        @Override
        public String getSourceIdentifier() {
            return delegate.getSourceIdentifier();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        /** Every backend read in arrival order; batched calls contribute one entry per request. */
        public List<ByteRange> reads() {
            return List.copyOf(reads);
        }

        /** How many readRanges batch calls reached the backend. */
        public int batchCalls() {
            return batchCalls.get();
        }

        public void clearRecordings() {
            reads.clear();
            batchCalls.set(0);
        }
    }

    public static RecordingRangeReader recording(RangeReader delegate) {
        return new RecordingRangeReader(delegate);
    }

    /**
     * Collects every tile index at {@code zoom} by walking the root and leaf directories through the bounded per-entry
     * API. Test-only replacement for the retired whole-archive traversal; fixtures are small enough to accumulate.
     */
    public static List<TileIndex> tileIndicesAtZoom(PMTilesReader reader, int zoom) {
        List<TileIndex> indices = new ArrayList<>();
        collectAtZoom(reader, reader.getRootDirectory(), zoom, indices);
        return indices;
    }

    private static void collectAtZoom(
            PMTilesReader reader, PMTilesDirectory directory, int zoom, List<TileIndex> into) {
        for (PMTilesEntry entry : directory) {
            if (entry.isLeaf()) {
                collectAtZoom(reader, reader.getDirectory(entry), zoom, into);
            } else {
                reader.getTileIndices(entry, index -> {
                    if (index.z() == zoom) {
                        into.add(index);
                    }
                });
            }
        }
    }
}
