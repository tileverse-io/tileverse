/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.versatiles.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tileverse.tiling.matrix.Tile;
import io.tileverse.tiling.matrix.TileMatrix;
import io.tileverse.tiling.matrix.TileMatrixSet;
import io.tileverse.tiling.pyramid.TileIndex;
import io.tileverse.tiling.store.TileData;
import io.tileverse.tiling.store.TileStore.Strategy;
import io.tileverse.versatiles.TestArchiveWriter;
import io.tileverse.versatiles.TileFormat;
import io.tileverse.versatiles.UnsupportedTileFormatException;
import io.tileverse.versatiles.VersatilesReader;
import io.tileverse.versatiles.VersatilesTestData;
import java.awt.image.RenderedImage;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Optional;
import javax.imageio.ImageIO;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;

/**
 * Exercises {@link VersatilesRasterTileStore} against the {@code flowers.versatiles} fixture, a small WebP raster
 * pyramid (zoom 0-21). Expectations match the output of {@code versatiles probe flowers.versatiles}: tile_format=WEBP,
 * tile_type=raster.
 */
class VersatilesRasterTileStoreTest {

    /** Guards the process-wide cache settings of {@link ImageIO} against tests running in parallel. */
    private static final String IMAGEIO_CACHE = "javax.imageio.ImageIO.cache";

    @TempDir
    private Path tmpFolder;

    private VersatilesReader flowersReader;
    private VersatilesRasterTileStore flowersStore;

    @BeforeEach
    void setup() throws IOException {
        Path file = VersatilesTestData.flowers(tmpFolder);
        this.flowersReader = VersatilesReader.open(file);
        this.flowersStore = new VersatilesRasterTileStore(flowersReader);
    }

    @AfterEach
    void teardown() throws IOException {
        if (flowersReader != null) {
            flowersReader.close();
        }
    }

    @Test
    void headerAdvertisesWebpRaster() {
        assertThat(flowersReader.getTileFormat()).isEqualTo(TileFormat.WEBP);
        assertThat(flowersReader.getTileFormat().isVector()).isFalse();
        assertThat(flowersReader.getMinZoom()).isZero();
        assertThat(flowersReader.getMaxZoom()).isEqualTo(21);
    }

    @Test
    void mimeType() {
        assertThat(flowersStore.mimeType()).isEqualTo("image/webp");
        assertThat(flowersStore.getTileFormat()).isEqualTo(TileFormat.WEBP);
    }

    @Test
    void matrixSetCoversHeaderZoomRange() {
        TileMatrixSet matrixSet = flowersStore.matrixSet();
        assertThat(matrixSet.minZoomLevel()).isZero();
        assertThat(matrixSet.maxZoomLevel()).isEqualTo(21);
        assertThat(flowersStore.getExtent()).isNotNull();
    }

    @Test
    void matrixSetHasTileSize() {
        TileMatrixSet matrixSet = flowersStore.matrixSet();
        assertThat(matrixSet.tileWidth()).isEqualTo(512);
        assertThat(matrixSet.tileHeight()).isEqualTo(512);
    }

    // 9783.94 m/px is the resolution of zoom 3 with 512 px tiles, and of zoom 4 with 256 px tiles
    @Test
    void zoomMatchesTileSize() {
        assertThat(flowersStore.findBestZoomLevel(9783.94, Strategy.QUALITY)).isEqualTo(3);
    }

    @Test
    void keeps256pxTiles() throws IOException {
        Path file = new TestArchiveWriter()
                .tileFormat(TileFormat.PNG)
                .tile(0, 0, 0, VersatilesTestData.png(256, 256))
                .write(tmpFolder.resolve("png256.versatiles"));

        try (VersatilesReader reader = VersatilesReader.open(file)) {
            VersatilesRasterTileStore store = new VersatilesRasterTileStore(reader);

            assertThat(store.matrixSet().tileWidth()).isEqualTo(256);
        }
    }

    @Test
    void archiveWithoutTiles() throws IOException {
        Path file = new TestArchiveWriter().tileFormat(TileFormat.PNG).write(tmpFolder.resolve("empty.versatiles"));

        try (VersatilesReader reader = VersatilesReader.open(file)) {
            VersatilesRasterTileStore store = new VersatilesRasterTileStore(reader);

            assertThat(store.matrixSet().tileWidth()).isEqualTo(256);
        }
    }

    @Test
    @ResourceLock(value = IMAGEIO_CACHE, mode = ResourceAccessMode.READ)
    void loadRootTileReturnsDecodedImage() {
        TileMatrix root = flowersStore.matrixSet().getTileMatrix(0);
        Tile rootTile = root.tile(TileIndex.xyz(0, 0, 0)).orElseThrow();

        Optional<TileData<RenderedImage>> data = flowersStore.loadTile(rootTile);

        assertThat(data).as("root tile must exist").isPresent();
        RenderedImage img = data.orElseThrow().data();
        assertThat(img).isNotNull();
        assertThat(img.getWidth()).isEqualTo(512);
        assertThat(img.getHeight()).isEqualTo(512);
    }

    // A container with a read-only file system often has no writable temporary directory.
    @Test
    @DisabledOnOs(value = OS.WINDOWS, disabledReason = "makes a directory read-only with POSIX permissions")
    @ResourceLock(value = IMAGEIO_CACHE, mode = ResourceAccessMode.READ_WRITE)
    void decodesWithoutTempFiles() throws IOException {
        Path readOnly = Files.createDirectory(tmpFolder.resolve("read-only"));
        Files.setPosixFilePermissions(readOnly, PosixFilePermissions.fromString("r-xr-xr-x"));
        File cacheDirectory = ImageIO.getCacheDirectory();
        boolean useCache = ImageIO.getUseCache();
        ImageIO.setUseCache(true);
        ImageIO.setCacheDirectory(readOnly.toFile());
        try {
            Tile rootTile = flowersStore
                    .matrixSet()
                    .getTileMatrix(0)
                    .tile(TileIndex.xyz(0, 0, 0))
                    .orElseThrow();

            assertThat(flowersStore.loadTile(rootTile)).isPresent();
        } finally {
            ImageIO.setCacheDirectory(cacheDirectory);
            ImageIO.setUseCache(useCache);
            Files.setPosixFilePermissions(readOnly, PosixFilePermissions.fromString("rwxr-xr-x"));
        }
    }

    @Test
    void corruptTileFailsToDecode() throws IOException {
        Path file = new TestArchiveWriter()
                .tileFormat(TileFormat.PNG)
                .tile(0, 0, 0, VersatilesTestData.png(256, 256))
                .tile(1, 0, 0, "not an image")
                .write(tmpFolder.resolve("corrupt.versatiles"));

        try (VersatilesReader corrupt = VersatilesReader.open(file)) {
            VersatilesRasterTileStore store = new VersatilesRasterTileStore(corrupt);
            Tile corruptTile = store.matrixSet()
                    .getTileMatrix(1)
                    .tile(TileIndex.xyz(0, 0, 1))
                    .orElseThrow();

            assertThatThrownBy(() -> store.loadTile(corruptTile))
                    .isInstanceOf(UncheckedIOException.class)
                    .hasCauseInstanceOf(IOException.class);
        }
    }

    // The store reads the first tile to find the tile size
    @Test
    void corruptFirstTileFailsToOpen() throws IOException {
        Path file = new TestArchiveWriter()
                .tileFormat(TileFormat.PNG)
                .tile(0, 0, 0, "not an image")
                .write(tmpFolder.resolve("corrupt.versatiles"));

        try (VersatilesReader corrupt = VersatilesReader.open(file)) {
            assertThatThrownBy(() -> new VersatilesRasterTileStore(corrupt))
                    .isInstanceOf(UncheckedIOException.class)
                    .hasCauseInstanceOf(IOException.class);
        }
    }

    // The JDK has no AVIF decoder, and this module brings none.
    @Test
    void rejectsAvifWithoutDecoder() throws IOException {
        Path file = new TestArchiveWriter()
                .tileFormat(TileFormat.AVIF)
                .tile(0, 0, 0, "not an image")
                .write(tmpFolder.resolve("avif.versatiles"));

        try (VersatilesReader avif = VersatilesReader.open(file)) {
            assertThatThrownBy(() -> new VersatilesRasterTileStore(avif))
                    .isInstanceOf(UnsupportedTileFormatException.class);
        }
    }

    @Test
    void rejectsVectorArchive() throws IOException {
        try (VersatilesReader andorra = VersatilesReader.open(VersatilesTestData.andorra(tmpFolder))) {
            assertThatThrownBy(() -> new VersatilesRasterTileStore(andorra))
                    .isInstanceOf(UnsupportedTileFormatException.class)
                    .hasMessageContaining("PBF");
        }
    }
}
