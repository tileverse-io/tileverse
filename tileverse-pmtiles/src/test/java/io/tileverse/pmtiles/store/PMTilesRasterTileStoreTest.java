/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.pmtiles.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tileverse.pmtiles.PMTilesHeader;
import io.tileverse.pmtiles.PMTilesReader;
import io.tileverse.pmtiles.PMTilesTestData;
import io.tileverse.pmtiles.TestArchiveWriter;
import io.tileverse.pmtiles.UnsupportedTileTypeException;
import io.tileverse.tiling.matrix.Tile;
import io.tileverse.tiling.matrix.TileMatrix;
import io.tileverse.tiling.matrix.TileMatrixSet;
import io.tileverse.tiling.pyramid.TileIndex;
import io.tileverse.tiling.store.TileData;
import io.tileverse.tiling.store.TileStore.Strategy;
import java.awt.image.RenderedImage;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Exercises {@link PMTilesRasterTileStore} against the {@code flowers.pmtiles} fixture, a small WebP raster pyramid
 * (zoom 0-21, bounds around 121.524 E / 25.014 N). The expected values below match the output of {@code pmtiles show
 * flowers.pmtiles}.
 */
class PMTilesRasterTileStoreTest {

    @TempDir
    private Path tmpFolder;

    private PMTilesReader flowersReader;
    private PMTilesRasterTileStore flowersStore;

    @BeforeEach
    void setup() throws IOException {
        URI file = PMTilesTestData.flowers(tmpFolder).toUri();
        this.flowersReader = PMTilesReader.open(file);
        this.flowersStore = new PMTilesRasterTileStore(flowersReader);
    }

    @AfterEach
    void teardown() throws IOException {
        if (flowersReader != null) {
            flowersReader.close();
        }
    }

    /** {@code pmtiles show flowers.pmtiles} reports tile type WEBP, min/max zoom 0/21, bounds 121.524 E / 25.014 N. */
    @Test
    void headerAdvertisesWebpRaster() {
        PMTilesHeader header = flowersReader.getHeader();
        assertThat(header.tileType()).isEqualTo(PMTilesHeader.TILETYPE_WEBP);
        assertThat(header.isRasterTileType()).isTrue();
        assertThat(header.tileMimeType()).isEqualTo("image/webp");
        assertThat(header.minZoom()).isZero();
        assertThat(header.maxZoom()).isEqualTo((byte) 21);
    }

    @Test
    void mimeType() {
        assertThat(flowersStore.mimeType()).isEqualTo("image/webp");
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
        Path file = new TestArchiveWriter(PMTilesTestData.png(256, 256)).write(tmpFolder.resolve("png256.pmtiles"));

        try (PMTilesReader reader = PMTilesReader.open(file.toUri())) {
            PMTilesRasterTileStore store = new PMTilesRasterTileStore(reader);

            assertThat(store.matrixSet().tileWidth()).isEqualTo(256);
        }
    }

    @Test
    void findsTileInLeafDirectory() throws IOException {
        Path file = new TestArchiveWriter(PMTilesTestData.png(512, 512))
                .inLeafDirectory()
                .write(tmpFolder.resolve("leaf.pmtiles"));

        try (PMTilesReader reader = PMTilesReader.open(file.toUri())) {
            PMTilesRasterTileStore store = new PMTilesRasterTileStore(reader);

            assertThat(store.matrixSet().tileWidth()).isEqualTo(512);
        }
    }

    // The store reads the first tile to find the tile size
    @Test
    void corruptFirstTileFailsToOpen() throws IOException {
        Path file = new TestArchiveWriter("not an image").write(tmpFolder.resolve("corrupt.pmtiles"));

        try (PMTilesReader reader = PMTilesReader.open(file.toUri())) {
            assertThatThrownBy(() -> new PMTilesRasterTileStore(reader))
                    .isInstanceOf(UncheckedIOException.class)
                    .hasCauseInstanceOf(IOException.class);
        }
    }

    // The JDK has no AVIF decoder, and this module brings none.
    @Test
    void rejectsAvifWithoutDecoder() throws IOException {
        Path file = new TestArchiveWriter("not an image")
                .tileType(PMTilesHeader.TILETYPE_AVIF)
                .write(tmpFolder.resolve("avif.pmtiles"));

        try (PMTilesReader reader = PMTilesReader.open(file.toUri())) {
            assertThatThrownBy(() -> new PMTilesRasterTileStore(reader))
                    .isInstanceOf(UnsupportedTileTypeException.class);
        }
    }

    /** The root tile is always populated for archives whose zoom range starts at 0. */
    @Test
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

    /**
     * Every tile the archive reports as present must decode to a 512x512 {@link RenderedImage}. Confirms the streaming
     * decode path works end-to-end with the bundled webp-imageio plugin.
     */
    @Test
    void allArchiveTilesAtLowZoomsDecodeAsImage() {
        TileMatrixSet matrixSet = flowersStore.matrixSet();
        for (int z = matrixSet.minZoomLevel(); z <= 5; z++) {
            int zoom = z;
            PMTilesTestData.tileIndicesAtZoom(flowersReader, zoom).forEach(index -> {
                Tile tile = matrixSet.getTileMatrix(zoom).tile(index).orElseThrow();
                Optional<TileData<RenderedImage>> data = flowersStore.loadTile(tile);
                assertThat(data).as("archive tile %s should load", index).isPresent();
                RenderedImage img = data.orElseThrow().data();
                assertThat(img.getWidth()).isEqualTo(512);
                assertThat(img.getHeight()).isEqualTo(512);
            });
        }
    }

    @Test
    void rejectsVectorArchive() throws IOException {
        try (PMTilesReader andorra =
                PMTilesReader.open(PMTilesTestData.andorra(tmpFolder).toUri())) {
            assertThatThrownBy(() -> new PMTilesRasterTileStore(andorra))
                    .isInstanceOf(UnsupportedTileTypeException.class)
                    .hasMessageContaining("vnd.mapbox-vector-tile");
        }
    }

    @Test
    void vectorStoreRejectsRasterArchive() {
        assertThatThrownBy(() -> new PMTilesVectorTileStore(flowersReader))
                .isInstanceOf(UnsupportedTileTypeException.class)
                .hasMessageContaining("MVT");
    }
}
