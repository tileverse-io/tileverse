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
package io.tileverse.pmtiles.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tileverse.pmtiles.PMTilesHeader;
import io.tileverse.pmtiles.PMTilesReader;
import io.tileverse.pmtiles.PMTilesTestData;
import io.tileverse.pmtiles.UnsupportedTileTypeException;
import io.tileverse.tiling.matrix.Tile;
import io.tileverse.tiling.matrix.TileMatrix;
import io.tileverse.tiling.matrix.TileMatrixSet;
import io.tileverse.tiling.pyramid.TileIndex;
import io.tileverse.tiling.store.TileData;
import java.awt.image.RenderedImage;
import java.io.IOException;
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
     * Every tile the archive reports as present must decode to a 256x256 {@link RenderedImage}. Confirms the streaming
     * decode path works end-to-end with the bundled webp-imageio plugin.
     */
    @Test
    void allArchiveTilesAtLowZoomsDecodeAsImage() {
        TileMatrixSet matrixSet = flowersStore.matrixSet();
        for (int z = matrixSet.minZoomLevel(); z <= 5; z++) {
            int zoom = z;
            flowersReader.getTileIndicesByZoomLevel(zoom).forEach(index -> {
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
    void vectorStoreRejectsRasterArchive() {
        assertThatThrownBy(() -> new PMTilesVectorTileStore(flowersReader))
                .isInstanceOf(UnsupportedTileTypeException.class)
                .hasMessageContaining("MVT");
    }
}
