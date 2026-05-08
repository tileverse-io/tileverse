/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
import static org.assertj.core.api.Assertions.fail;

import io.tileverse.cache.CacheManager;
import io.tileverse.jackson.databind.pmtiles.v3.PMTilesMetadata;
import io.tileverse.jackson.databind.tilejson.v3.VectorLayer;
import io.tileverse.pmtiles.PMTilesHeader;
import io.tileverse.pmtiles.PMTilesReader;
import io.tileverse.pmtiles.PMTilesTestData;
import io.tileverse.tiling.common.BoundingBox2D;
import io.tileverse.tiling.common.CornerOfOrigin;
import io.tileverse.tiling.matrix.DefaultTileMatrixSets;
import io.tileverse.tiling.matrix.Tile;
import io.tileverse.tiling.matrix.TileMatrix;
import io.tileverse.tiling.matrix.TileMatrixSet;
import io.tileverse.tiling.pyramid.TileIndex;
import io.tileverse.tiling.pyramid.TileRange;
import io.tileverse.tiling.store.TileData;
import io.tileverse.vectortile.model.VectorTile;
import io.tileverse.vectortile.mvt.VectorTileCodec;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Slf4j
class PMTilesVectorTileStoreTest {

    @TempDir
    private Path tmpFolder;

    private CacheManager cacheManager;
    private PMTilesReader andorraReader;
    private PMTilesVectorTileStore andorraStore;

    @BeforeEach
    void setup() throws IOException {
        URI file = PMTilesTestData.andorra(tmpFolder).toUri();
        this.cacheManager = CacheManager.newInstance();
        this.andorraReader = PMTilesReader.open(file).cacheManager(cacheManager);
        this.andorraStore = new PMTilesVectorTileStore(andorraReader).cacheManager(cacheManager);
    }

    @Test
    void layerNames() {

        PMTilesMetadata metadata = andorraStore.getMetadata();
        List<VectorLayer> vectorLayers = metadata.vectorLayers();
        List<String> layerNames = vectorLayers.stream().map(VectorLayer::id).toList();

        assertThat(layerNames).containsExactlyElementsOf(PMTilesTestData.andorraLayerNames());
    }

    @Test
    void getLayerMetadata() {
        assertThat(andorraStore.getLayerMetadata("badname")).isEmpty();
        assertThat(andorraStore.getLayerMetadata("addresses").orElseThrow().fields())
                .hasSize(2)
                .containsEntry("housenumber", "String")
                .containsEntry("housename", "String");

        assertThat(andorraStore.getLayerMetadata("aerialways").orElseThrow().fields())
                .hasSize(1)
                .containsEntry("kind", "String");

        assertThat(andorraStore.getLayerMetadata("boundaries").orElseThrow().fields())
                .hasSize(3)
                .containsEntry("admin_level", "Number")
                .containsEntry("maritime", "Boolean")
                .containsEntry("disputed", "Boolean");

        assertThat(andorraStore
                        .getLayerMetadata("boundary_labels")
                        .orElseThrow()
                        .fields())
                .hasSize(4)
                .containsEntry("name_en", "String")
                .containsEntry("name_de", "String")
                .containsEntry("way_area", "Number")
                .containsEntry("name", "String");

        assertThat(andorraStore.getLayerMetadata("dam_lines").orElseThrow().fields())
                .hasSize(1)
                .containsEntry("kind", "String");

        assertThat(andorraStore.getLayerMetadata("land").orElseThrow().fields())
                .hasSize(1)
                .containsEntry("kind", "String");

        assertThat(andorraStore.getLayerMetadata("place_labels").orElseThrow().fields())
                .hasSize(5)
                .containsEntry("kind", "String")
                .containsEntry("name_de", "String")
                .containsEntry("population", "Number")
                .containsEntry("name", "String")
                .containsEntry("name_en", "String");

        assertThat(andorraStore.getLayerMetadata("pois")).isPresent();

        assertThat(andorraStore.getLayerMetadata("pois").orElseThrow().fields())
                .hasSize(24)
                .containsEntry("man_made", "String")
                .containsEntry("tower:type", "String")
                .containsEntry("emergency", "String")
                .containsEntry("sport", "String")
                .containsEntry("denomination", "String")
                .containsEntry("amenity", "String")
                .containsEntry("atm", "Boolean")
                .containsEntry("name_en", "String")
                .containsEntry("historic", "String")
                .containsEntry("recycling:glass_bottles", "Boolean")
                .containsEntry("cuisine", "String")
                .containsEntry("name_de", "String")
                .containsEntry("shop", "String")
                .containsEntry("leisure", "String")
                .containsEntry("tourism", "String")
                .containsEntry("office", "String")
                .containsEntry("vending", "String")
                .containsEntry("recycling:clothes", "Boolean")
                .containsEntry("recycling:scrap_metal", "Boolean")
                .containsEntry("name", "String")
                .containsEntry("religion", "String")
                .containsEntry("recycling:paper", "Boolean")
                .containsEntry("information", "String")
                .containsEntry("housenumber", "String");
    }

    @Test
    void matrixSet() {
        TileMatrixSet webMercatorMatrixSet = DefaultTileMatrixSets.WEB_MERCATOR_QUAD;
        TileMatrixSet andorraMatrixSet = andorraStore.matrixSet();

        PMTilesHeader pmtilesHeader = andorraReader.getHeader();
        BoundingBox2D geographicBoundingBox = pmtilesHeader.geographicBoundingBox();
        BoundingBox2D andorraBoundingBox = WebMercatorTransform.latLonToWebMercator(geographicBoundingBox);

        assertThat(andorraBoundingBox).isEqualTo(andorraStore.getExtent());

        assertThat(andorraMatrixSet.minZoomLevel()).isZero();
        assertThat(andorraMatrixSet.maxZoomLevel()).isEqualTo(14);

        andorraMatrixSet.getTileMatrix(0).boundingBox();

        // the matrixset extent is snapped to tile extents though
        for (int z = andorraMatrixSet.minZoomLevel(); z <= andorraMatrixSet.maxZoomLevel(); z++) {
            TileMatrix zMatrix = andorraMatrixSet.getTileMatrix(z);

            assertThat(zMatrix.boundingBox()).isNotEqualTo(andorraBoundingBox);
            assertThat(zMatrix.boundingBox().contains(andorraBoundingBox)).isTrue();

            TileMatrix fullMatrix = webMercatorMatrixSet.getTileMatrix(z);
            if (z == 0) {
                assertThat(zMatrix.boundingBox()).isEqualTo(fullMatrix.boundingBox());
            } else {
                assertThat(zMatrix.boundingBox()).isNotEqualTo(fullMatrix.boundingBox());
            }
            assertThat(fullMatrix.boundingBox().contains(zMatrix.boundingBox())).isTrue();
        }
    }

    @Test
    void getWithTileIndex() {
        TileMatrixSet andorraMatrixSet = andorraStore.matrixSet();

        assertThat(andorraMatrixSet.tilePyramid().cornerOfOrigin()).isEqualTo(CornerOfOrigin.TOP_LEFT);

        log.debug("Total available indices: {}", andorraReader.getTileIndices().count());

        for (int z = andorraMatrixSet.minZoomLevel(); z <= 14 /*andorraMatrixSet.maxZoomLevel()*/; z++) {
            andorraReader.getTileIndicesByZoomLevel(z).forEach(index -> {
                try {
                    Optional<ByteBuffer> tile = andorraReader.getTile(index);
                    Optional<VectorTile> vectorTile = decode(tile);
                    assertThat(vectorTile).isPresent();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });

            TileMatrix zMatrix = andorraMatrixSet.getTileMatrix(z);
            TileRange zRange = zMatrix.tileRange();
            TileIndex index = zRange.first();
            do {
                assertExists(index);
                index = zRange.next(index).orElse(null);
            } while (index != null);
        }
    }

    private Optional<VectorTile> decode(Optional<ByteBuffer> tile) {
        if (tile.isEmpty()) {
            return Optional.empty();
        }
        ByteBuffer buff = tile.orElseThrow();
        try {
            return Optional.of(new VectorTileCodec().decode(buff));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }

    private void assertExists(TileIndex tileIndex) {
        Optional<TileData<VectorTile>> tileOpt = andorraStore.findTile(tileIndex);
        log.debug(" -> tile found: {}", tileOpt.isPresent());
    }

    /**
     * Every tile in the archive at zoom {@code z} must fall inside the {@link TileRange} that
     * {@link TileMatrix#extentToRange(BoundingBox2D)} returns for the header's WebMercator bbox.
     *
     * <p>Index-side check: the archive's coverage matches the bbox it declares.
     */
    @Test
    void tilesFallWithinHeaderBboxRange() {
        TileMatrixSet matrixSet = andorraStore.matrixSet();
        PMTilesHeader header = andorraReader.getHeader();
        BoundingBox2D headerBbox = WebMercatorTransform.latLonToWebMercator(header.geographicBoundingBox());

        for (int z = matrixSet.minZoomLevel(); z <= matrixSet.maxZoomLevel(); z++) {
            TileMatrix matrix = matrixSet.getTileMatrix(z);
            Optional<TileRange> rangeOpt = matrix.extentToRange(headerBbox);
            assertThat(rangeOpt)
                    .as("header bbox should produce a tile range at zoom %d", z)
                    .isPresent();
            TileRange expectedRange = rangeOpt.orElseThrow();

            List<TileIndex> archiveTiles =
                    andorraReader.getTileIndicesByZoomLevel(z).toList();
            assertThat(archiveTiles)
                    .as("archive tiles at zoom %d should fall inside %s", z, expectedRange)
                    .isNotEmpty()
                    .allMatch(expectedRange::contains);
        }
    }

    /**
     * Every tile present in the archive must have a geographic extent that intersects the header's WebMercator bbox.
     *
     * <p>Extent-side check, complementary to {@link #tilesFallWithinHeaderBboxRange()}.
     */
    @Test
    void tileExtentsIntersectHeaderBbox() {
        TileMatrixSet matrixSet = andorraStore.matrixSet();
        PMTilesHeader header = andorraReader.getHeader();
        BoundingBox2D headerBbox = WebMercatorTransform.latLonToWebMercator(header.geographicBoundingBox());

        for (int z = matrixSet.minZoomLevel(); z <= matrixSet.maxZoomLevel(); z++) {
            TileMatrix matrix = matrixSet.getTileMatrix(z);
            List<TileIndex> archiveTiles =
                    andorraReader.getTileIndicesByZoomLevel(z).toList();

            for (TileIndex tileIndex : archiveTiles) {
                Tile tile = matrix.tile(tileIndex).orElseThrow();
                BoundingBox2D tileExtent = tile.extent();
                assertThat(tileExtent.intersects(headerBbox))
                        .as("tile %s extent %s should intersect header bbox %s", tileIndex, tileExtent, headerBbox)
                        .isTrue();
            }
        }
    }

    @Test
    @Disabled("implement!")
    void findBestZoomLevelResolutionStrategy() {
        fail();
    }
}
