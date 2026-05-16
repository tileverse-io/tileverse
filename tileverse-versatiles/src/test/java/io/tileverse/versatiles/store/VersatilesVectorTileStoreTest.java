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
package io.tileverse.versatiles.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tileverse.geom.BoundingBox2D;
import io.tileverse.jackson.databind.tilejson.v3.VectorLayer;
import io.tileverse.tiling.matrix.Tile;
import io.tileverse.tiling.matrix.TileMatrixSet;
import io.tileverse.tiling.pyramid.TileIndex;
import io.tileverse.tiling.store.TileData;
import io.tileverse.vectortile.model.VectorTile;
import io.tileverse.versatiles.TestArchiveWriter;
import io.tileverse.versatiles.TileFormat;
import io.tileverse.versatiles.UnsupportedTileFormatException;
import io.tileverse.versatiles.VersatilesReader;
import io.tileverse.versatiles.VersatilesTestData;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class VersatilesVectorTileStoreTest {

    @TempDir
    Path tmp;

    VersatilesReader reader;
    VersatilesVectorTileStore store;

    @BeforeEach
    void open() throws IOException {
        Path file = VersatilesTestData.andorra(tmp);
        reader = VersatilesReader.open(file);
        store = new VersatilesVectorTileStore(reader);
    }

    @AfterEach
    void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }

    @Test
    void matrixSetCoversZoomRange() {
        TileMatrixSet matrixSet = store.matrixSet();
        assertThat(matrixSet.minZoomLevel()).isZero();
        assertThat(matrixSet.maxZoomLevel()).isEqualTo(14);
    }

    @Test
    void layerMetadataMatchesShortbread() {
        List<VectorLayer> layers = store.getVectorLayersMetadata();
        List<String> ids = layers.stream().map(VectorLayer::id).toList();
        assertThat(ids).containsExactlyInAnyOrderElementsOf(VersatilesTestData.andorraLayerNames());
    }

    @Test
    void getExtentInWebMercator() {
        // Andorra (lon 1.4..1.8, lat 42.42..42.67) projects to roughly x=156k..200k, y=5.22M..5.27M in EPSG:3857.
        BoundingBox2D extent = store.getExtent();
        assertThat(extent.minX()).isBetween(150_000.0, 170_000.0);
        assertThat(extent.maxX()).isBetween(190_000.0, 210_000.0);
        assertThat(extent.minY()).isBetween(5_220_000.0, 5_240_000.0);
        assertThat(extent.maxY()).isBetween(5_260_000.0, 5_280_000.0);
    }

    // Tile (14, 8261, 6050) covers the center of Andorra la Vella.
    @Test
    void loadTileDecodesLayers() {
        Tile tile = store.matrixSet()
                .getTileMatrix(14)
                .tile(TileIndex.xyz(8261, 6050, 14))
                .orElseThrow();

        Optional<TileData<VectorTile>> data = store.loadTile(tile);

        assertThat(data).isPresent();
        VectorTile vectorTile = data.get().data();
        assertThat(vectorTile.getLayerNames()).contains("buildings", "streets");
        assertThat(vectorTile.getFeatures("buildings")).isNotEmpty();
    }

    @Test
    void rejectsRasterArchive() throws IOException {
        try (VersatilesReader flowers = VersatilesReader.open(VersatilesTestData.flowers(tmp))) {
            assertThatThrownBy(() -> new VersatilesVectorTileStore(flowers))
                    .isInstanceOf(UnsupportedTileFormatException.class)
                    .hasMessageContaining("WEBP");
        }
    }

    // The versatiles tool writes an inverted box when it converts with a bounding box outside the data.
    @Test
    void invertedBoundsCoverTheWorld() throws IOException {
        Path file = new TestArchiveWriter()
                .bounds(10, 42.42, 1.8, 11)
                .tile(0, 0, 0, "world")
                .write(tmp.resolve("inverted.versatiles"));

        try (VersatilesReader inverted = VersatilesReader.open(file)) {
            VersatilesVectorTileStore invertedStore = new VersatilesVectorTileStore(inverted);
            assertThat(invertedStore.matrixSet().getTileMatrix(0).tile(TileIndex.xyz(0, 0, 0)))
                    .isPresent();
        }
    }

    // WebMercatorQuad stops at zoom level 24; deeper tiles stay readable through the reader.
    @Test
    void zoomBeyondQuadIsCapped() throws IOException {
        Path file = new TestArchiveWriter()
                .tile(24, 0, 0, "z24")
                .tile(25, 0, 0, "z25")
                .write(tmp.resolve("deep.versatiles"));

        try (VersatilesReader deep = VersatilesReader.open(file)) {
            VersatilesVectorTileStore deepStore = new VersatilesVectorTileStore(deep);
            assertThat(deepStore.matrixSet().maxZoomLevel()).isEqualTo(24);
            assertThat(deep.getTile(25, 0, 0)).isPresent();
        }
    }

    @Test
    void onlyDeepZoomsAreRejected() throws IOException {
        Path file = new TestArchiveWriter().tile(25, 0, 0, "z25").write(tmp.resolve("deepest.versatiles"));

        try (VersatilesReader deepest = VersatilesReader.open(file)) {
            assertThatThrownBy(() -> new VersatilesVectorTileStore(deepest))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    // GeoJSON is a vector format, but the store decodes Mapbox Vector Tiles only.
    @Test
    void rejectsGeoJsonArchive() throws IOException {
        Path file = new TestArchiveWriter()
                .tileFormat(TileFormat.GEOJSON)
                .tile(0, 0, 0, "{\"type\":\"FeatureCollection\",\"features\":[]}")
                .write(tmp.resolve("geojson.versatiles"));

        try (VersatilesReader geojson = VersatilesReader.open(file)) {
            assertThatThrownBy(() -> new VersatilesVectorTileStore(geojson))
                    .isInstanceOf(UnsupportedTileFormatException.class);
        }
    }
}
