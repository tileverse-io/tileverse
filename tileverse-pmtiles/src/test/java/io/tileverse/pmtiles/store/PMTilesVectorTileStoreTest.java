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

import static io.tileverse.tiling.pyramid.TileIndex.xyz;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import io.tileverse.jackson.databind.pmtiles.v3.PMTilesMetadata;
import io.tileverse.jackson.databind.tilejson.v3.VectorLayer;
import io.tileverse.pmtiles.InvalidHeaderException;
import io.tileverse.pmtiles.PMTilesHeader;
import io.tileverse.pmtiles.PMTilesReader;
import io.tileverse.pmtiles.PMTilesTestData;
import io.tileverse.tiling.common.BoundingBox2D;
import io.tileverse.tiling.common.Coordinate;
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
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PMTilesVectorTileStoreTest {

    @TempDir
    private Path tmpFolder;

    private PMTilesReader andorraReader;
    private PMTilesVectorTileStore andorraStore;

    @BeforeEach
    void setup() throws IOException, InvalidHeaderException {
        Path file = PMTilesTestData.andorra(tmpFolder);
        andorraReader = new PMTilesReader(file);
        andorraStore = new PMTilesVectorTileStore(andorraReader);
    }

    @Test
    void layerNames() throws IOException, InvalidHeaderException {

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
    void getWithTileIndex() throws IOException {
        TileMatrixSet andorraMatrixSet = andorraStore.matrixSet();

        assertThat(andorraMatrixSet.tilePyramid().cornerOfOrigin()).isEqualTo(CornerOfOrigin.TOP_LEFT);

        System.out.println(
                "Total available indices: " + andorraReader.getTileIndices().count());

        for (int z = andorraMatrixSet.minZoomLevel(); z <= 12 /*andorraMatrixSet.maxZoomLevel()*/; z++) {
            System.out.println("--------------------------------------");
            System.out.println("Zoom level " + z);

            System.out.println("Available indices: "
                    + andorraReader.getTileIndicesByZoomLevel(z).count());
            andorraReader.getTileIndicesByZoomLevel(z).forEach(index -> {
                try {
                    Optional<ByteBuffer> tile = andorraReader.getTile(index);
                    Optional<VectorTile> vectorTile = decode(tile);
                    System.out.printf(
                            "%s -> tile found: %s, decoded: %s %n", index, tile.isPresent(), vectorTile.isPresent());
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });

            TileMatrix zMatrix = andorraMatrixSet.getTileMatrix(z);
            TileRange zRange = zMatrix.tileRange();
            System.out.println("Matrix at z=%d (%s to %s): ".formatted(z, zRange.first(), zRange.last()));
            System.out.println("Available indices: %d".formatted(zRange.count()));
            System.out.printf(
                    "Covers PMTiles indices: %s%n",
                    andorraReader.getTileIndicesByZoomLevel(z).noneMatch(i -> !zMatrix.contains(i)));
            TileIndex index = zRange.first();
            do {
                assertExists(index);
                index = zRange.next(index).orElse(null);
            } while (index != null);
        }
    }

    private Optional<VectorTile> decode(Optional<ByteBuffer> tile) {
        if (tile.isEmpty()) return Optional.empty();
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
        System.out.printf(" -> tile found: %s%n", tileOpt.isPresent());
    }

    @Test
    void debugCoordinateConversion() throws IOException {
        PMTilesHeader header = andorraReader.getHeader();

        System.out.println("=== PMTiles Header Info ===");
        System.out.println("Geographic bounds: " + header.geographicBoundingBox());
        System.out.println("Min zoom: " + header.minZoom() + ", Max zoom: " + header.maxZoom());

        // Convert to WebMercator
        BoundingBox2D webMercatorExtent = WebMercatorTransform.latLonToWebMercator(header.geographicBoundingBox());
        System.out.println("WebMercator extent: " + webMercatorExtent);

        TileMatrixSet baseTms = DefaultTileMatrixSets.WEB_MERCATOR_QUAD.toBuilder()
                .zoomRange(header.minZoom(), header.maxZoom())
                .build();

        // Check what tiles the geographic extent should produce at different zoom levels
        for (int z = 0; z <= 5; z++) {
            System.out.println("\n--- Zoom Level " + z + " ---");

            // What tiles does the geographic extent suggest?
            TileMatrix matrix = baseTms.getTileMatrix(z);
            Optional<io.tileverse.tiling.pyramid.TileRange> expectedRange = matrix.extentToRange(webMercatorExtent);

            System.out.println("Geographic extent suggests tile range: "
                    + (expectedRange.isPresent() ? expectedRange.get() : "NONE"));

            if (expectedRange.isPresent()) {
                io.tileverse.tiling.pyramid.TileRange range = expectedRange.get();
                System.out.println("  Range bounds: (" + range.minx() + "," + range.miny() + ") to (" + range.maxx()
                        + "," + range.maxy() + ")");
                System.out.println("  First tile would be: (" + range.minx() + "," + range.miny() + "," + z + ")");
            }

            // What tiles actually exist in PMTiles?
            List<io.tileverse.tiling.pyramid.TileIndex> actualTiles =
                    andorraReader.getTileIndicesByZoomLevel(z).collect(java.util.stream.Collectors.toList());

            System.out.println("Actual PMTiles tiles: " + actualTiles.size() + " tiles");
            actualTiles.forEach(tile -> System.out.println("  " + tile));

            // Check if actual tiles are within the expected range
            if (expectedRange.isPresent() && !actualTiles.isEmpty()) {
                io.tileverse.tiling.pyramid.TileRange range = expectedRange.get();
                for (io.tileverse.tiling.pyramid.TileIndex tile : actualTiles) {
                    boolean inRange = tile.x() >= range.minx()
                            && tile.x() <= range.maxx()
                            && tile.y() >= range.miny()
                            && tile.y() <= range.maxy();
                    System.out.println("  " + tile + " in expected range: " + inRange);
                }
            }
        }
    }

    @Test
    void debugZoomLevel11TileRange() {
        TileMatrixSet andorraMatrixSet = andorraStore.matrixSet();
        int targetZoom = 11;

        System.out.println("=== Debugging Zoom Level " + targetZoom + " ===");

        TileMatrix zMatrix = andorraMatrixSet.getTileMatrix(targetZoom);
        TileRange zRange = zMatrix.tileRange();

        System.out.println("TileMatrix: " + zMatrix);
        System.out.println("TileRange: " + zRange);
        System.out.println("Axis Origin: " + zRange.cornerOfOrigin());
        System.out.println("Range bounds: (" + zRange.minx() + "," + zRange.miny() + ") to (" + zRange.maxx() + ","
                + zRange.maxy() + ")");
        System.out.println("Expected count: " + zRange.count() + " tiles");
        System.out.println("First tile: " + zRange.first());
        System.out.println("Last tile: " + zRange.last());

        // Manual traversal with debugging
        System.out.println("\n--- Manual Traversal ---");
        java.util.List<TileIndex> traversed = new java.util.ArrayList<>();
        TileIndex index = zRange.min();
        int count = 0;

        do {
            traversed.add(index);
            count++;
            System.out.println("Tile " + count + ": " + index);

            Optional<TileIndex> nextOpt = zRange.next(index);
            if (nextOpt.isPresent()) {
                index = nextOpt.get();
            } else {
                System.out.println("No next tile - traversal complete");
                break;
            }
        } while (count < 20); // Safety limit to prevent infinite loop

        System.out.println("\nTraversed " + count + " tiles, expected " + zRange.count() + " tiles");

        if (count != zRange.count()) {
            System.out.println("ERROR: Traversal count mismatch!");

            // Show what tiles should exist in the range
            System.out.println("\nExpected tiles in range:");
            for (long x = zRange.minx(); x <= zRange.maxx(); x++) {
                for (long y = zRange.miny(); y <= zRange.maxy(); y++) {
                    TileIndex expected = xyz(x, y, targetZoom);
                    boolean wasTraversed = traversed.contains(expected);
                    System.out.println("  " + expected + " - traversed: " + wasTraversed);
                }
            }
        }
    }

    @Test
    @Disabled("implement!")
    void findBestZoomLevelResolutionStrategy() {
        fail();
    }

    @Test
    void testLatLonToWebMercatorAccuracy() {
        PMTilesHeader header = andorraReader.getHeader();
        BoundingBox2D geoExtent = header.geographicBoundingBox();

        System.out.println("=== Testing LatLon to WebMercator Accuracy ===");
        System.out.println("Geographic extent: " + geoExtent);

        // Convert to WebMercator
        BoundingBox2D wmExtent = WebMercatorTransform.latLonToWebMercator(geoExtent);
        System.out.println("WebMercator extent: " + wmExtent);

        // Test at different zoom levels what tile coordinates this extent maps to
        TileMatrixSet baseTms = DefaultTileMatrixSets.WEB_MERCATOR_QUAD;

        for (int zoom = 0; zoom <= 8; zoom++) {
            System.out.println("\n--- Zoom " + zoom + " ---");

            TileMatrix matrix = baseTms.getTileMatrix(zoom);

            // Convert each corner of the WebMercator extent to tile coordinates
            Coordinate minCorner = wmExtent.lowerLeft();
            Coordinate maxCorner = wmExtent.upperRight();

            Optional<Tile> minTileOpt = matrix.coordinateToTile(minCorner);
            Optional<Tile> maxTileOpt = matrix.coordinateToTile(maxCorner);

            if (minTileOpt.isPresent() && maxTileOpt.isPresent()) {
                Tile minTile = minTileOpt.get();
                Tile maxTile = maxTileOpt.get();

                System.out.printf(
                        "Coordinate (%f, %f) -> Tile (%d, %d)%n",
                        minCorner.x(), minCorner.y(), minTile.x(), minTile.y());
                System.out.printf(
                        "Coordinate (%f, %f) -> Tile (%d, %d)%n",
                        maxCorner.x(), maxCorner.y(), maxTile.x(), maxTile.y());

                // What are the actual tile extents?
                System.out.printf("Min tile extent: %s%n", minTile.extent());
                System.out.printf("Max tile extent: %s%n", maxTile.extent());

                // How much do they differ from the WebMercator extent?
                BoundingBox2D combinedTileExtent = minTile.extent().union(maxTile.extent());
                System.out.printf("Combined tile extent: %s%n", combinedTileExtent);

                double deltaMinX = Math.abs(wmExtent.minX() - combinedTileExtent.minX());
                double deltaMaxX = Math.abs(wmExtent.maxX() - combinedTileExtent.maxX());
                double deltaMinY = Math.abs(wmExtent.minY() - combinedTileExtent.minY());
                double deltaMaxY = Math.abs(wmExtent.maxY() - combinedTileExtent.maxY());

                System.out.printf(
                        "Extent deltas: minX=%.6f, maxX=%.6f, minY=%.6f, maxY=%.6f%n",
                        deltaMinX, deltaMaxX, deltaMinY, deltaMaxY);

                // Check if the actual PMTiles tiles are within this range
                List<io.tileverse.tiling.pyramid.TileIndex> actualTiles =
                        andorraReader.getTileIndicesByZoomLevel(zoom).collect(java.util.stream.Collectors.toList());

                if (!actualTiles.isEmpty()) {
                    System.out.println("Actual PMTiles tiles: " + actualTiles);
                    for (io.tileverse.tiling.pyramid.TileIndex actualTile : actualTiles) {
                        boolean inRange = actualTile.x() >= minTile.x()
                                && actualTile.x() <= maxTile.x()
                                && actualTile.y() >= minTile.y()
                                && actualTile.y() <= maxTile.y();
                        System.out.printf("  %s in predicted range: %s%n", actualTile, inRange);

                        // What extent does the actual tile cover?
                        Optional<Tile> actualTileObj = matrix.tile(actualTile);
                        if (actualTileObj.isPresent()) {
                            BoundingBox2D actualExtent = actualTileObj.get().extent();
                            System.out.printf("    Actual tile extent: %s%n", actualExtent);

                            // Does it intersect with the geographic WebMercator extent?
                            boolean intersects = actualExtent.intersects(wmExtent);
                            System.out.printf("    Intersects WM extent: %s%n", intersects);
                        }
                    }
                }
            }
        }
    }
}
