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
package io.tileverse.tiling.matrix;

import static org.assertj.core.api.Assertions.assertThat;

import io.tileverse.tiling.pyramid.CornerOfOrigin;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import javax.xml.stream.XMLStreamException;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

class TileMatrixSetIOTest {

    @Test
    void jsonRoundtripWebMercatorQuad() {
        TileMatrixSet original = DefaultTileMatrixSets.WEB_MERCATOR_QUAD;

        String json = TileMatrixSetIO.toJSON(original);
        TileMatrixSet deserialized = TileMatrixSetIO.readJSON(json);

        assertThat(deserialized.identifier()).isEqualTo("WebMercatorQuad");
        assertThat(deserialized.crsId()).isEqualTo(original.crsId());
        assertThat(deserialized.tileWidth()).isEqualTo(original.tileWidth());
        assertThat(deserialized.tileHeight()).isEqualTo(original.tileHeight());
        assertThat(deserialized.tileMatrices()).hasSameSizeAs(original.tileMatrices());
        assertThat(deserialized.boundingBox()).isEqualTo(original.boundingBox());
        assertThat(deserialized.wellKnownScaleSet())
                .contains(original.wellKnownScaleSet().orElseThrow());

        TileMatrix originalZ0 = original.tileMatrix(0).orElseThrow();
        TileMatrix roundTripZ0 = deserialized.tileMatrix(0).orElseThrow();
        assertThat(roundTripZ0.identifier()).isEqualTo("0");
        assertThat(roundTripZ0.matrixWidth()).isEqualTo(originalZ0.matrixWidth());
        assertThat(roundTripZ0.matrixHeight()).isEqualTo(originalZ0.matrixHeight());
        assertThat(roundTripZ0.scaleDenominator()).isCloseTo(originalZ0.scaleDenominator(), within(1e-6));
        assertThat(roundTripZ0.resolution()).isCloseTo(originalZ0.resolution(), within(1e-6));
        assertThat(roundTripZ0.topLeftCorner()).isEqualTo(originalZ0.topLeftCorner());
    }

    @Test
    void jsonFieldsFollowOgcSchema() throws IOException {
        TileMatrixSet tms = DefaultTileMatrixSets.WEB_MERCATOR_QUAD;
        String json = TileMatrixSetIO.toJSON(tms);

        JsonNode root = JsonMapper.builder().build().readTree(json);
        assertThat(root.get("id").asString()).isEqualTo("WebMercatorQuad");
        assertThat(root.has("crs")).isTrue();
        assertThat(root.has("wellKnownScaleSet")).isTrue();
        assertThat(root.has("boundingBox")).isTrue();
        assertThat(root.get("tileMatrices").isArray()).isTrue();

        JsonNode matrix0 = root.get("tileMatrices").get(0);
        assertThat(matrix0.get("id").asString()).isEqualTo("0");
        assertThat(matrix0.has("scaleDenominator")).isTrue();
        assertThat(matrix0.has("cellSize")).isTrue();
        assertThat(matrix0.has("pointOfOrigin")).isTrue();
        assertThat(matrix0.has("tileWidth")).isTrue();
        assertThat(matrix0.has("tileHeight")).isTrue();
        assertThat(matrix0.has("matrixWidth")).isTrue();
        assertThat(matrix0.has("matrixHeight")).isTrue();
    }

    @Test
    void xmlRoundtripWebMercatorQuad() throws XMLStreamException, IOException {
        TileMatrixSet original = DefaultTileMatrixSets.WEB_MERCATOR_QUAD;

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        TileMatrixSetIO.writeXML(original, out);
        String xml = out.toString();

        assertThat(xml).contains("<Identifier>WebMercatorQuad</Identifier>");
        assertThat(xml).contains("<SupportedCRS>");
        assertThat(xml).contains("<TileMatrix>");
        assertThat(xml).contains("<TopLeftCorner>");

        TileMatrixSet deserialized = TileMatrixSetIO.readXML(new ByteArrayInputStream(out.toByteArray()));
        assertThat(deserialized.identifier()).isEqualTo("WebMercatorQuad");
        assertThat(deserialized.tileMatrices()).hasSameSizeAs(original.tileMatrices());

        TileMatrix originalZ0 = original.tileMatrix(0).orElseThrow();
        TileMatrix roundTripZ0 = deserialized.tileMatrix(0).orElseThrow();
        assertThat(roundTripZ0.matrixWidth()).isEqualTo(originalZ0.matrixWidth());
        assertThat(roundTripZ0.matrixHeight()).isEqualTo(originalZ0.matrixHeight());
        assertThat(roundTripZ0.scaleDenominator()).isCloseTo(originalZ0.scaleDenominator(), within(1e-6));
        assertThat(roundTripZ0.cornerOfOrigin()).isEqualTo(CornerOfOrigin.TOP_LEFT);
    }

    @Test
    void jsonAcceptsScaleDenominatorWithoutCellSize() {
        String json = """
                {
                  "id": "TestSet",
                  "crs": "http://www.opengis.net/def/crs/EPSG/0/3857",
                  "tileMatrices": [
                    {
                      "id": "0",
                      "scaleDenominator": 559082264.028717,
                      "pointOfOrigin": [-20037508.3427892, 20037508.3427892],
                      "tileWidth": 256,
                      "tileHeight": 256,
                      "matrixWidth": 1,
                      "matrixHeight": 1
                    }
                  ]
                }
                """;
        TileMatrixSet tms = TileMatrixSetIO.readJSON(json);
        assertThat(tms.identifier()).isEqualTo("TestSet");
        assertThat(tms.tileMatrices()).hasSize(1);
        TileMatrix m0 = tms.tileMatrix(0).orElseThrow();
        assertThat(m0.matrixWidth()).isEqualTo(1);
        // Resolution derived from scaleDenominator via the CRS units helper.
        assertThat(m0.resolution()).isCloseTo(156543.0339, within(1e-3));
    }

    private static org.assertj.core.data.Offset<Double> within(double tolerance) {
        return org.assertj.core.data.Offset.offset(tolerance);
    }
}
