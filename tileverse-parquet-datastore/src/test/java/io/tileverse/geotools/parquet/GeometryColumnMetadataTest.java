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
package io.tileverse.geotools.parquet;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Envelope;

class GeometryColumnMetadataTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void bounds_returnsEmptyEnvelopeWhenBboxIsMissingOrTooShort() {
        GeometryColumnMetadata metadata = new GeometryColumnMetadata();
        assertThat(metadata.bounds()).isEqualTo(new Envelope());

        metadata.setBbox(List.of(1.0, 2.0, 3.0));
        assertThat(metadata.bounds()).isEqualTo(new Envelope());
    }

    @Test
    void bounds_usesTwoDimensionalAndThreeDimensionalBboxLayouts() {
        GeometryColumnMetadata metadata = new GeometryColumnMetadata();
        metadata.setBbox(List.of(-58.0, -34.0, -57.5, -33.5));

        assertThat(metadata.bounds()).isEqualTo(new Envelope(-58.0, -57.5, -34.0, -33.5));

        metadata.setBbox(List.of(-58.0, -34.0, 10.0, -57.5, -33.5, 30.0));
        assertThat(metadata.bounds()).isEqualTo(new Envelope(-58.0, -57.5, -34.0, -33.5));
    }

    @Test
    void accessors_roundTripAllMetadataFields() {
        GeometryColumnMetadata.BboxCovering bboxCovering = new GeometryColumnMetadata.BboxCovering();
        bboxCovering.setXmin(List.of("bbox", "xmin"));
        bboxCovering.setXmax(List.of("bbox", "xmax"));
        bboxCovering.setYmin(List.of("bbox", "ymin"));
        bboxCovering.setYmax(List.of("bbox", "ymax"));
        bboxCovering.setZmin(List.of("bbox", "zmin"));
        bboxCovering.setZmax(List.of("bbox", "zmax"));

        GeometryColumnMetadata.Covering covering = new GeometryColumnMetadata.Covering();
        covering.setBbox(bboxCovering);

        GeometryColumnMetadata metadata = new GeometryColumnMetadata();
        metadata.setEncoding("WKB");
        metadata.setGeometryTypes(List.of("Point", "MultiPoint"));
        metadata.setEdges("planar");
        metadata.setOrientation("counterclockwise");
        metadata.setBbox(List.of(-58.0, -34.0, -57.5, -33.5));
        metadata.setEpoch(2026.25);
        metadata.setCovering(covering);

        assertThat(metadata.getEncoding()).isEqualTo("WKB");
        assertThat(metadata.getGeometryTypes()).containsExactly("Point", "MultiPoint");
        assertThat(metadata.getEdges()).isEqualTo("planar");
        assertThat(metadata.getOrientation()).isEqualTo("counterclockwise");
        assertThat(metadata.getBbox()).containsExactly(-58.0, -34.0, -57.5, -33.5);
        assertThat(metadata.getEpoch()).isEqualTo(2026.25);
        assertThat(metadata.getCovering()).isSameAs(covering);
        assertThat(metadata.getCovering().getBbox()).isSameAs(bboxCovering);
        assertThat(metadata.getCovering().getBbox().getXmin()).containsExactly("bbox", "xmin");
        assertThat(metadata.getCovering().getBbox().getXmax()).containsExactly("bbox", "xmax");
        assertThat(metadata.getCovering().getBbox().getYmin()).containsExactly("bbox", "ymin");
        assertThat(metadata.getCovering().getBbox().getYmax()).containsExactly("bbox", "ymax");
        assertThat(metadata.getCovering().getBbox().getZmin()).containsExactly("bbox", "zmin");
        assertThat(metadata.getCovering().getBbox().getZmax()).containsExactly("bbox", "zmax");
    }

    @Test
    void deserialization_readsNestedCoveringStructure() throws Exception {
        String json =
                """
                {
                  "encoding": "WKB",
                  "geometry_types": ["Polygon"],
                  "bbox": [-58.0, -34.0, -57.5, -33.5],
                  "covering": {
                    "bbox": {
                      "xmin": ["bbox", "xmin"],
                      "xmax": ["bbox", "xmax"],
                      "ymin": ["bbox", "ymin"],
                      "ymax": ["bbox", "ymax"]
                    }
                  }
                }
                """;

        GeometryColumnMetadata metadata = MAPPER.readValue(json, GeometryColumnMetadata.class);

        assertThat(metadata.getEncoding()).isEqualTo("WKB");
        assertThat(metadata.getGeometryTypes()).containsExactly("Polygon");
        assertThat(metadata.bounds()).isEqualTo(new Envelope(-58.0, -57.5, -34.0, -33.5));
        assertThat(metadata.getCovering()).isNotNull();
        assertThat(metadata.getCovering().getBbox().getXmin()).containsExactly("bbox", "xmin");
        assertThat(metadata.getCovering().getBbox().getYmax()).containsExactly("bbox", "ymax");
    }
}
