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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import org.locationtech.jts.geom.Envelope;

/**
 * Metadata for a geometry column in a GeoParquet file.
 *
 * <p>Adapted from the GeoTools GeoParquet module (LGPL 2.1, authored by the same contributor).
 *
 * @see <a href="https://github.com/opengeospatial/geoparquet/blob/main/format-specs/metadata.md">
 *     GeoParquet Metadata Specification</a>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class GeometryColumnMetadata {

    @JsonProperty(value = "encoding", required = true)
    private String encoding;

    @JsonProperty(value = "geometry_types", required = true)
    private List<String> geometryTypes;

    @JsonProperty("crs")
    private JsonNode crs;

    @JsonProperty("edges")
    private String edges;

    @JsonProperty("orientation")
    private String orientation;

    @JsonProperty("bbox")
    private List<Double> bbox;

    @JsonProperty("epoch")
    private Double epoch;

    @JsonProperty("covering")
    private Covering covering;

    public Envelope bounds() {
        if (bbox == null || bbox.size() < 4) {
            return new Envelope();
        }
        double minx = bbox.get(0);
        double miny = bbox.get(1);
        // 6-element bbox: [minx, miny, minz, maxx, maxy, maxz]
        double maxx = bbox.get(bbox.size() == 6 ? 3 : 2);
        double maxy = bbox.get(bbox.size() == 6 ? 4 : 3);
        return new Envelope(minx, maxx, miny, maxy);
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public List<String> getGeometryTypes() {
        return geometryTypes;
    }

    public void setGeometryTypes(List<String> geometryTypes) {
        this.geometryTypes = geometryTypes;
    }

    public JsonNode getCrs() {
        return crs;
    }

    public void setCrs(JsonNode crs) {
        this.crs = crs;
    }

    public String getEdges() {
        return edges;
    }

    public void setEdges(String edges) {
        this.edges = edges;
    }

    public String getOrientation() {
        return orientation;
    }

    public void setOrientation(String orientation) {
        this.orientation = orientation;
    }

    public List<Double> getBbox() {
        return bbox;
    }

    public void setBbox(List<Double> bbox) {
        this.bbox = bbox;
    }

    public Double getEpoch() {
        return epoch;
    }

    public void setEpoch(Double epoch) {
        this.epoch = epoch;
    }

    public Covering getCovering() {
        return covering;
    }

    public void setCovering(Covering covering) {
        this.covering = covering;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Covering {

        @JsonProperty(value = "bbox", required = true)
        private BboxCovering bbox;

        public BboxCovering getBbox() {
            return bbox;
        }

        public void setBbox(BboxCovering bbox) {
            this.bbox = bbox;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class BboxCovering {

        @JsonProperty(value = "xmin", required = true)
        private List<String> xmin;

        @JsonProperty(value = "xmax", required = true)
        private List<String> xmax;

        @JsonProperty(value = "ymin", required = true)
        private List<String> ymin;

        @JsonProperty(value = "ymax", required = true)
        private List<String> ymax;

        @JsonProperty("zmin")
        private List<String> zmin;

        @JsonProperty("zmax")
        private List<String> zmax;

        public List<String> getXmin() {
            return xmin;
        }

        public void setXmin(List<String> xmin) {
            this.xmin = xmin;
        }

        public List<String> getXmax() {
            return xmax;
        }

        public void setXmax(List<String> xmax) {
            this.xmax = xmax;
        }

        public List<String> getYmin() {
            return ymin;
        }

        public void setYmin(List<String> ymin) {
            this.ymin = ymin;
        }

        public List<String> getYmax() {
            return ymax;
        }

        public void setYmax(List<String> ymax) {
            this.ymax = ymax;
        }

        public List<String> getZmin() {
            return zmin;
        }

        public void setZmin(List<String> zmin) {
            this.zmin = zmin;
        }

        public List<String> getZmax() {
            return zmax;
        }

        public void setZmax(List<String> zmax) {
            this.zmax = zmax;
        }
    }
}
