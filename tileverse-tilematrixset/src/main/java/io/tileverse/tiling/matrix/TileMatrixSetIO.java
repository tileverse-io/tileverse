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

import io.tileverse.geom.BoundingBox2D;
import io.tileverse.jackson.databind.tms.v1.BoundingBoxJson;
import io.tileverse.jackson.databind.tms.v1.TileMatrixJson;
import io.tileverse.jackson.databind.tms.v1.TileMatrixSetJson;
import io.tileverse.tiling.pyramid.CornerOfOrigin;
import io.tileverse.tiling.pyramid.TilePyramid;
import io.tileverse.tiling.pyramid.TileRange;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import javax.xml.stream.XMLStreamException;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.json.JsonMapper;

/**
 * Read and write {@link TileMatrixSet} instances using the encodings defined by OGC 17-083r2 (JSON in Annex C, XML in
 * Annex D).
 *
 * <p>The JSON encoding round-trips through {@link io.tileverse.jackson.databind.tms.v1 the data-binding records}; the
 * XML encoding is handled by {@link TileMatrixSetXmlReader} / {@link TileMatrixSetXmlWriter}.
 *
 * @since 1.4
 */
public final class TileMatrixSetIO {

    private TileMatrixSetIO() {
        // utility
    }

    // -----------------------------------------------------------------------
    // JSON
    // -----------------------------------------------------------------------

    /** Read a TileMatrixSet from JSON. */
    public static TileMatrixSet readJSON(InputStream in) {
        return fromJsonDto(jsonMapper().readValue(in, TileMatrixSetJson.class));
    }

    /** Read a TileMatrixSet from JSON. */
    public static TileMatrixSet readJSON(Reader in) {
        return fromJsonDto(jsonMapper().readValue(in, TileMatrixSetJson.class));
    }

    /** Read a TileMatrixSet from a JSON string. */
    public static TileMatrixSet readJSON(String json) {
        return fromJsonDto(jsonMapper().readValue(json, TileMatrixSetJson.class));
    }

    /** Write a TileMatrixSet as JSON. */
    public static void writeJSON(TileMatrixSet tms, OutputStream out) {
        prettyJsonMapper().writeValue(out, toJsonDto(tms));
    }

    /** Write a TileMatrixSet as JSON. */
    public static void writeJSON(TileMatrixSet tms, Writer out) {
        prettyJsonMapper().writeValue(out, toJsonDto(tms));
    }

    /** Render a TileMatrixSet to a JSON string. */
    public static String toJSON(TileMatrixSet tms) {
        return prettyJsonMapper().writeValueAsString(toJsonDto(tms));
    }

    // -----------------------------------------------------------------------
    // XML
    // -----------------------------------------------------------------------

    /** Read a TileMatrixSet from OGC TMS XML. */
    public static TileMatrixSet readXML(InputStream in) throws XMLStreamException, IOException {
        return new TileMatrixSetXmlReader().read(in);
    }

    /** Write a TileMatrixSet as OGC TMS XML. */
    public static void writeXML(TileMatrixSet tms, OutputStream out) throws XMLStreamException {
        new TileMatrixSetXmlWriter().write(tms, out);
    }

    // -----------------------------------------------------------------------
    // DTO conversion (live model <-> Jackson DTO)
    // -----------------------------------------------------------------------

    /** Convert a live {@link TileMatrixSet} to its JSON DTO representation. */
    public static TileMatrixSetJson toJsonDto(TileMatrixSet tms) {
        List<TileMatrixJson> matrices = new ArrayList<>();
        for (TileMatrix matrix : tms.tileMatrices()) {
            matrices.add(toJsonDto(matrix));
        }
        BoundingBox2D bbox = tms.boundingBox();
        BoundingBoxJson bboxJson = new BoundingBoxJson(
                List.of(bbox.minX(), bbox.minY()),
                List.of(bbox.maxX(), bbox.maxY()),
                tms.supportedCRS().toString());
        return new TileMatrixSetJson(
                tms.identifier(),
                tms.title().orElse(null),
                tms.abstractDescription().orElse(null),
                tms.keywords().isEmpty() ? null : tms.keywords(),
                null,
                tms.supportedCRS().toString(),
                null,
                tms.wellKnownScaleSet().map(URI::toString).orElse(null),
                bboxJson,
                matrices);
    }

    /** Convert a live {@link TileMatrix} to its JSON DTO representation. */
    public static TileMatrixJson toJsonDto(TileMatrix matrix) {
        var origin = matrix.topLeftCorner();
        return new TileMatrixJson(
                matrix.identifier(),
                null,
                null,
                null,
                matrix.scaleDenominator(),
                matrix.resolution(),
                matrix.cornerOfOrigin() == CornerOfOrigin.TOP_LEFT
                        ? null
                        : matrix.cornerOfOrigin().name(),
                List.of(origin.x(), origin.y()),
                matrix.tileWidth(),
                matrix.tileHeight(),
                matrix.matrixWidth(),
                matrix.matrixHeight(),
                null);
    }

    /** Convert a JSON DTO into a live {@link TileMatrixSet}. */
    public static TileMatrixSet fromJsonDto(TileMatrixSetJson dto) {
        if (dto.id() == null) {
            throw new IllegalArgumentException("TileMatrixSet 'id' is required");
        }
        if (dto.crs() == null) {
            throw new IllegalArgumentException("TileMatrixSet 'crs' is required");
        }
        if (dto.tileMatrices() == null || dto.tileMatrices().isEmpty()) {
            throw new IllegalArgumentException("TileMatrixSet 'tileMatrices' must contain at least one entry");
        }

        String crsUri = dto.crs();
        String crsId = normalizeCrsId(crsUri);
        List<TileMatrixJson> sortedMatrices = new ArrayList<>(dto.tileMatrices());
        sortedMatrices.sort(
                Comparator.comparingDouble(TileMatrixSetIO::sortKeyFor).reversed());

        TileMatrixJson firstMatrix = sortedMatrices.get(0);
        int tileWidth = firstMatrix.tileWidth();
        int tileHeight = firstMatrix.tileHeight();
        CornerOfOrigin corner = parseCorner(firstMatrix.cornerOfOrigin());

        TilePyramid.Builder pyramidBuilder = TilePyramid.builder().cornerOfOrigin(corner);
        double[] resolutions = new double[sortedMatrices.size()];

        for (int z = 0; z < sortedMatrices.size(); z++) {
            TileMatrixJson m = sortedMatrices.get(z);
            resolutions[z] = cellSizeOrDerive(m, crsId);
            TileRange range = TileRange.of(0, 0, m.matrixWidth() - 1, m.matrixHeight() - 1, z, corner);
            pyramidBuilder.level(range);
        }

        BoundingBox2D extent = deriveExtent(sortedMatrices.get(0), resolutions[0], corner, dto.boundingBox());

        TileMatrixSetBuilder builder = TileMatrixSet.builder()
                .identifier(dto.id())
                .tilePyramid(pyramidBuilder.build())
                .crs(crsId)
                .supportedCRS(URI.create(crsUri))
                .tileSize(tileWidth, tileHeight)
                .extent(extent)
                .resolutions(resolutions);

        if (dto.title() != null) {
            builder.title(dto.title());
        }
        if (dto.description() != null) {
            builder.abstractDescription(dto.description());
        }
        if (dto.keywords() != null && !dto.keywords().isEmpty()) {
            builder.keywords(dto.keywords());
        }
        if (dto.wellKnownScaleSet() != null) {
            builder.wellKnownScaleSet(URI.create(dto.wellKnownScaleSet()));
        }
        return builder.build();
    }

    /**
     * Convert a CRS URI such as {@code http://www.opengis.net/def/crs/EPSG/0/3857} into the {@code EPSG:NNNN} short
     * form preferred by the existing model. Pass-through if the input does not match a recognized pattern.
     */
    private static String normalizeCrsId(String crsUri) {
        if (crsUri == null) {
            return null;
        }
        String trimmed = crsUri.trim();
        if (trimmed.startsWith("http://www.opengis.net/def/crs/")
                || trimmed.startsWith("https://www.opengis.net/def/crs/")) {
            String[] parts = trimmed.split("/");
            if (parts.length >= 3) {
                String authority = parts[parts.length - 3];
                String code = parts[parts.length - 1];
                if ("OGC".equalsIgnoreCase(authority) && "CRS84".equalsIgnoreCase(code)) {
                    return "CRS:84";
                }
                return authority + ":" + code;
            }
        }
        return trimmed;
    }

    private static double sortKeyFor(TileMatrixJson m) {
        Double sd = m.scaleDenominator();
        if (sd != null) {
            return sd;
        }
        Double cs = m.cellSize();
        return cs != null ? cs : 0d;
    }

    private static CornerOfOrigin parseCorner(String cornerName) {
        if (cornerName == null || cornerName.isBlank()) {
            return CornerOfOrigin.TOP_LEFT;
        }
        // Spec uses camelCase ("topLeft"/"bottomLeft"); CornerOfOrigin enum uses UPPER_SNAKE.
        String normalized = cornerName.trim().toUpperCase();
        return switch (normalized) {
            case "TOPLEFT", "TOP_LEFT", "UPPERLEFT", "UPPER_LEFT" -> CornerOfOrigin.TOP_LEFT;
            case "BOTTOMLEFT", "BOTTOM_LEFT", "LOWERLEFT", "LOWER_LEFT" -> CornerOfOrigin.BOTTOM_LEFT;
            default -> throw new IllegalArgumentException("Unsupported cornerOfOrigin: " + cornerName);
        };
    }

    private static double cellSizeOrDerive(TileMatrixJson m, String crsId) {
        if (m.cellSize() != null && m.cellSize() > 0) {
            return m.cellSize();
        }
        if (m.scaleDenominator() != null && m.scaleDenominator() > 0) {
            return CrsUnits.scaleDenominatorToResolution(m.scaleDenominator(), crsId);
        }
        throw new IllegalArgumentException(
                "TileMatrix '" + m.id() + "' must declare either 'cellSize' or 'scaleDenominator'");
    }

    private static BoundingBox2D deriveExtent(
            TileMatrixJson firstMatrix, double resolution, CornerOfOrigin corner, BoundingBoxJson bboxJson) {
        if (bboxJson != null) {
            List<Double> lower = bboxJson.lowerLeft();
            List<Double> upper = bboxJson.upperRight();
            if (lower != null && upper != null && lower.size() >= 2 && upper.size() >= 2) {
                return BoundingBox2D.extent(lower.get(0), lower.get(1), upper.get(0), upper.get(1));
            }
        }
        // Compute from the first matrix's pointOfOrigin + tileSpan * matrixWidth/matrixHeight
        List<Double> origin = firstMatrix.pointOfOrigin();
        if (origin == null || origin.size() < 2) {
            throw new IllegalArgumentException("Cannot derive extent: first TileMatrix has no pointOfOrigin");
        }
        double originX = origin.get(0);
        double originY = origin.get(1);
        double tileSpanX = firstMatrix.tileWidth() * resolution;
        double tileSpanY = firstMatrix.tileHeight() * resolution;
        double minX;
        double minY;
        double maxX;
        double maxY;
        switch (corner) {
            case TOP_LEFT -> {
                minX = originX;
                maxY = originY;
                maxX = minX + tileSpanX * firstMatrix.matrixWidth();
                minY = maxY - tileSpanY * firstMatrix.matrixHeight();
            }
            case BOTTOM_LEFT -> {
                minX = originX;
                minY = originY;
                maxX = minX + tileSpanX * firstMatrix.matrixWidth();
                maxY = minY + tileSpanY * firstMatrix.matrixHeight();
            }
            default -> throw new IllegalStateException("Unsupported corner: " + corner);
        }
        return BoundingBox2D.extent(minX, minY, maxX, maxY);
    }

    private static ObjectMapper jsonMapper() {
        return JsonMapper.builder().build();
    }

    private static ObjectMapper prettyJsonMapper() {
        return JsonMapper.builder().enable(SerializationFeature.INDENT_OUTPUT).build();
    }
}
