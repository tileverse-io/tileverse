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
import io.tileverse.tiling.pyramid.CornerOfOrigin;
import io.tileverse.tiling.pyramid.TilePyramid;
import io.tileverse.tiling.pyramid.TileRange;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import javax.xml.XMLConstants;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * Reads a {@link TileMatrixSet} from OGC TMS XML using StAX.
 *
 * <p>The reader is namespace-agnostic and matches elements by local name so it accepts both the OGC TMS 2D namespace
 * and the historical WMTS 1.0 / OWS 1.1 namespace forms.
 *
 * @since 1.4
 */
public class TileMatrixSetXmlReader {

    /**
     * Read a tile matrix set from the given input stream.
     *
     * @param in the input stream
     * @return the parsed tile matrix set
     * @throws XMLStreamException if the input is not well-formed
     * @throws IOException if a required element is missing or has an invalid value
     */
    public TileMatrixSet read(InputStream in) throws XMLStreamException, IOException {
        XMLInputFactory factory = XMLInputFactory.newInstance();
        factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
        factory.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");

        XMLStreamReader reader = factory.createXMLStreamReader(in);

        ParseState state = new ParseState();
        Deque<String> elementStack = new ArrayDeque<>();

        while (reader.hasNext()) {
            final int event = reader.next();
            processEvent(reader, state, elementStack, event);
        }

        if (state.identifier == null) {
            throw new IOException("TileMatrixSet/Identifier is required");
        }
        if (state.supportedCRS == null) {
            throw new IOException("TileMatrixSet/SupportedCRS is required");
        }
        if (state.parsedMatrices.isEmpty()) {
            throw new IOException("At least one TileMatrix is required");
        }

        return buildTileMatrixSet(state);
    }

    private void processEvent(XMLStreamReader reader, ParseState state, Deque<String> elementStack, final int event)
            throws XMLStreamException {
        switch (event) {
            case XMLStreamConstants.START_ELEMENT -> {
                String name = reader.getLocalName();
                elementStack.push(name);
                if ("TileMatrix".equals(name)) {
                    state.parsedMatrices.add(parseTileMatrix(reader));
                    elementStack.pop(); // parseTileMatrix consumed the END_ELEMENT
                } else if ("BoundingBox".equals(name)) {
                    state.boundingBox = parseBoundingBox(reader);
                    elementStack.pop();
                } else if ("Keywords".equals(name)) {
                    parseKeywords(reader, state);
                    elementStack.pop();
                } else if (elementStack.size() == 2) {
                    // Direct children of TileMatrixSet
                    switch (name) {
                        case "Identifier" -> state.identifier = reader.getElementText();
                        case "Title" -> state.title = reader.getElementText();
                        case "Abstract" -> state.abstractText = reader.getElementText();
                        case "SupportedCRS" -> state.supportedCRS = reader.getElementText();
                        case "WellKnownScaleSet" -> state.wellKnownScaleSet = reader.getElementText();
                        default -> {
                            // ignore unknown elements
                        }
                    }
                    elementStack.pop(); // getElementText consumed the END_ELEMENT
                }
            }
            case XMLStreamConstants.END_ELEMENT -> {
                if (!elementStack.isEmpty()) {
                    elementStack.pop();
                }
            }
            default -> {
                // ignore
            }
        }
    }

    private TileMatrixSet buildTileMatrixSet(ParseState state) throws IOException {
        state.parsedMatrices.sort(Comparator.comparingDouble((ParsedMatrix m) -> m.scaleDenominator)
                .reversed());

        ParsedMatrix first = state.parsedMatrices.get(0);
        int tileWidth = first.tileWidth;
        int tileHeight = first.tileHeight;
        CornerOfOrigin corner = first.cornerOfOrigin;

        TilePyramid.Builder pyramidBuilder = TilePyramid.builder().cornerOfOrigin(corner);
        double[] resolutions = new double[state.parsedMatrices.size()];

        for (int z = 0; z < state.parsedMatrices.size(); z++) {
            ParsedMatrix pm = state.parsedMatrices.get(z);
            if (pm.tileWidth != tileWidth || pm.tileHeight != tileHeight) {
                throw new IOException(
                        "Non-uniform tile sizes are not supported: matrix " + pm.identifier + " differs from zoom 0");
            }
            resolutions[z] = CrsUnits.scaleDenominatorToResolution(pm.scaleDenominator, state.supportedCRS);
            TileRange range = TileRange.of(0, 0, pm.matrixWidth - 1, pm.matrixHeight - 1, z, corner);
            pyramidBuilder.level(range);
        }

        BoundingBox2D extent = state.boundingBox;
        if (extent == null) {
            extent = deriveExtent(first, resolutions[0], corner);
        }

        String crsId = normalizeCrsId(state.supportedCRS);
        TileMatrixSetBuilder builder = TileMatrixSet.builder()
                .identifier(state.identifier)
                .tilePyramid(pyramidBuilder.build())
                .crs(crsId)
                .supportedCRS(URI.create(state.supportedCRS))
                .tileSize(tileWidth, tileHeight)
                .extent(extent)
                .resolutions(resolutions);
        if (state.title != null) {
            builder.title(state.title);
        }
        if (state.abstractText != null) {
            builder.abstractDescription(state.abstractText);
        }
        if (!state.keywords.isEmpty()) {
            builder.keywords(state.keywords);
        }
        if (state.wellKnownScaleSet != null) {
            builder.wellKnownScaleSet(URI.create(state.wellKnownScaleSet));
        }
        return builder.build();
    }

    private BoundingBox2D deriveExtent(ParsedMatrix first, double resolution, CornerOfOrigin corner) {
        double tileSpanX = first.tileWidth * resolution;
        double tileSpanY = first.tileHeight * resolution;
        double minX;
        double minY;
        double maxX;
        double maxY;
        switch (corner) {
            case TOP_LEFT -> {
                minX = first.originX;
                maxY = first.originY;
                maxX = minX + tileSpanX * first.matrixWidth;
                minY = maxY - tileSpanY * first.matrixHeight;
            }
            case BOTTOM_LEFT -> {
                minX = first.originX;
                minY = first.originY;
                maxX = minX + tileSpanX * first.matrixWidth;
                maxY = minY + tileSpanY * first.matrixHeight;
            }
            default -> throw new IllegalStateException("Unsupported corner: " + corner);
        }
        return BoundingBox2D.extent(minX, minY, maxX, maxY);
    }

    private BoundingBox2D parseBoundingBox(XMLStreamReader reader) throws XMLStreamException {
        double minx = 0;
        double miny = 0;
        double maxx = 0;
        double maxy = 0;
        boolean hasLower = false;
        boolean hasUpper = false;

        while (reader.hasNext()) {
            int event = reader.next();
            if (event == XMLStreamConstants.START_ELEMENT) {
                String name = reader.getLocalName();
                if ("LowerCorner".equals(name)) {
                    String[] coords = reader.getElementText().trim().split("\\s+");
                    minx = Double.parseDouble(coords[0]);
                    miny = Double.parseDouble(coords[1]);
                    hasLower = true;
                } else if ("UpperCorner".equals(name)) {
                    String[] coords = reader.getElementText().trim().split("\\s+");
                    maxx = Double.parseDouble(coords[0]);
                    maxy = Double.parseDouble(coords[1]);
                    hasUpper = true;
                }
            } else if (event == XMLStreamConstants.END_ELEMENT && "BoundingBox".equals(reader.getLocalName())) {
                break;
            }
        }
        if (!hasLower || !hasUpper) {
            throw new XMLStreamException("BoundingBox must contain LowerCorner and UpperCorner");
        }
        return BoundingBox2D.extent(minx, miny, maxx, maxy);
    }

    private void parseKeywords(XMLStreamReader reader, ParseState state) throws XMLStreamException {
        while (reader.hasNext()) {
            int event = reader.next();
            if (event == XMLStreamConstants.START_ELEMENT) {
                if ("Keyword".equals(reader.getLocalName())) {
                    state.keywords.add(reader.getElementText());
                }
            } else if (event == XMLStreamConstants.END_ELEMENT && "Keywords".equals(reader.getLocalName())) {
                return;
            }
        }
    }

    private ParsedMatrix parseTileMatrix(XMLStreamReader reader) throws XMLStreamException {
        ParsedMatrix pm = new ParsedMatrix();
        pm.cornerOfOrigin = CornerOfOrigin.TOP_LEFT;
        while (reader.hasNext()) {
            int event = reader.next();
            if (event == XMLStreamConstants.START_ELEMENT) {
                String name = reader.getLocalName();
                switch (name) {
                    case "Identifier" -> pm.identifier = reader.getElementText();
                    case "ScaleDenominator" -> pm.scaleDenominator = Double.parseDouble(reader.getElementText());
                    case "TopLeftCorner" -> {
                        String[] coords = reader.getElementText().trim().split("\\s+");
                        pm.originX = Double.parseDouble(coords[0]);
                        pm.originY = Double.parseDouble(coords[1]);
                    }
                    case "CornerOfOrigin" -> pm.cornerOfOrigin = parseCorner(reader.getElementText());
                    case "TileWidth" -> pm.tileWidth = Integer.parseInt(reader.getElementText());
                    case "TileHeight" -> pm.tileHeight = Integer.parseInt(reader.getElementText());
                    case "MatrixWidth" -> pm.matrixWidth = Long.parseLong(reader.getElementText());
                    case "MatrixHeight" -> pm.matrixHeight = Long.parseLong(reader.getElementText());
                    default -> {
                        // ignore unknown fields (Title, Abstract, Keywords, VariableMatrixWidths)
                    }
                }
            } else if (event == XMLStreamConstants.END_ELEMENT && "TileMatrix".equals(reader.getLocalName())) {
                break;
            }
        }
        return pm;
    }

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

    private CornerOfOrigin parseCorner(String s) {
        String normalized = s.trim().toUpperCase();
        return switch (normalized) {
            case "TOPLEFT", "TOP_LEFT", "UPPERLEFT" -> CornerOfOrigin.TOP_LEFT;
            case "BOTTOMLEFT", "BOTTOM_LEFT", "LOWERLEFT" -> CornerOfOrigin.BOTTOM_LEFT;
            default -> throw new IllegalArgumentException("Unsupported CornerOfOrigin: " + s);
        };
    }

    private static final class ParseState {
        String identifier;
        String title;
        String abstractText;
        String supportedCRS;
        String wellKnownScaleSet;
        BoundingBox2D boundingBox;
        final List<String> keywords = new ArrayList<>();
        final List<ParsedMatrix> parsedMatrices = new ArrayList<>();
    }

    private static final class ParsedMatrix {
        String identifier;
        double scaleDenominator;
        double originX;
        double originY;
        int tileWidth;
        int tileHeight;
        long matrixWidth;
        long matrixHeight;
        CornerOfOrigin cornerOfOrigin;
    }
}
