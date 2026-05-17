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
import io.tileverse.geom.Coordinate;
import io.tileverse.tiling.pyramid.CornerOfOrigin;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

/**
 * Writes a {@link TileMatrixSet} to OGC TMS XML using a single default namespace.
 *
 * <p>This writer emits the structure described by OGC 17-083r2 Annex D, simplified to use a single default namespace
 * for both TMS elements and the {@code Identifier}/{@code Title}/{@code Abstract}/{@code Keywords} elements that the
 * full WMTS schema sources from the OWS namespace. {@link TileMatrixSetXmlReader} accepts either form.
 *
 * @since 1.4
 */
public class TileMatrixSetXmlWriter {

    private static final String NS = "http://www.opengis.net/tms/1.0";

    /**
     * Write the given tile matrix set to the output stream as XML in UTF-8.
     *
     * @param tms the tile matrix set
     * @param out the destination stream
     * @throws XMLStreamException if an XML error occurs
     */
    public void write(TileMatrixSet tms, OutputStream out) throws XMLStreamException {
        XMLStreamWriter writer =
                XMLOutputFactory.newInstance().createXMLStreamWriter(out, StandardCharsets.UTF_8.name());
        writer.setDefaultNamespace(NS);
        writer.writeStartDocument("UTF-8", "1.0");

        writer.writeStartElement(NS, "TileMatrixSet");
        writer.writeDefaultNamespace(NS);

        writeElement(writer, "Identifier", tms.identifier());
        tms.title().ifPresent(t -> writeElementUnchecked(writer, "Title", t));
        tms.abstractDescription().ifPresent(a -> writeElementUnchecked(writer, "Abstract", a));
        writeKeywords(writer, tms);
        writeElement(writer, "SupportedCRS", tms.supportedCRS().toString());
        if (tms.wellKnownScaleSet().isPresent()) {
            writeElement(
                    writer, "WellKnownScaleSet", tms.wellKnownScaleSet().get().toString());
        }
        writeBoundingBox(writer, tms.boundingBox(), tms.supportedCRS().toString());

        for (TileMatrix matrix : tms.tileMatrices()) {
            writeTileMatrix(writer, matrix);
        }

        writer.writeEndElement(); // TileMatrixSet
        writer.writeEndDocument();
        writer.flush();
    }

    private void writeKeywords(XMLStreamWriter writer, TileMatrixSet tms) throws XMLStreamException {
        if (tms.keywords().isEmpty()) {
            return;
        }
        writer.writeStartElement(NS, "Keywords");
        for (String kw : tms.keywords()) {
            writeElement(writer, "Keyword", kw);
        }
        writer.writeEndElement();
    }

    private void writeBoundingBox(XMLStreamWriter writer, BoundingBox2D bbox, String crsRef) throws XMLStreamException {
        writer.writeStartElement(NS, "BoundingBox");
        writer.writeAttribute("crs", crsRef);
        writeElement(writer, "LowerCorner", bbox.minX() + " " + bbox.minY());
        writeElement(writer, "UpperCorner", bbox.maxX() + " " + bbox.maxY());
        writer.writeEndElement();
    }

    private void writeTileMatrix(XMLStreamWriter writer, TileMatrix matrix) throws XMLStreamException {
        writer.writeStartElement(NS, "TileMatrix");

        writeElement(writer, "Identifier", matrix.identifier());
        writeElement(writer, "ScaleDenominator", Double.toString(matrix.scaleDenominator()));

        Coordinate origin = matrix.topLeftCorner();
        writeElement(writer, "TopLeftCorner", origin.x() + " " + origin.y());
        if (matrix.cornerOfOrigin() != CornerOfOrigin.TOP_LEFT) {
            writeElement(writer, "CornerOfOrigin", matrix.cornerOfOrigin().name());
        }

        writeElement(writer, "TileWidth", Integer.toString(matrix.tileWidth()));
        writeElement(writer, "TileHeight", Integer.toString(matrix.tileHeight()));
        writeElement(writer, "MatrixWidth", Long.toString(matrix.matrixWidth()));
        writeElement(writer, "MatrixHeight", Long.toString(matrix.matrixHeight()));

        writer.writeEndElement(); // TileMatrix
    }

    private void writeElement(XMLStreamWriter writer, String localName, String content) throws XMLStreamException {
        writer.writeStartElement(NS, localName);
        writer.writeCharacters(content);
        writer.writeEndElement();
    }

    private void writeElementUnchecked(XMLStreamWriter writer, String localName, String content) {
        try {
            writeElement(writer, localName, content);
        } catch (XMLStreamException e) {
            throw new IllegalStateException("Failed to write element " + localName, e);
        }
    }
}
