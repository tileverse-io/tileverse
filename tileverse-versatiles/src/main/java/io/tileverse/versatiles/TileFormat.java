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
package io.tileverse.versatiles;

/**
 * The format of the tile bodies in a Versatiles container, as recorded at byte 14 of the header.
 *
 * <p>The codes come from the Versatiles container 2.0 specification.
 */
public enum TileFormat {
    BIN((byte) 0x00, "application/octet-stream", false),
    PNG((byte) 0x10, "image/png", false),
    JPEG((byte) 0x11, "image/jpeg", false),
    WEBP((byte) 0x12, "image/webp", false),
    AVIF((byte) 0x13, "image/avif", false),
    SVG((byte) 0x14, "image/svg+xml", false),
    PBF((byte) 0x20, "application/x-protobuf", true),
    GEOJSON((byte) 0x21, "application/geo+json", true),
    TOPOJSON((byte) 0x22, "application/topo+json", true),
    JSON((byte) 0x23, "application/json", false);

    private final byte code;
    private final String mimeType;
    private final boolean vector;

    TileFormat(byte code, String mimeType, boolean vector) {
        this.code = code;
        this.mimeType = mimeType;
        this.vector = vector;
    }

    /** Returns the code of this format in the header. */
    public byte code() {
        return code;
    }

    /** Returns the MIME type of the tile bodies, for example {@code image/png}. */
    public String mimeType() {
        return mimeType;
    }

    /** Whether the tiles hold vector data: {@link #PBF}, {@link #GEOJSON}, or {@link #TOPOJSON}. */
    public boolean isVector() {
        return vector;
    }

    /**
     * Returns the format recorded with {@code code} in a header.
     *
     * @throws IllegalArgumentException if the specification defines no format with this code
     */
    public static TileFormat fromCode(byte code) {
        for (TileFormat format : values()) {
            if (format.code == code) {
                return format;
            }
        }
        throw new IllegalArgumentException("Unknown tile format code: 0x%02X".formatted(code & 0xFF));
    }
}
