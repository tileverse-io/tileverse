/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.versatiles;

/**
 * The compression of the metadata and of the tile bodies in a Versatiles container, as recorded at byte 15 of the
 * header.
 *
 * <p>The block index and the tile indexes are always compressed with Brotli, regardless of this setting.
 */
public enum Compression {
    NONE((byte) 0x00),
    GZIP((byte) 0x01),
    BROTLI((byte) 0x02),
    ZSTD((byte) 0x03);

    private final byte code;

    Compression(byte code) {
        this.code = code;
    }

    /** Returns the code of this compression in the header. */
    public byte code() {
        return code;
    }

    /**
     * Returns the compression recorded with {@code code} in a header.
     *
     * @throws IllegalArgumentException if the specification defines no compression with this code
     */
    public static Compression fromCode(byte code) {
        for (Compression compression : values()) {
            if (compression.code == code) {
                return compression;
            }
        }
        throw new IllegalArgumentException("Unknown compression code: 0x%02X".formatted(code & 0xFF));
    }
}
