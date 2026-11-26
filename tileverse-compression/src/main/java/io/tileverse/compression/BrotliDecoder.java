/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.compression;

import com.aayushatharva.brotli4j.Brotli4jLoader;
import com.aayushatharva.brotli4j.decoder.BrotliInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decodes Brotli streams with the native brotli4j library. When the native library cannot load on this platform,
 * decoding falls back to the slower pure-Java decoder of {@code org.brotli:dec}, and one warning is logged.
 */
final class BrotliDecoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(BrotliDecoder.class);

    private static final BrotliDecoder NATIVE = new BrotliDecoder(true);

    private static final BrotliDecoder PURE_JAVA = new BrotliDecoder(false);

    private final boolean nativeLibrary;

    private BrotliDecoder(boolean nativeLibrary) {
        this.nativeLibrary = nativeLibrary;
    }

    /** Returns the native decoder if brotli4j loaded on this platform, or the pure-Java decoder otherwise. */
    static BrotliDecoder nativeOrPureJava() {
        return NativeAvailability.DECODER;
    }

    /** Returns the pure-Java decoder, regardless of the availability of the native library. */
    static BrotliDecoder pureJava() {
        return PURE_JAVA;
    }

    boolean isNative() {
        return nativeLibrary;
    }

    /**
     * Returns a stream over the decoded bytes of {@code compressed}. A corrupt or truncated stream fails with an
     * {@link IOException} while it is read. Closing the returned stream frees the native decoder state and closes
     * {@code compressed}.
     */
    InputStream decode(InputStream compressed) throws IOException {
        if (nativeLibrary) {
            return nativeStream(compressed);
        }
        return pureJavaStream(compressed);
    }

    private static InputStream nativeStream(InputStream compressed) throws IOException {
        return new BrotliInputStream(compressed);
    }

    // Fully qualified because its simple name is the same as the imported native stream
    private static InputStream pureJavaStream(InputStream compressed) throws IOException {
        return new org.brotli.dec.BrotliInputStream(compressed);
    }

    /**
     * Checks the native library on first use, in a holder class to run the check and log the warning once for the life
     * of the JVM.
     */
    private static final class NativeAvailability {

        static final BrotliDecoder DECODER = select();

        private static BrotliDecoder select() {
            if (Brotli4jLoader.isAvailable()) {
                return NATIVE;
            }
            String cause = String.valueOf(Brotli4jLoader.getUnavailabilityCause());
            LOGGER.warn("Brotli native library failed to load, decoding with the slower pure-Java decoder: {}", cause);
            return PURE_JAVA;
        }
    }
}
