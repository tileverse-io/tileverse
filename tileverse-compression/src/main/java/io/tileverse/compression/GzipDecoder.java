/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.compression;

import io.tileverse.io.ByteBufferInputStream;
import com.fulcrumgenomics.jlibdeflate.DecompressionResult;
import com.fulcrumgenomics.jlibdeflate.LibdeflateDecompressor;
import com.fulcrumgenomics.jlibdeflate.LibdeflateException;
import io.tileverse.io.ByteBufferPool;
import io.tileverse.io.ByteBufferPool.PooledByteBuffer;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.zip.GZIPInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decodes gzip blobs with the native libdeflate library through jlibdeflate. libdeflate decodes a whole member in one
 * call, into a pooled buffer of the size recorded in the member trailer. The other blobs go to the JDK decoder: several
 * members, a size too large to trust, or corrupt data. When the native library cannot load on this platform, decoding
 * uses the JDK decoder only, and one warning is logged.
 *
 * <p>Instances are thread-safe. A libdeflate decompressor must not serve two threads at once: each decode borrows one
 * from a pool and returns it afterwards.
 */
final class GzipDecoder {

    /** The largest decoded size allocated at once for the native decoder; larger blobs stream through the JDK. */
    static final int MAX_NATIVE_OUTPUT_BYTES = 64 * 1024 * 1024;

    // DEFLATE cannot expand its input more than 1032 times: a larger trailer size is a lie
    private static final int MAX_COMPRESSION_RATIO = 1032;

    // A 10-byte header and an 8-byte trailer holding the CRC32 and the size of the decoded bytes
    private static final int MIN_MEMBER_LENGTH = 18;

    private static final int TRAILER_SIZE_LENGTH = 4;

    private static final Logger LOGGER = LoggerFactory.getLogger(GzipDecoder.class);

    private static final GzipDecoder NATIVE = new GzipDecoder(true);

    private static final GzipDecoder PURE_JAVA = new GzipDecoder(false);

    private final boolean nativeLibrary;

    // Never closed: the decompressors live as long as the decoder, and the pool grows to the peak concurrency
    private final Queue<LibdeflateDecompressor> decompressors = new ConcurrentLinkedQueue<>();

    private GzipDecoder(boolean nativeLibrary) {
        this.nativeLibrary = nativeLibrary;
    }

    /** Returns the native decoder if jlibdeflate loaded on this platform, or the pure-Java decoder otherwise. */
    static GzipDecoder nativeOrPureJava() {
        return NativeAvailability.DECODER;
    }

    /** Returns the pure-Java decoder, regardless of the availability of the native library. */
    static GzipDecoder pureJava() {
        return PURE_JAVA;
    }

    boolean isNative() {
        return nativeLibrary;
    }

    /**
     * Returns a stream over the decoded bytes of the gzip blob held in {@code compressed}, from its position to its
     * limit. A blob of several members decodes to their concatenation. A corrupt or truncated blob fails with an
     * {@link IOException}, from this method or while the stream is read. The caller must not change the buffer before
     * the stream is closed, and must close the stream to return its pooled buffer.
     */
    InputStream decode(ByteBuffer compressed) throws IOException {
        if (nativeLibrary) {
            Optional<InputStream> decoded = decodeNatively(compressed);
            if (decoded.isPresent()) {
                return decoded.get();
            }
        }
        return decodeWithJdk(compressed);
    }

    /**
     * Decodes the blob as one member, into a pooled buffer of the same kind as {@code compressed}. jlibdeflate decodes
     * without a copy when both buffers are direct or both expose their array; otherwise, as for a read-only input, it
     * copies the input and the output. On success, the position of {@code compressed} moves to its limit. The result is
     * empty when the blob is not such a member, and {@code compressed} is left unchanged.
     */
    Optional<InputStream> decodeNatively(ByteBuffer compressed) {
        int length = compressed.remaining();
        if (length < MIN_MEMBER_LENGTH) {
            return Optional.empty();
        }
        int size = trailerSize(compressed);
        if (!plausibleSize(size, length)) {
            return Optional.empty();
        }
        PooledByteBuffer decoded = borrowLike(compressed, size);
        boolean oneMember = false;
        try {
            oneMember = decodeSingleMember(compressed, decoded.buffer());
        } finally {
            if (!oneMember) {
                decoded.close();
            }
        }
        if (!oneMember) {
            return Optional.empty();
        }
        compressed.position(compressed.limit());
        decoded.buffer().flip();
        return Optional.of(ByteBufferInputStream.releasing(decoded));
    }

    /** Reads the size of the decoded bytes, stored little-endian in the last four bytes of the last member. */
    private static int trailerSize(ByteBuffer compressed) {
        ByteBuffer littleEndian = compressed.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        return littleEndian.getInt(compressed.limit() - TRAILER_SIZE_LENGTH);
    }

    private static boolean plausibleSize(int size, int length) {
        return size >= 0 && size <= MAX_NATIVE_OUTPUT_BYTES && size <= (long) length * MAX_COMPRESSION_RATIO;
    }

    private static PooledByteBuffer borrowLike(ByteBuffer compressed, int size) {
        if (compressed.isDirect()) {
            return ByteBufferPool.directBuffer(size);
        }
        return ByteBufferPool.heapBuffer(size);
    }

    /**
     * Decodes {@code compressed} into {@code decoded}, a buffer of the exact size read from the trailer. Returns false
     * when the blob is not one member of that size: it may hold several members, or corrupt data.
     */
    private boolean decodeSingleMember(ByteBuffer compressed, ByteBuffer decoded) {
        int length = compressed.remaining();
        int size = decoded.remaining();
        LibdeflateDecompressor decompressor = borrowDecompressor();
        try {
            DecompressionResult result = decompressor.gzipDecompressEx(compressed.duplicate(), decoded);
            return result.inputBytesConsumed() == length && result.outputBytesProduced() == size;
        } catch (LibdeflateException notOneMember) {
            // Valid blobs of several members fail here too, and the JDK decoder tells them apart from corrupt ones
            return false;
        } finally {
            decompressors.offer(decompressor);
        }
    }

    private LibdeflateDecompressor borrowDecompressor() {
        LibdeflateDecompressor decompressor = decompressors.poll();
        if (decompressor == null) {
            decompressor = new LibdeflateDecompressor();
        }
        return decompressor;
    }

    private static InputStream decodeWithJdk(ByteBuffer compressed) throws IOException {
        GZIPInputStream decoded = new GZIPInputStream(ByteBufferInputStream.over(compressed));
        return DecodedStreams.buffered(decoded);
    }

    /**
     * Checks the native library on first use, in a holder class to run the check and log the warning once for the life
     * of the JVM. jlibdeflate loads its library when the first decompressor is created, and has no other check.
     */
    private static final class NativeAvailability {

        static final GzipDecoder DECODER = select();

        private static GzipDecoder select() {
            try {
                new LibdeflateDecompressor().close();
                return NATIVE;
            } catch (LinkageError e) {
                LOGGER.warn(
                        "libdeflate native library failed to load, decoding gzip with the slower JDK decoder: {}",
                        describe(e));
                return PURE_JAVA;
            }
        }

        // An ExceptionInInitializerError has a null message: its cause holds the reason
        private static String describe(LinkageError e) {
            Throwable reason = e.getCause() == null ? e : e.getCause();
            return reason.toString();
        }
    }
}
