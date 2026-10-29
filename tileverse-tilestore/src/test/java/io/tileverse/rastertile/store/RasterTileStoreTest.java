/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.rastertile.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.awt.Dimension;
import java.awt.image.BufferedImage;
import java.awt.image.RenderedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import javax.imageio.ImageIO;
import javax.imageio.stream.ImageOutputStream;
import javax.imageio.stream.MemoryCacheImageOutputStream;
import org.junit.jupiter.api.Test;

class RasterTileStoreTest {

    @Test
    void readsImageSize() throws IOException {
        InputStream image = new ByteArrayInputStream(png(512, 256));

        Dimension size = RasterTileStore.readImageSize(image, "image/png");

        assertThat(size).isEqualTo(new Dimension(512, 256));
    }

    @Test
    void sizeOfCorruptImageFails() {
        InputStream image = new ByteArrayInputStream("not an image".getBytes(StandardCharsets.UTF_8));

        assertThatThrownBy(() -> RasterTileStore.readImageSize(image, "image/png"))
                .isInstanceOf(IOException.class);
    }

    @Test
    void sizeWithoutDecoderFails() throws IOException {
        InputStream image = new ByteArrayInputStream(png(256, 256));

        assertThatThrownBy(() -> RasterTileStore.readImageSize(image, "image/x-unknown"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("image/x-unknown");
    }

    @Test
    void readsImage() throws IOException {
        InputStream image = new ByteArrayInputStream(png(512, 256));

        RenderedImage decoded = RasterTileStore.readImage(image, "image/png");

        assertThat(decoded.getWidth()).isEqualTo(512);
        assertThat(decoded.getHeight()).isEqualTo(256);
    }

    // a decompressing stream does not report its length, and this image outgrows the first buffer
    @Test
    void readsStreamOfUnknownLength() throws IOException {
        BufferedImage source = noise(256, 256);
        byte[] encoded = encode(source);
        assertThat(encoded).hasSizeGreaterThan(16 * 1024);

        RenderedImage decoded = RasterTileStore.readImage(withoutLength(encoded), "image/png");

        assertThat(pixels((BufferedImage) decoded)).isEqualTo(pixels(source));
    }

    @Test
    void corruptImageFails() {
        InputStream image = new ByteArrayInputStream("not an image".getBytes(StandardCharsets.UTF_8));

        assertThatThrownBy(() -> RasterTileStore.readImage(image, "image/png")).isInstanceOf(IOException.class);
    }

    @Test
    void hasImageReader() {
        assertThat(RasterTileStore.hasImageReader("image/png")).isTrue();
        assertThat(RasterTileStore.hasImageReader("image/x-unknown")).isFalse();
    }

    private static byte[] png(int width, int height) throws IOException {
        return encode(new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB));
    }

    /** An image of random pixels: its PNG encoding is about as large as its pixels. */
    private static BufferedImage noise(int width, int height) {
        BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        Random random = new Random(42);
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                image.setRGB(x, y, random.nextInt(0x1000000));
            }
        }
        return image;
    }

    private static byte[] encode(BufferedImage image) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ImageOutputStream out = new MemoryCacheImageOutputStream(bytes)) {
            ImageIO.write(image, "png", out);
        }
        return bytes.toByteArray();
    }

    private static int[] pixels(BufferedImage image) {
        int width = image.getWidth();
        return image.getRGB(0, 0, width, image.getHeight(), null, 0, width);
    }

    private static InputStream withoutLength(byte[] bytes) {
        return new FilterInputStream(new ByteArrayInputStream(bytes)) {
            @Override
            public int available() {
                return 0;
            }
        };
    }
}
