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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
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
    void hasImageReader() {
        assertThat(RasterTileStore.hasImageReader("image/png")).isTrue();
        assertThat(RasterTileStore.hasImageReader("image/x-unknown")).isFalse();
    }

    private static byte[] png(int width, int height) throws IOException {
        BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ImageOutputStream out = new MemoryCacheImageOutputStream(bytes)) {
            ImageIO.write(image, "png", out);
        }
        return bytes.toByteArray();
    }
}
