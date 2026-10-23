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
