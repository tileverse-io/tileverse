/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.tiling.matrix;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class StandardTileMatrixSetTest {

    @Test
    void resolutionsReturnsACopy() {
        StandardTileMatrixSet tms = webMercatorQuadCopy();
        double levelZero = tms.resolution(0);

        tms.resolutions()[0] = 1.0;

        assertThat(tms.resolution(0)).isEqualTo(levelZero);
    }

    @Test
    void constructorCopiesResolutions() {
        StandardTileMatrixSet source = webMercatorQuadCopy();
        double levelZero = source.resolution(0);
        double[] resolutions = source.resolutions();
        StandardTileMatrixSet tms = new StandardTileMatrixSet(
                source.identifier(),
                source.tilePyramid(),
                source.crsId(),
                source.supportedCRS(),
                source.tileWidth(),
                source.tileHeight(),
                source.extent(),
                resolutions,
                source.title(),
                source.abstractDescription(),
                source.keywords(),
                source.wellKnownScaleSet());

        resolutions[0] = 1.0;

        assertThat(tms.resolution(0)).isEqualTo(levelZero);
    }

    // A test changing DefaultTileMatrixSets.WEB_MERCATOR_QUAD itself would break the tests running beside it.
    private static StandardTileMatrixSet webMercatorQuadCopy() {
        TileMatrixSet copy = DefaultTileMatrixSets.WEB_MERCATOR_QUAD.toBuilder().build();
        return (StandardTileMatrixSet) copy;
    }
}
