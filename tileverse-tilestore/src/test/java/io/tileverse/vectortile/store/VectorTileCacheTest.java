/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.tileverse.tiling.pyramid.TileIndex;
import io.tileverse.vectortile.model.VectorTile;
import io.tileverse.vectortile.mvt.VectorTileBuilder;
import io.tileverse.vectortile.mvt.VectorTileCodec;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

class VectorTileCacheTest {

    private static final TileIndex KEY = TileIndex.xyz(0, 0, 0);

    /** The weights of the key and of the {@code Optional} holding the tile. */
    private static final int KEY_AND_OPTIONAL = 36 + 16;

    @Test
    void weighsDecodedTile() throws IOException {
        VectorTile tile = decodedTile();
        int heapSize = tile.heapSize().getAsInt();

        int weight = VectorTileCache.weigh(KEY, Optional.of(tile));

        assertThat(weight).isEqualTo(KEY_AND_OPTIONAL + heapSize);
    }

    @Test
    void weighsTileWithoutEstimate() {
        VectorTile tile = mock(VectorTile.class);
        when(tile.serializedSize()).thenReturn(OptionalInt.of(1000));
        // An implementation without an estimate keeps the default method of the interface
        doCallRealMethod().when(tile).heapSize();

        int weight = VectorTileCache.weigh(KEY, Optional.of(tile));

        assertThat(weight).isEqualTo(KEY_AND_OPTIONAL + 5 * 1000);
    }

    // Caffeine takes the weight as an int
    @Test
    void clampsWeightOfHugeTile() {
        VectorTile tile = mock(VectorTile.class);
        when(tile.heapSize()).thenReturn(OptionalInt.of(Integer.MAX_VALUE));

        int weight = VectorTileCache.weigh(KEY, Optional.of(tile));

        assertThat(weight).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void clampsWeightWithoutEstimate() {
        VectorTile tile = mock(VectorTile.class);
        when(tile.serializedSize()).thenReturn(OptionalInt.of(500_000_000));
        doCallRealMethod().when(tile).heapSize();

        int weight = VectorTileCache.weigh(KEY, Optional.of(tile));

        assertThat(weight).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void weighsMissingTile() {
        int weight = VectorTileCache.weigh(KEY, Optional.empty());

        assertThat(weight).isEqualTo(KEY_AND_OPTIONAL);
    }

    private static VectorTile decodedTile() throws IOException {
        Point point = new GeometryFactory().createPoint(new Coordinate(10, 20));
        VectorTileBuilder builder = new VectorTileBuilder();
        builder.layer().name("points").feature(Map.of("name", "cafe"), point).build();
        VectorTileCodec codec = new VectorTileCodec();
        byte[] encoded = codec.encode(builder.build());
        return codec.decode(encoded);
    }
}
