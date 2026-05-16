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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.util.List;
import org.junit.jupiter.api.Test;

class TileMatrixLimitsTest {

    @Test
    void tileMatrixLimitsAccepts_validRange() {
        TileMatrixLimits limits = new TileMatrixLimits("5", 0, 31, 0, 31);
        assertThat(limits.tileMatrix()).isEqualTo("5");
        assertThat(limits.rowSpan()).isEqualTo(32);
        assertThat(limits.colSpan()).isEqualTo(32);
        assertThat(limits.tileCount()).isEqualTo(1024);
    }

    @Test
    void tileMatrixLimitsRejects_negativeMinRow() {
        assertThatThrownBy(() -> new TileMatrixLimits("5", -1, 10, 0, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("minTileRow");
    }

    @Test
    void tileMatrixLimitsRejects_negativeMinCol() {
        assertThatThrownBy(() -> new TileMatrixLimits("5", 0, 10, -1, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("minTileCol");
    }

    @Test
    void tileMatrixLimitsRejects_invertedRowRange() {
        assertThatThrownBy(() -> new TileMatrixLimits("5", 10, 5, 0, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("minTileRow");
    }

    @Test
    void tileMatrixLimitsRejects_invertedColRange() {
        assertThatThrownBy(() -> new TileMatrixLimits("5", 0, 10, 20, 5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("minTileCol");
    }

    @Test
    void tileMatrixLimitsRejects_nullIdentifier() {
        assertThatThrownBy(() -> new TileMatrixLimits(null, 0, 10, 0, 10)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void tileMatrixSetLimitsLookup_returnsLimitsForKnownMatrix() {
        TileMatrixLimits l0 = new TileMatrixLimits("0", 0, 0, 0, 0);
        TileMatrixLimits l1 = new TileMatrixLimits("1", 0, 1, 0, 1);
        TileMatrixSetLimits set = new TileMatrixSetLimits(List.of(l0, l1));

        assertThat(set.limitsFor("0")).contains(l0);
        assertThat(set.limitsFor("1")).contains(l1);
        assertThat(set.limitsFor("2")).isEmpty();
    }

    @Test
    void tileMatrixSetLimitsRejects_duplicateIdentifiers() {
        List<TileMatrixLimits> dupIds =
                List.of(new TileMatrixLimits("0", 0, 0, 0, 0), new TileMatrixLimits("0", 0, 1, 0, 1));
        assertThatThrownBy(() -> new TileMatrixSetLimits(dupIds))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate");
    }

    @Test
    void tileMatrixSetLimitsEmpty_isReusable() {
        assertThat(TileMatrixSetLimits.empty().tileMatrixLimits()).isEmpty();
        assertThat(TileMatrixSetLimits.empty()).isSameAs(TileMatrixSetLimits.empty());
    }

    @Test
    void tileMatrixSetLinkWithoutLimits_isAllowed() {
        URI uri = URI.create("http://example.com/tms/WebMercatorQuad");
        TileMatrixSetLink link = TileMatrixSetLink.of(uri);
        assertThat(link.tileMatrixSet()).isEqualTo(uri);
        assertThat(link.tileMatrixSetLimits()).isEmpty();
    }

    @Test
    void tileMatrixSetLinkWithLimits_carriesLimits() {
        URI uri = URI.create("http://example.com/tms/WebMercatorQuad");
        TileMatrixSetLimits limits = new TileMatrixSetLimits(List.of(new TileMatrixLimits("0", 0, 0, 0, 0)));
        TileMatrixSetLink link = TileMatrixSetLink.of(uri, limits);
        assertThat(link.tileMatrixSetLimits()).contains(limits);
    }

    @Test
    void variableMatrixWidthRejects_coalesceLessThan2() {
        assertThatThrownBy(() -> new VariableMatrixWidth(1, 0, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("coalesce");
    }

    @Test
    void variableMatrixWidthAccepts_validValues() {
        VariableMatrixWidth vmw = new VariableMatrixWidth(4, 0, 3);
        assertThat(vmw.coalesce()).isEqualTo(4);
        assertThat(vmw.minTileRow()).isZero();
        assertThat(vmw.maxTileRow()).isEqualTo(3);
    }
}
