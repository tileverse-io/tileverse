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

import static java.util.Objects.requireNonNull;

import java.net.URI;
import java.util.Optional;

/**
 * Reference from a dataset to a {@link TileMatrixSet} defined elsewhere, optionally narrowed by
 * {@link TileMatrixSetLimits}, as defined by OGC 17-083r2 clause 7.4.
 *
 * <p>When {@code tileMatrixSetLimits} is absent, the dataset implicitly covers the full matrix extent of every tile
 * matrix in the referenced set.
 *
 * @param tileMatrixSet URI identifying the referenced tile matrix set
 * @param tileMatrixSetLimits optional limits narrowing the referenced set
 * @since 1.4
 * @see <a href="https://docs.ogc.org/is/17-083r2/17-083r2.html#28">OGC 17-083r2 clause 7.4</a>
 */
public record TileMatrixSetLink(URI tileMatrixSet, Optional<TileMatrixSetLimits> tileMatrixSetLimits) {

    public TileMatrixSetLink {
        requireNonNull(tileMatrixSet, "tileMatrixSet URI cannot be null");
        requireNonNull(tileMatrixSetLimits, "tileMatrixSetLimits Optional cannot be null");
    }

    /** Creates a link without limits (the dataset covers the full referenced matrix set). */
    public static TileMatrixSetLink of(URI tileMatrixSet) {
        return new TileMatrixSetLink(tileMatrixSet, Optional.empty());
    }

    /** Creates a link with the given limits. */
    public static TileMatrixSetLink of(URI tileMatrixSet, TileMatrixSetLimits limits) {
        return new TileMatrixSetLink(tileMatrixSet, Optional.of(limits));
    }
}
