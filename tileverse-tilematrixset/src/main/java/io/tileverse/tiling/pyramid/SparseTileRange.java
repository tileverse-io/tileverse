/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.tiling.pyramid;

import static java.util.Objects.requireNonNull;

import io.tileverse.tiling.common.CornerOfOrigin;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A sparse {@link TileRange} is composed of two or more tile ranges and covers exactly the union of
 * tile indices of its parts, with the same {@link #cornerOfOrigin()} and {@link #zoomLevel()}.
 * <p>
 * The bound-related method may return tile indices that lay outside the actual tile indices:
 * <ul>
 * <li> {@link #minx()}, {@link #miny()}, {@link #maxx()}, {@link #maxy()}
 * <li> {@link #min()}, {@link #max()}
 * <li> {@link #topLeft()}, {@link #topRight()}, {@link #bottomLeft()}, {@link #bottomRight()}
 * <li> {@link #spanX()}, {@link #spanY()}
 * </ul>
 * The navigation-related methods will though only traverse the tile indices covered by its parts, with no duplication
 * even if the parts intersect themselves:
 * <ul>
 * <li> {@link #first()}, {@link #last()}
 * <li> {@link #next(TileIndex)}, {@link #prev(TileIndex)}
 * <li> {@link #all()}
 * </ul>
 * {@link #count()} will return the actual tile count.
 *
 * @implNote internally, the list of parts will be split into non-overlapping tile ranges
 */
record SparseTileRange(List<TileRange> parts) implements TileRange {

    SparseTileRange(List<TileRange> parts) {
        checkPreconditions(parts);
        this.parts = parts;
    }

    private void checkPreconditions(List<TileRange> parts) {
        if (requireNonNull(parts, "parts is null").size() < 2) {
            throw new IllegalArgumentException("SparseTileRange should have at least 2 parts");
        }
        TileRange first = parts.get(0);
        for (int i = 1; i < parts.size(); i++) {
            TileRange part = requireNonNull(parts.get(i), "parts can't be null");
            if (part.zoomLevel() != first.zoomLevel()) {
                throw new IllegalArgumentException("All parts must have the same zoom level");
            }
            if (part.cornerOfOrigin() != first.cornerOfOrigin()) {
                throw new IllegalArgumentException("All parts must have the same cornerOfOrigin");
            }
        }
    }

    private TileRange firstRange() {
        return parts.get(0);
    }

    private TileRange lastRange() {
        return parts.get(parts.size() - 1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isSparse() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int zoomLevel() {
        return firstRange().zoomLevel();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CornerOfOrigin cornerOfOrigin() {
        return firstRange().cornerOfOrigin();
    }

    /**
     * {@inheritDoc}
     * Note for a sparse tile range the min index may not be {@link #contains(TileIndex) contained}
     */
    @Override
    public TileIndex min() {
        return TileIndex.xyz(minx(), miny(), zoomLevel());
    }

    /**
     * {@inheritDoc}
     * Note for a sparse tile range the max index may not be {@link #contains(TileIndex) contained}
     */
    @Override
    public TileIndex max() {
        return TileIndex.xyz(maxx(), maxy(), zoomLevel());
    }

    /**
     * {@inheritDoc}
     * Note for a sparse tile range the top left index may not be {@link #contains(TileIndex) contained}
     */
    @Override
    public TileIndex topLeft() {
        return cornerOfOrigin().upperLeft(this);
    }

    /**
     * {@inheritDoc}
     * Note for a sparse tile range the bottom left index may not be {@link #contains(TileIndex) contained}
     */
    @Override
    public TileIndex bottomLeft() {
        return cornerOfOrigin().lowerLeft(this);
    }

    /**
     * {@inheritDoc}
     * Note for a sparse tile range the top right index may not be {@link #contains(TileIndex) contained}
     */
    @Override
    public TileIndex topRight() {
        return cornerOfOrigin().upperRight(this);
    }

    /**
     * {@inheritDoc}
     * Note for a sparse tile range the bottom right index may not be {@link #contains(TileIndex) contained}
     */
    @Override
    public TileIndex bottomRight() {
        return cornerOfOrigin().lowerRight(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long minx() {
        return parts().stream().mapToLong(TileRange::minx).min().orElseThrow();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long maxx() {
        return parts().stream().mapToLong(TileRange::maxx).max().orElseThrow();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long miny() {
        return parts().stream().mapToLong(TileRange::miny).min().orElseThrow();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long maxy() {
        return parts().stream().mapToLong(TileRange::maxy).max().orElseThrow();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long count() {
        return parts().stream().mapToLong(TileRange::count).sum();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TileIndex first() {
        return firstRange().first();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TileIndex last() {
        return lastRange().last();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<TileIndex> next(TileIndex current) {
        // Find which part contains the current tile
        for (int i = 0; i < parts.size(); i++) {
            TileRange currentPart = parts.get(i);
            if (currentPart.contains(current)) {
                // Try to get next within this part
                Optional<TileIndex> nextInPart = currentPart.next(current);
                if (nextInPart.isPresent()) {
                    return nextInPart;
                }

                // Reached end of current part, move to first tile of next part
                if (i + 1 < parts.size()) {
                    return Optional.of(parts.get(i + 1).first());
                }

                // No more parts
                return Optional.empty();
            }
        }

        // Current tile not found in any part
        return Optional.empty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<TileIndex> prev(TileIndex current) {
        // Find which part contains the current tile
        for (int i = 0; i < parts.size(); i++) {
            TileRange currentPart = parts.get(i);
            if (currentPart.contains(current)) {
                // Try to get previous within this part
                Optional<TileIndex> prevInPart = currentPart.prev(current);
                if (prevInPart.isPresent()) {
                    return prevInPart;
                }

                // Reached start of current part, move to last tile of previous part
                if (i > 0) {
                    return Optional.of(parts.get(i - 1).last());
                }

                // No previous parts
                return Optional.empty();
            }
        }

        // Current tile not found in any part
        return Optional.empty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<TileIndex> all() {
        return parts().stream().flatMap(TileRange::all);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean contains(TileIndex tile) {
        return parts().stream().anyMatch(range -> range.contains(tile));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean contains(TileRange range) {
        if (range instanceof SparseTileRange sparse) {
            return sparse.parts().stream().allMatch(this::contains);
        }
        // continuous range, we'll need to check all corners
        return contains(range.bottomLeft())
                && contains(range.bottomRight())
                && contains(range.topLeft())
                && contains(range.topRight());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TileRange withCornerOfOrigin(CornerOfOrigin targetOrigin) {
        if (this.cornerOfOrigin() == targetOrigin) {
            return this;
        }

        List<TileRange> newOriginParts = parts().stream()
                .map(part -> part.withCornerOfOrigin(targetOrigin))
                .toList();
        return new SparseTileRange(newOriginParts);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(TileRange o) {
        return COMPARATOR.compare(this, o);
    }

    @Override
    public String toString() {
        return "Sparse[%s]".formatted(parts.stream().map(TileRange::toString).collect(Collectors.joining(", ")));
    }
}
