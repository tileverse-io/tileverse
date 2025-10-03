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

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.tileverse.tiling.common.CornerOfOrigin;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Abstract base class representing a complete multi-level tile pyramid.
 * Contains tile range operations and provides pyramid-wide functionality.
 *
 * @since 1.0
 */
public abstract class TilePyramid {

    private final CornerOfOrigin cornerOfOrigin;

    /**
     * Default constructor for backward compatibility.
     * Uses LOWER_LEFT axis origin.
     */
    protected TilePyramid() {
        this(CornerOfOrigin.BOTTOM_LEFT);
    }

    /**
     * Constructor with explicit axis origin.
     *
     * @param cornerOfOrigin the axis origin for this pyramid
     */
    protected TilePyramid(CornerOfOrigin cornerOfOrigin) {
        this.cornerOfOrigin = cornerOfOrigin != null ? cornerOfOrigin : CornerOfOrigin.BOTTOM_LEFT;
    }

    /**
     * Returns the immutable list of tile ranges.
     *
     * @return the list of tile ranges, sorted by zoom level
     */
    public abstract List<TileRange> levels();

    /**
     * Returns the axis origin for this pyramid.
     * All levels in the pyramid use the same axis origin.
     *
     * @return the axis origin
     */
    public CornerOfOrigin cornerOfOrigin() {
        return cornerOfOrigin;
    }

    /**
     * Factory method to create an empty TilePyramid.
     *
     * @return an empty TilePyramid instance with LOWER_LEFT axis origin
     */
    public static TilePyramid empty() {
        return new TilePyramidImpl(List.of(), CornerOfOrigin.BOTTOM_LEFT);
    }

    /**
     * Factory method to create an empty TilePyramid with axis origin.
     *
     * @param cornerOfOrigin the axis origin
     * @return an empty TilePyramid instance
     */
    public static TilePyramid empty(CornerOfOrigin cornerOfOrigin) {
        return new TilePyramidImpl(List.of(), cornerOfOrigin);
    }

    /**
     * Factory method to create a TilePyramid from a single TileRange.
     *
     * @param range the tile range to create a pyramid from
     * @return a new TilePyramid containing the specified range with LOWER_LEFT axis origin
     */
    public static TilePyramid of(TileRange range) {
        return new TilePyramidImpl(List.of(range), CornerOfOrigin.BOTTOM_LEFT);
    }

    /**
     * Factory method to create a TilePyramid from a single TileRange with axis origin.
     *
     * @param range the tile range to create a pyramid from
     * @param cornerOfOrigin the axis origin
     * @return a new TilePyramid containing the specified range
     */
    public static TilePyramid of(TileRange range, CornerOfOrigin cornerOfOrigin) {
        return new TilePyramidImpl(List.of(range), cornerOfOrigin);
    }

    /**
     * Factory method to create a TilePyramid from a collection of ranges.
     *
     * @param ranges the tile ranges to include in the pyramid
     * @return a new TilePyramid containing the specified ranges with LOWER_LEFT axis origin
     */
    public static TilePyramid of(Collection<TileRange> ranges) {
        return new TilePyramidImpl(ranges, CornerOfOrigin.BOTTOM_LEFT);
    }

    /**
     * Factory method to create a TilePyramid from a collection of ranges with axis origin.
     *
     * @param ranges the tile ranges to include in the pyramid
     * @param cornerOfOrigin the axis origin
     * @return a new TilePyramid containing the specified ranges
     */
    public static TilePyramid of(Collection<TileRange> ranges, CornerOfOrigin cornerOfOrigin) {
        return new TilePyramidImpl(ranges, cornerOfOrigin);
    }

    /**
     * Returns true if the pyramid contains no tiles.
     *
     * @return true if the pyramid is empty
     */
    @JsonIgnore
    public boolean isEmpty() {
        return count() == 0;
    }

    /**
     * Returns the total number of tiles across all zoom levels in the pyramid.
     *
     * @return the total number of tiles in the pyramid
     * @throws ArithmeticException if the total count would overflow Long.MAX_VALUE
     */
    public long count() {
        return levels().stream().mapToLong(TileRange::count).reduce(0L, Math::addExact);
    }

    /**
     * Returns the minimum zoom level in the pyramid.
     *
     * @return the minimum zoom level
     * @throws java.util.NoSuchElementException if the pyramid is empty
     */
    public int minZoomLevel() {
        List<TileRange> levelsList = levels();
        if (levelsList.isEmpty()) {
            throw new java.util.NoSuchElementException("Empty pyramid");
        }
        return levelsList.get(0).zoomLevel();
    }

    /**
     * Returns the maximum zoom level in the pyramid.
     *
     * @return the maximum zoom level
     * @throws java.util.NoSuchElementException if the pyramid is empty
     */
    public int maxZoomLevel() {
        List<TileRange> levelsList = levels();
        if (levelsList.isEmpty()) {
            throw new java.util.NoSuchElementException("Empty pyramid");
        }
        return levelsList.get(levelsList.size() - 1).zoomLevel();
    }

    /**
     * Returns the tile range for a specific zoom level, if present.
     *
     * @param zoomLevel the zoom level to query
     * @return an Optional containing the TileRange for the specified zoom level, or empty if not present
     */
    public Optional<TileRange> level(int zoomLevel) {
        return levels().stream().filter(r -> r.zoomLevel() == zoomLevel).findFirst();
    }

    /**
     * Returns the tile range for a specific zoom level.
     * Throws an exception if the zoom level is not present.
     *
     * @param zoomLevel the zoom level to query
     * @return the TileRange for the specified zoom level
     * @throws IllegalArgumentException if the zoom level is not present in this pyramid
     */
    public TileRange tileRange(int zoomLevel) {
        return level(zoomLevel)
                .orElseThrow(() -> new IllegalArgumentException("Zoom level " + zoomLevel + " not found in pyramid"));
    }

    /**
     * Returns true if the pyramid contains the specified zoom level.
     *
     * @param zoomLevel the zoom level to check
     * @return true if the pyramid contains the zoom level
     */
    public boolean hasZoom(int zoomLevel) {
        return level(zoomLevel).isPresent();
    }

    /**
     * Returns true if the pyramid contains the specified tile index.
     * The tile must be at a valid zoom level and within the bounds of that level's range.
     *
     * @param tile the tile index to check
     * @return true if the pyramid contains the tile
     */
    public boolean contains(TileIndex tile) {
        return level(tile.z()).map(range -> range.contains(tile)).orElse(false);
    }

    /**
     * Returns true if the pyramid contains the specified tile range.
     * The range must be at a valid zoom level and be fully contained within that level's bounds.
     *
     * @param range the tile range to check
     * @return true if the pyramid contains the range
     */
    public boolean contains(TileRange range) {
        Optional<TileRange> level = level(range.zoomLevel());
        if (level.isEmpty()) {
            return false;
        }
        TileRange pyramidRange = level.get();
        return range.minx() >= pyramidRange.minx()
                && range.miny() >= pyramidRange.miny()
                && range.maxx() <= pyramidRange.maxx()
                && range.maxy() <= pyramidRange.maxy();
    }

    /**
     * Creates a subset of the pyramid starting from the specified minimum zoom level.
     *
     * @param minZLevel the minimum zoom level (inclusive) for the subset
     * @return a new TilePyramid containing only the specified zoom levels
     */
    public TilePyramid fromLevel(int minZLevel) {
        return subset(minZLevel, maxZoomLevel());
    }

    /**
     * Creates a subset of the pyramid up to the specified maximum zoom level.
     *
     * @param maxZLevel the maximum zoom level (inclusive) for the subset
     * @return a new TilePyramid containing only the specified zoom levels
     */
    public TilePyramid toLevel(int maxZLevel) {
        return subset(minZoomLevel(), maxZLevel);
    }

    /**
     * Creates a subset view of the pyramid within the specified zoom level range.
     *
     * @param minZLevel the minimum zoom level (inclusive)
     * @param maxZLevel the maximum zoom level (inclusive)
     * @return a TilePyramid view containing only the specified zoom level range
     * @throws IllegalArgumentException if {@code minZLevel < 0} or {@code minZLevel > maxZLevel}
     */
    public TilePyramid subset(int minZLevel, int maxZLevel) {
        if (minZLevel < 0 || minZLevel > maxZLevel)
            throw new IllegalArgumentException("minZLevel must be >= 0 and <= maxZLevel");

        return new SubsetView(this, minZLevel, maxZLevel);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof TilePyramid other && (other == this || levels().equals(other.levels()));
    }

    @Override
    public int hashCode() {
        return levels().hashCode();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{levels=" + levels() + "}";
    }

    /**
     * Returns a new builder for constructing TilePyramid instances.
     *
     * @return a new TilePyramid.Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder class for constructing TilePyramid instances.
     */
    public static class Builder {
        private final List<TileRange> ranges = new ArrayList<>();
        private CornerOfOrigin cornerOfOrigin = CornerOfOrigin.BOTTOM_LEFT;

        /**
         * Adds a tile range to the pyramid.
         *
         * @param range the tile range to add
         * @return this builder
         */
        public Builder level(TileRange range) {
            if (range != null) {
                ranges.add(range);
            }
            return this;
        }

        /**
         * Adds multiple tile ranges to the pyramid.
         *
         * @param ranges the tile ranges to add
         * @return this builder
         */
        public Builder levels(Collection<TileRange> ranges) {
            if (ranges != null) {
                this.ranges.addAll(ranges);
            }
            return this;
        }

        /**
         * Adds multiple tile ranges to the pyramid.
         *
         * @param ranges the tile ranges to add
         * @return this builder
         */
        public Builder levels(TileRange... ranges) {
            if (ranges != null) {
                this.ranges.addAll(List.of(ranges));
            }
            return this;
        }

        /**
         * Sets the axis origin for the pyramid.
         * All tile ranges will be converted to this axis origin if needed.
         *
         * @param cornerOfOrigin the axis origin
         * @return this builder
         */
        public Builder cornerOfOrigin(CornerOfOrigin cornerOfOrigin) {
            this.cornerOfOrigin = cornerOfOrigin != null ? cornerOfOrigin : CornerOfOrigin.BOTTOM_LEFT;
            return this;
        }

        /**
         * Builds the TilePyramid instance.
         * All tile ranges are converted to the pyramid's axis origin if needed.
         *
         * @return a new TilePyramid
         * @throws ArithmeticException if the total tile count would overflow Long.MAX_VALUE
         */
        public TilePyramid build() {
            // Convert all ranges to the pyramid's axis origin
            List<TileRange> convertedRanges = ranges.stream()
                    .map(range -> range.withCornerOfOrigin(cornerOfOrigin))
                    .toList();

            TilePyramid pyramid = new TilePyramidImpl(convertedRanges, cornerOfOrigin);
            // Trigger overflow check by calling count()
            pyramid.count();
            return pyramid;
        }
    }

    /**
     * Standard implementation of TilePyramid that stores tile ranges directly.
     */
    private static class TilePyramidImpl extends TilePyramid {
        private final List<TileRange> levels;

        public TilePyramidImpl(Collection<TileRange> ranges, CornerOfOrigin cornerOfOrigin) {
            super(cornerOfOrigin);
            if (ranges == null) {
                throw new IllegalArgumentException("ranges cannot be null");
            }
            // Create immutable, sorted, distinct list
            this.levels = ranges.stream().sorted().distinct().toList();
        }

        @Override
        public List<TileRange> levels() {
            return levels;
        }
    }

    /**
     * A view of the pyramid that filters ranges to a specific zoom level range.
     * Uses efficient subList() to avoid copying or lazy initialization.
     */
    private static class SubsetView extends TilePyramid {
        private final TilePyramid source;
        private final int startIndex;
        private final int endIndex;
        private final int minZLevel;
        private final int maxZLevel;

        public SubsetView(TilePyramid source, int minZLevel, int maxZLevel) {
            super(source.cornerOfOrigin());
            this.source = source;
            this.minZLevel = minZLevel;
            this.maxZLevel = maxZLevel;

            List<TileRange> sourceLevels = source.levels();

            // Find start index (first level >= minZLevel)
            int start = 0;
            while (start < sourceLevels.size() && sourceLevels.get(start).zoomLevel() < minZLevel) {
                start++;
            }

            // Find end index (last level <= maxZLevel)
            int end = start;
            while (end < sourceLevels.size() && sourceLevels.get(end).zoomLevel() <= maxZLevel) {
                end++;
            }

            this.startIndex = start;
            this.endIndex = end;
        }

        @Override
        public List<TileRange> levels() {
            return source.levels().subList(startIndex, endIndex);
        }

        @Override
        public String toString() {
            return "SubsetView{levels=" + minZLevel + "-" + maxZLevel + ", source=" + source + "}";
        }
    }
}
