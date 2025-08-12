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
package io.tileverse.tiling.model;

/**
 * A view of a TileRange that transforms coordinates to a different axis origin.
 * Computes transformed coordinates on-demand without storing duplicate data.
 *
 * @since 1.0
 */
class AxisOriginView implements TileRange {

    private final TileRange source;
    private final AxisOrigin targetOrigin;

    public AxisOriginView(TileRange source, AxisOrigin targetOrigin) {
        this.source = source;
        this.targetOrigin = targetOrigin;
    }

    @Override
    public int zoomLevel() {
        return source.zoomLevel();
    }

    @Override
    public long minx() {
        if (source.axisOrigin().needsXFlip(targetOrigin)) {
            long tilesPerSide = 1L << zoomLevel();
            return (tilesPerSide - 1) - source.maxx();
        }
        return source.minx();
    }

    @Override
    public long miny() {
        if (source.axisOrigin().needsYFlip(targetOrigin)) {
            long tilesPerSide = 1L << zoomLevel();
            return (tilesPerSide - 1) - source.maxy();
        }
        return source.miny();
    }

    @Override
    public long maxx() {
        if (source.axisOrigin().needsXFlip(targetOrigin)) {
            long tilesPerSide = 1L << zoomLevel();
            return (tilesPerSide - 1) - source.minx();
        }
        return source.maxx();
    }

    @Override
    public long maxy() {
        if (source.axisOrigin().needsYFlip(targetOrigin)) {
            long tilesPerSide = 1L << zoomLevel();
            return (tilesPerSide - 1) - source.miny();
        }
        return source.maxy();
    }

    @Override
    public long spanX() {
        return source.spanX(); // Span doesn't change with axis origin
    }

    @Override
    public long spanY() {
        return source.spanY(); // Span doesn't change with axis origin
    }

    @Override
    public long count() {
        return source.count(); // Count doesn't change with axis origin
    }

    @Override
    public long countMetaTiles(int width, int height) {
        return source.countMetaTiles(width, height); // Meta-tile count doesn't change with axis origin
    }

    // Note: Corner methods use the default implementations from TileRange interface
    // which are axis-origin aware and use the switch statements based on axisOrigin()

    @Override
    public AxisOrigin axisOrigin() {
        return targetOrigin;
    }

    @Override
    public TileRange withAxisOrigin(AxisOrigin newTargetOrigin) {
        if (targetOrigin == newTargetOrigin) {
            return this;
        }
        // If converting back to source origin, unwrap the view
        if (source.axisOrigin() == newTargetOrigin) {
            return source;
        }
        // Chain views for multiple transformations
        return new AxisOriginView(this, newTargetOrigin);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof TileRange other && TileRange.equals(this, other);
    }

    @Override
    public int hashCode() {
        return TileRange.hashCode(this);
    }

    @Override
    public String toString() {
        return "AxisOriginView{" + targetOrigin + ", source=" + source + "}";
    }
}
