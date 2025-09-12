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
package io.tileverse.pmtiles;

import io.tileverse.tiling.pyramid.TileIndex;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;

/**
 * Interface for writing PMTiles files.
 * <p>
 * A PMTilesWriter is used to create PMTiles files by adding tiles and metadata,
 * then finalizing the file with the complete() method.
 */
public interface PMTilesWriter extends Closeable {

    /**
     * Adds a tile with the specified coordinates and data.
     *
     * @param tileIndex the tile coordinates
     * @param data the tile data
     * @throws IOException if an I/O error occurs
     * @throws IllegalStateException if the writer has been completed
     */
    void addTile(TileIndex tileIndex, byte[] data) throws IOException;

    /**
     * Sets the metadata for the PMTiles file.
     *
     * @param metadata the metadata as a JSON string
     * @throws IOException if an I/O error occurs
     * @throws IllegalStateException if the writer has been completed
     */
    void setMetadata(String metadata) throws IOException;

    /**
     * Completes the writing process and finalizes the PMTiles file.
     * This must be called to ensure all data is written to disk.
     *
     * @throws IOException if an I/O error occurs
     * @throws IllegalStateException if the writer has already been completed
     */
    void complete() throws IOException;

    /**
     * Sets a listener for progress updates during the writing process.
     *
     * @param listener the progress listener
     */
    void setProgressListener(ProgressListener listener);

    /**
     * Creates a new builder for configuring a PMTilesWriter.
     *
     * @return a new builder
     */
    static Builder builder() {
        return new PMTilesWriterImpl.BuilderImpl();
    }

    /**
     * Builder interface for creating a PMTilesWriter.
     */
    interface Builder {
        /**
         * Sets the path where the PMTiles file will be written.
         *
         * @param path the output path
         * @return this builder
         */
        Builder outputPath(Path path);

        /**
         * Sets the minimum zoom level for the PMTiles file.
         *
         * @param minZoom the minimum zoom level
         * @return this builder
         */
        Builder minZoom(int minZoom);

        /**
         * Sets the maximum zoom level for the PMTiles file.
         *
         * @param maxZoom the maximum zoom level
         * @return this builder
         */
        Builder maxZoom(int maxZoom);

        /**
         * Sets the compression type for tile data.
         *
         * @param compressionType the compression type
         * @return this builder
         */
        Builder tileCompression(byte compressionType);

        /**
         * Sets the compression type for internal structures (directories and metadata).
         *
         * @param compressionType the compression type
         * @return this builder
         */
        Builder internalCompression(byte compressionType);

        /**
         * Sets the tile type.
         *
         * @param tileType the tile type
         * @return this builder
         */
        Builder tileType(byte tileType);

        /**
         * Sets the geographic bounds of the tileset.
         *
         * @param minLon the minimum longitude
         * @param minLat the minimum latitude
         * @param maxLon the maximum longitude
         * @param maxLat the maximum latitude
         * @return this builder
         */
        Builder bounds(double minLon, double minLat, double maxLon, double maxLat);

        /**
         * Sets the center point for the tileset.
         *
         * @param lon the center longitude
         * @param lat the center latitude
         * @param zoom the center zoom level
         * @return this builder
         */
        Builder center(double lon, double lat, byte zoom);

        /**
         * Builds and returns a new PMTilesWriter.
         *
         * @return a new PMTilesWriter
         * @throws IOException if an I/O error occurs
         */
        PMTilesWriter build() throws IOException;
    }

    /**
     * Listener for progress updates during PMTiles file creation.
     */
    interface ProgressListener {
        /**
         * Called when progress is made during the writing process.
         *
         * @param progress the progress as a value between 0.0 and 1.0
         */
        void onProgress(double progress);

        /**
         * Checks if the operation should be cancelled.
         *
         * @return true if the operation should be cancelled, false otherwise
         */
        boolean isCancelled();
    }
}
