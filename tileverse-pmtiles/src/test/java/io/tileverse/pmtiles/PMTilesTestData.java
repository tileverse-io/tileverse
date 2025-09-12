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

import static java.util.Objects.requireNonNull;

import io.tileverse.rangereader.RangeReader;
import io.tileverse.rangereader.file.FileRangeReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

public class PMTilesTestData {

    public static Path andorra(Path tmpFolder) throws IOException {
        Path andorraPmTiles = tmpFolder.resolve("andorra.pmtiles");
        if (!Files.isRegularFile(andorraPmTiles)) {
            try (InputStream in = requireNonNull(
                    PMTilesReaderTest.class.getResourceAsStream("/io/tileverse/pmtiles/andorra.pmtiles"))) {
                Files.copy(in, andorraPmTiles);
            }
        }
        return andorraPmTiles;
    }

    public static RangeReader andorraFileRangeReader(Path tmpFolder) throws IOException {
        return FileRangeReader.of(andorra(tmpFolder));
    }

    public static List<String> andorraLayerNames() {
        return Arrays.asList(
                "addresses",
                "aerialways",
                "boundaries",
                "boundary_labels",
                "bridges",
                "buildings",
                "dam_lines",
                "dam_polygons",
                "land",
                "place_labels",
                "pois",
                "public_transport",
                "sites",
                "street_labels",
                "street_polygons",
                "streets",
                "streets_polygons_labels",
                "water_lines",
                "water_lines_labels",
                "water_polygons",
                "water_polygons_labels");
    }
}
