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
package io.tileverse.versatiles;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Shared test fixtures for the Versatiles module. The Andorra fixture is the Shortbread tileset of
 * {@code PMTilesTestData}, converted with {@code versatiles convert andorra.pmtiles andorra.versatiles}.
 */
public class VersatilesTestData {

    /** Copies {@code andorra.versatiles} into {@code tmpFolder}, unless an earlier call already did. */
    public static Path andorra(Path tmpFolder) throws IOException {
        return copyFixture("andorra.versatiles", tmpFolder);
    }

    /**
     * Copies {@code flowers.versatiles} into {@code tmpFolder}, unless an earlier call already did. The fixture is a
     * small WebP raster pyramid, converted from the {@code flowers.pmtiles} file published at
     * https://air.mtn.tw/flowers.pmtiles.
     */
    public static Path flowers(Path tmpFolder) throws IOException {
        return copyFixture("flowers.versatiles", tmpFolder);
    }

    /** The 21 vector layer ids present in the Andorra Shortbread fixture. */
    public static List<String> andorraLayerNames() {
        return List.of(
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

    private static Path copyFixture(String fileName, Path tmpFolder) throws IOException {
        Path target = tmpFolder.resolve(fileName);
        if (!Files.isRegularFile(target)) {
            String resource = "/io/tileverse/versatiles/" + fileName;
            try (InputStream in = requireNonNull(VersatilesTestData.class.getResourceAsStream(resource), resource)) {
                Files.copy(in, target);
            }
        }
        return target;
    }
}
