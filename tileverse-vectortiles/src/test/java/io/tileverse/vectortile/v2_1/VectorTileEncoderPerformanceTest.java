/*
 * Copyright 2015 Electronic Chart Centre
 * Copyright 2025 Multiversio LLC. All rights reserved.
 *
 * Modifications: Modernized and integrated into Tileverse PMTiles project.
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
package io.tileverse.vectortile.v2_1;

import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.util.Stopwatch;

class VectorTileEncoderPerformanceTest {

    /**
     * A utility to help benchmark the building performance of large Point based
     * vector tiles. This adds 512x512 "pixels" of data 100 times, allowing a
     * profiler to connect and determine where the bottlenecks are.
     */
    @Test
    void testManyPoints() {
        int tileSize = 512;
        Map<String, Object> empty = Collections.emptyMap();
        for (int i = 0; i < 100; i++) {
            VectorTileEncoder encoder = new VectorTileEncoder(tileSize, 0, false);
            GeometryFactory geometryFactory = new GeometryFactory();
            Stopwatch sw = new Stopwatch();
            int features = 0;
            for (int x = 0; x < tileSize; x++) {
                for (int y = 0; y < tileSize; y++) {
                    Geometry geom = geometryFactory.createPoint(new Coordinate(x, y));
                    encoder.addFeature("layer1", empty, geom);
                    features++;
                }
            }
            System.out.println("Added " + features + " in " + sw.getTime() + "msecs");
        }
    }
}
