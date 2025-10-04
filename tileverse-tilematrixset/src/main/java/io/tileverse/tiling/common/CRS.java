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
package io.tileverse.tiling.common;

/**
 * Coordinate Reference System representation supporting both URI and WKT formats.
 *
 * @param uri the CRS URI identifier (e.g., "EPSG:4326")
 * @param wkt the Well-Known Text representation of the CRS
 */
public record CRS(String uri, String wkt) {

    /**
     * Creates a CRS from a URI identifier.
     *
     * @param uri the CRS URI identifier
     * @return a new CRS with the specified URI
     */
    public static CRS ofURI(String uri) {
        return new CRS(uri, null);
    }

    /**
     * Creates a CRS from a Well-Known Text representation.
     *
     * @param wkt the WKT string
     * @return a new CRS with the specified WKT
     */
    public static CRS ofWKT(String wkt) {
        return new CRS(null, wkt);
    }
}
