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
package io.tileverse.vectortile.mvt;

/**
 * MVT command constants for geometry encoding.
 */
final class GeometryCommand {
    /**
     * MoveTo: 1. (2 parameters follow)
     */
    public static final int MoveTo = 1;

    /**
     * LineTo: 2. (2 parameters follow)
     */
    public static final int LineTo = 2;

    /**
     * ClosePath: 7. (no parameters follow)
     */
    public static final int ClosePath = 7;

    private GeometryCommand() {}

    static int packCommandAndLength(int command, int repeat) {
        return repeat << 3 | command;
    }

    /**
     * Extracts the command type from an MVT command integer.
     * <p>
     * MVT commands encode both command type (lower 3 bits) and count (upper bits).
     *
     * @param cmd the packed command integer
     * @return command type (1=MoveTo, 2=LineTo, 7=ClosePath)
     */
    static int extractCommand(int cmd) {
        return cmd & 0b00000111;
    }

    /**
     * Extracts the parameter count from an MVT command integer.
     * <p>
     * MVT commands encode both command type (lower 3 bits) and count (upper bits).
     *
     * @param cmd the packed command integer
     * @return number of coordinate pairs that follow this command
     */
    static int extractCount(int cmd) {
        return cmd >> 3;
    }
}
