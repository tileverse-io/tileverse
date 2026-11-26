/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * Steps run on the geometry of each feature before a vector tile writer writes the feature.
 *
 * <p>A {@link io.tileverse.vectortile.pipeline.GeometryPipeline} runs its
 * {@link io.tileverse.vectortile.pipeline.GeometryStep}s in order. A step can change the geometry, split it into the
 * members of a collection, or drop the feature. The steps know nothing of the tile format.
 *
 * <p>The steps work on JTS geometries in the coordinates given to the writer. These are tile coordinates, where the
 * tile spans from 0 to its extent on both axes, or coordinates in units of the input when the writer has an input
 * scale. The writer multiplies them by the scale after the steps, when it rounds them. The parameters of a step are in
 * the units of its geometries, and the steps working on the integer grid of the tile take the scale of the geometries.
 */
package io.tileverse.vectortile.pipeline;
