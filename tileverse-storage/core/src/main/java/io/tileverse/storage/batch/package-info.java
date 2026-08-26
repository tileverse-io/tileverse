/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Batch read planning and execution for {@link io.tileverse.storage.RangeReader#readRanges}.
 *
 * <p>{@link io.tileverse.storage.batch.BatchPlanner} merges nearby ranges into
 * {@link io.tileverse.storage.batch.PlannedFetch}es under a {@link io.tileverse.storage.batch.CoalescingPolicy};
 * {@link io.tileverse.storage.batch.BatchRunner} executes a plan with bounded parallelism on the
 * {@link io.tileverse.storage.batch.BatchExecutors shared executor}. Backends reuse the planner with their own
 * execution strategies (parallel object-store fetches, HTTP multipart).
 *
 * <p>System properties: {@code io.tileverse.storage.batch.executor} ({@code auto}, {@code virtual}, {@code pool}),
 * {@code io.tileverse.storage.batch.pool.size}, {@code io.tileverse.storage.batch.objectstore.maxgap},
 * {@code io.tileverse.storage.batch.http.maxgap}, and {@code io.tileverse.storage.batch.maxfetch}.
 */
package io.tileverse.storage.batch;
