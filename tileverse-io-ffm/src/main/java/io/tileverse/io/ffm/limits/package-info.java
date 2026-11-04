/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
/**
 * The machine resource facts the read path sizes its native and disk budgets from, and the caps derived from them.
 *
 * <p>{@link io.tileverse.io.ffm.limits.ResourceLimits} is an injectable port reporting the raw facts - permitted
 * off-heap memory, usable disk, processor count, and the spill directory. The default reads the running container; a
 * test supplies a fixed instance for a deterministic bound independent of the host.
 *
 * <p>{@link io.tileverse.io.ffm.limits.IoLimits} derives the off-heap and spill caps by applying conservative
 * fractions to those facts. This is the single home for that sizing policy; the budgets of the readers consume
 * the caps and own no sizing of their own.
 */
package io.tileverse.io.ffm.limits;
