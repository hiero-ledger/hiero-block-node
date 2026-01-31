// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

// org.apache.commons.lang3.tuple.Pair
/** A generic immutable pair of two values, replacing {@code org.apache.commons.lang3.tuple.Pair}. */
public record Pair<L, R>(L left, R right) {}
