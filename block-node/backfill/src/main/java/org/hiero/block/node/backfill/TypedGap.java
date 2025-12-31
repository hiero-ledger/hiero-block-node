// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import edu.umd.cs.findbugs.annotations.NonNull;
import org.hiero.block.node.spi.historicalblocks.LongRange;

/**
 * A detected gap with its classification for scheduling decisions.
 *
 * @param range the block range representing the gap
 * @param type the classification of this gap (historical or live-tail)
 */
public record TypedGap(@NonNull LongRange range, @NonNull GapType type) {}
