// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.config;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Min;
import org.hiero.block.node.base.Loggable;

/**
 * Configuration for the shared block-storage read bulkhead ("Component B" in
 * {@code docs/design/apis/api-throttling.md} — a single, bounded, non-client-keyed permit pool
 * protecting block storage from combined read load across every API that reads from it, currently
 * {@code getBlock} and a subscriber session catching up on historical blocks).
 *
 * <p>Unlike {@link GlobalThrottleConfig}'s per-method admission ceilings (Component A, which limits
 * how much any one client/method may have outstanding), this bounds the node's actual backend read
 * capacity itself, shared across every call path that draws on it. Should be sized with
 * {@code throttle.getBlockHistorical.maxConcurrentPerClient}/{@code throttle.global.getBlockHistoricalMaxConcurrent}
 * in mind, since both draw from the same underlying resource — see the design doc's Component B
 * section for why sizing the two independently is a reasonable approximation, not a guarantee.
 *
 * @param permits the fixed size of the bulkhead's permit pool
 */
@ConfigData("throttle.blockReadBulkhead")
public record BlockReadBulkheadConfig(
        @Loggable @ConfigProperty(defaultValue = "50") @Min(1)
        int permits) {}
