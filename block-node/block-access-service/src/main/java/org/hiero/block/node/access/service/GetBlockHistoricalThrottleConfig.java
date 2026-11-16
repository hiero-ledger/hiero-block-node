// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.access.service;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Min;
import org.hiero.block.node.base.Loggable;
import org.hiero.block.node.spi.throttle.MethodThrottleConfig;

/// Per-client throttle settings for `BlockAccessService.getBlock` requests for historical/archived
/// blocks ([org.hiero.block.node.spi.throttle.WeightClass#HEAVY]), deliberately stricter than the
/// live settings. The node-wide ceiling lives in the shared `GlobalThrottleConfig` (app-config).
///
/// @param ratePerSecond sustained requests per second allowed for one client
/// @param burstTolerance how many pacing intervals early a request may arrive and still be admitted
/// @param maxConcurrentPerClient maximum concurrent in-flight calls for one client
/// @param maxPermitHoldSeconds age after which a still-held permit is force-released; `0` never
///     reaps
/// @param historicalThresholdBlocks a request is historical if it targets a block more than this
///     many blocks behind the current maximum available block
@ConfigData("throttle.getBlockHistorical")
public record GetBlockHistoricalThrottleConfig(
        @Loggable @ConfigProperty(defaultValue = "5") @Min(1)
        int ratePerSecond,

        @Loggable @ConfigProperty(defaultValue = "3") @Min(0)
        int burstTolerance,

        @Loggable @ConfigProperty(defaultValue = "3") @Min(1)
        int maxConcurrentPerClient,

        @Loggable @ConfigProperty(defaultValue = "10") @Min(0)
        int maxPermitHoldSeconds,

        @Loggable @ConfigProperty(defaultValue = "96000") @Min(0)
        long historicalThresholdBlocks)
        implements MethodThrottleConfig {}
