// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.server.status;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Min;
import org.hiero.block.node.base.Loggable;
import org.hiero.block.node.spi.throttle.MethodThrottleConfig;

/// Per-client throttle settings for `BlockNodeService.serverStatus`. The node-wide concurrency
/// ceiling for this method lives separately in the shared `GlobalThrottleConfig` (app-config).
///
/// @param ratePerSecond sustained requests per second allowed for one client
/// @param burstTolerance how many pacing intervals early a client's request may arrive and still
///     be admitted
/// @param maxConcurrentPerClient maximum concurrent in-flight calls for one client
/// @param maxPermitHoldSeconds age after which a still-held concurrency permit is force-released
///     by the background sweep; `0` never reaps. Only corrects the throttle's accounting for calls
///     whose connection dropped before completing, it does not terminate any call.
@ConfigData("throttle.serverStatus")
public record ServerStatusThrottleConfig(
        @Loggable @ConfigProperty(defaultValue = "50") @Min(1)
        int ratePerSecond,

        @Loggable @ConfigProperty(defaultValue = "20") @Min(0)
        int burstTolerance,

        @Loggable @ConfigProperty(defaultValue = "5") @Min(1)
        int maxConcurrentPerClient,

        @Loggable @ConfigProperty(defaultValue = "10") @Min(0)
        int maxPermitHoldSeconds)
        implements MethodThrottleConfig {}
