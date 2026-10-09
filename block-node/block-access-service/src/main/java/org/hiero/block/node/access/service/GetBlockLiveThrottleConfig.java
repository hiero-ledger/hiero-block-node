// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.access.service;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Min;
import org.hiero.block.node.base.Loggable;

/**
 * Per-client throttle settings for {@code getBlock} requests classified as live/recent (see
 * {@link GetBlockWeigher}). The node-wide concurrency ceiling for this tier lives separately, in
 * the shared {@code GlobalThrottleConfig} (app-config) — see
 * {@code docs/design/apis/api-throttling.md} ("Configuration ownership") for why.
 *
 * @param ratePerSecond sustained requests per second allowed for one client
 * @param burstTolerance how many pacing intervals early a client's request may arrive and still
 *     be admitted
 * @param maxConcurrentPerClient maximum concurrent in-flight calls for one client
 */
@ConfigData("throttle.getBlockLive")
public record GetBlockLiveThrottleConfig(
        @Loggable @ConfigProperty(defaultValue = "20") @Min(1)
        int ratePerSecond,

        @Loggable @ConfigProperty(defaultValue = "10") @Min(0)
        int burstTolerance,

        @Loggable @ConfigProperty(defaultValue = "10") @Min(1)
        int maxConcurrentPerClient) {}
