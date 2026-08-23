// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.subscriber;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Min;
import org.hiero.block.node.base.Loggable;

/**
 * Per-client throttle settings for {@code subscribeBlockStream}, for both the live and historical
 * weight classes a session can be classified into (see {@link SubscribeStreamWeigher}), plus the
 * threshold that defines that classification. The node-wide concurrency ceiling for both classes
 * lives separately, in the shared {@code GlobalThrottleConfig} (app-config) — see
 * {@code docs/design/apis/api-throttling.md} ("Configuration ownership") for why, and for why a
 * subscription's weight class, once classified at admission time, is never re-evaluated mid-session
 * even if it later catches up to the live tip or falls behind it.
 *
 * @param liveRatePerSecond sustained new live-tier subscriptions per second allowed for one client
 * @param liveBurstTolerance how many pacing intervals early a client's live-tier subscription may
 *     arrive and still be admitted
 * @param liveMaxConcurrentPerClient maximum concurrent in-flight live-tier sessions for one client
 * @param historicalRatePerSecond sustained new historical-tier subscriptions per second allowed for
 *     one client
 * @param historicalBurstTolerance how many pacing intervals early a client's historical-tier
 *     subscription may arrive and still be admitted
 * @param historicalMaxConcurrentPerClient maximum concurrent in-flight historical-tier sessions for
 *     one client
 * @param historicalThresholdBlocks a subscription whose requested start block is more than this
 *     many blocks behind the current maximum available block is classified as historical rather
 *     than live; a request with no fixed start block (a pure live subscription) is always live,
 *     regardless of this threshold
 */
// spotless:off - long annotations on record components must stay on one line
@ConfigData("throttle.subscribe")
public record SubscribeThrottleConfig(
        @Loggable @ConfigProperty(defaultValue = "5") @Min(1) int liveRatePerSecond,
        @Loggable @ConfigProperty(defaultValue = "3") @Min(0) int liveBurstTolerance,
        @Loggable @ConfigProperty(defaultValue = "5") @Min(1) int liveMaxConcurrentPerClient,
        @Loggable @ConfigProperty(defaultValue = "2") @Min(1) int historicalRatePerSecond,
        @Loggable @ConfigProperty(defaultValue = "1") @Min(0) int historicalBurstTolerance,
        @Loggable @ConfigProperty(defaultValue = "2") @Min(1) int historicalMaxConcurrentPerClient,
        @Loggable @ConfigProperty(defaultValue = "96_000") @Min(1) long historicalThresholdBlocks) {}
// spotless:on
