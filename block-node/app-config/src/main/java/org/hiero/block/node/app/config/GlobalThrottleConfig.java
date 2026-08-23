// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.config;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Min;
import org.hiero.block.node.base.Loggable;

/**
 * Node-wide concurrency ceilings for throttled gRPC methods, one field per method.
 *
 * <p>A ceiling here represents an allocation of the node's total shared capacity (connections,
 * heap, disk I/O) across every throttled API, which is inherently a node-level view rather than
 * something any single plugin can reason about on its own — unlike a method's per-client rate and
 * concurrency settings, which are owned by the plugin that implements that method. See
 * {@code docs/design/apis/api-throttling.md} ("Configuration ownership") for the full rationale.
 *
 * @param serverStatusMaxConcurrent maximum concurrent in-flight {@code serverStatus} /
 *     {@code serverStatusDetail} calls across all clients
 * @param getBlockLiveMaxConcurrent maximum concurrent in-flight {@code getBlock} calls for a
 *     live/recent block, across all clients
 * @param getBlockHistoricalMaxConcurrent maximum concurrent in-flight {@code getBlock} calls for a
 *     historical/archived block, across all clients — sized with the shared backend block-read
 *     bulkhead's capacity in mind once it exists (see the design doc's Component B)
 * @param clientStateTtlMinutes how long a throttled service keeps a client's rate/concurrency
 *     state after that client's last call before the entry becomes eligible for eviction; bounds
 *     the throttle's own memory use as new clients are seen over time (see
 *     {@code docs/design/apis/api-throttling.md} §5)
 * @param clientStateSweepIntervalMinutes how often the backstop sweep for stale, never-looked-up-again
 *     client entries runs
 */
@ConfigData("throttle.global")
public record GlobalThrottleConfig(
        @Loggable @ConfigProperty(defaultValue = "1000") @Min(1)
        int serverStatusMaxConcurrent,

        @Loggable @ConfigProperty(defaultValue = "200") @Min(1)
        int getBlockLiveMaxConcurrent,

        @Loggable @ConfigProperty(defaultValue = "50") @Min(1)
        int getBlockHistoricalMaxConcurrent,

        @Loggable @ConfigProperty(defaultValue = "30") @Min(1)
        int clientStateTtlMinutes,

        @Loggable @ConfigProperty(defaultValue = "5") @Min(1)
        int clientStateSweepIntervalMinutes) {}
