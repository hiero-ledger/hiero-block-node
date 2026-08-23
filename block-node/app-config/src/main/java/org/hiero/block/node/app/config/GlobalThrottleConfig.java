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
 */
@ConfigData("throttle.global")
public record GlobalThrottleConfig(
        @Loggable @ConfigProperty(defaultValue = "1000") @Min(1)
        int serverStatusMaxConcurrent) {}
