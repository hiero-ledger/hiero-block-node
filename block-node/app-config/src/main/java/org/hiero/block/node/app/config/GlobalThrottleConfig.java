// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.config;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Min;
import org.hiero.block.node.base.Loggable;

/// Node-wide concurrency ceilings for throttled gRPC methods, one field per method, plus
/// mechanism-wide client-state tuning.
///
/// These ceilings allocate the node's total shared capacity across every throttled API, a
/// node-level view no single plugin can reason about on its own, unlike per-client rate and
/// concurrency settings which the owning plugin configures itself. See
/// `docs/design/apis/api-throttling.md` ("Configuration ownership").
///
/// @param serverStatusMaxConcurrent maximum concurrent in-flight `serverStatus` calls across all clients
/// @param serverStatusDetailMaxConcurrent maximum concurrent in-flight `serverStatusDetail` calls
///     across all clients
/// @param clientStateTtlMinutes how long a throttled method keeps a client's rate/concurrency
///     state after its last call before the entry becomes eligible for eviction
/// @param clientStateSweepIntervalMinutes how often the backstop sweep for stale client entries runs
@ConfigData("throttle.global")
public record GlobalThrottleConfig(
        @Loggable @ConfigProperty(defaultValue = "1000") @Min(1)
        int serverStatusMaxConcurrent,

        @Loggable @ConfigProperty(defaultValue = "1000") @Min(1)
        int serverStatusDetailMaxConcurrent,

        @Loggable @ConfigProperty(defaultValue = "30") @Min(1)
        int clientStateTtlMinutes,

        @Loggable @ConfigProperty(defaultValue = "5") @Min(1)
        int clientStateSweepIntervalMinutes) {}
