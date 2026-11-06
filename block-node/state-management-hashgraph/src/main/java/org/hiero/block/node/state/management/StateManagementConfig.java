// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Max;
import com.swirlds.config.api.validation.annotation.Min;
import org.hiero.block.node.base.Loggable;

// spotless:off - long annotations on record components must stay on one line
/// Configuration for the live Hashgraph state plugin. Defaults match the production
/// filesystem layout described in `docs/design/state/live-state.md`.
///
/// `port` is the dedicated port the `StateService` gRPC service binds to; `null` (the default)
/// shares `server.port`. No `@Min`/`@Max` because those validators reject `null`; the web
/// server enforces the range when binding. `maxQueueRangeSize` caps the elements a single
/// `getQueue` returns, for an explicit `queue_range` and for a full-queue read alike.
@ConfigData("state.management")
public record StateManagementConfig(
        @ConfigProperty(defaultValue = "/opt/hiero/block-node/data/state/snapshot/recent")
        String snapshotRecentPath,

        @Loggable @ConfigProperty(defaultValue = ConfigProperty.NULL_DEFAULT_VALUE) Integer port,

        @Loggable @ConfigProperty(defaultValue = "1000") @Min(1) @Max(100_000) int maxQueueRangeSize) {
    // spotless:on
}
