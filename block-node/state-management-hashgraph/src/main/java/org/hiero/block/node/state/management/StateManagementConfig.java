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
@ConfigData("state.management")
public record StateManagementConfig(
        @ConfigProperty(defaultValue = "/opt/hiero/block-node/data/state/snapshot/recent")
        String snapshotRecentPath,

        @Loggable @ConfigProperty(defaultValue = "64") @Max(512) @Min(1) int historicCatchUpBatchSize,

        @Loggable @ConfigProperty(defaultValue = "50") @Max(10_000) @Min(1) int pendingBlocksMax) {
    // spotless:on
}
