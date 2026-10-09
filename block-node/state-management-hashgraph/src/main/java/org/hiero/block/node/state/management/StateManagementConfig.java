// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;

/// Configuration for the live Hashgraph state plugin. Defaults match the production
/// filesystem layout described in `docs/design/state/live-state.md`.
@ConfigData("state.management")
public record StateManagementConfig(
        @ConfigProperty(defaultValue = "/opt/hiero/block-node/data/state/stateMetadata.json")
        String stateMetadataPath,

        @ConfigProperty(defaultValue = "/opt/hiero/block-node/data/state/snapshot/recent")
        String stateSnapshotRecentPath,

        @ConfigProperty(defaultValue = "64") int historicCatchUpBatchSize) {}
