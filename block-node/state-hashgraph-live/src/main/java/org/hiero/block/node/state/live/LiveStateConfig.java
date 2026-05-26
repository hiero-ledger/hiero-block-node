// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.live;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;

/**
 * Configuration for the live Hashgraph state plugin. Defaults match the production
 * filesystem layout described in {@code docs/design/state/live-state.md}.
 */
@ConfigData("state.live")
public record LiveStateConfig(
        @ConfigProperty(defaultValue = "/opt/hiero/block-node/data/state/stateMetadata.json")
                String stateMetadataPath,
        @ConfigProperty(defaultValue = "/opt/hiero/block-node/data/state/snapshot/recent")
                String stateSnapshotRecentPath,
        @ConfigProperty(defaultValue = "/opt/hiero/block-node/data/state/snapshot/historic")
                String stateSnapshotHistoricPath,
        @ConfigProperty(defaultValue = "900000") long snapshotIntervalMillis,
        @ConfigProperty(defaultValue = "2000") long stateChangesApplyIntervalMillis,
        @ConfigProperty(defaultValue = "64") int historicCatchUpBatchSize) {}
