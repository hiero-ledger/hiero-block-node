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

        @ConfigProperty(defaultValue = "900000") long snapshotIntervalMillis,
        @ConfigProperty(defaultValue = "2000") long stateChangesApplyIntervalMillis,
        @ConfigProperty(defaultValue = "64") int historicCatchUpBatchSize,
        /*
         * Number of recent snapshot directories to retain under
         * {@code stateSnapshotRecentPath} as hot, ready-to-load dirs. Each dir
         * hard-links into the live MerkleDb, so snapshots are cheap in time and
         * disk. The newest snapshot is always kept; older ones beyond this count
         * are removed. Default {@code 3} keeps the current dir plus two prior for
         * fast restart / roll-back. Long-term archival is out of scope for this
         * plugin — a future archiving plugin owns it.
         */
        @ConfigProperty(defaultValue = "3") int stateSnapshotRecentRetentionCount) {}
