// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;

/**
 * Configuration for the live Hashgraph state plugin. Defaults match the production
 * filesystem layout described in {@code docs/design/state/live-state.md}.
 */
@ConfigData("state.management")
public record StateManagementConfig(
        @ConfigProperty(defaultValue = "/opt/hiero/block-node/data/state/stateMetadata.json")
        String stateMetadataPath,

        @ConfigProperty(defaultValue = "/opt/hiero/block-node/data/state/snapshot/recent")
        String stateSnapshotRecentPath,

        @ConfigProperty(defaultValue = "/opt/hiero/block-node/data/state/snapshot/historic")
        String stateSnapshotHistoricPath,

        @ConfigProperty(defaultValue = "900000") long snapshotIntervalMillis,
        @ConfigProperty(defaultValue = "2000") long stateChangesApplyIntervalMillis,
        @ConfigProperty(defaultValue = "64") int historicCatchUpBatchSize,
        /*
         * Number of historic snapshot tar archives to retain under
         * {@code stateSnapshotHistoricPath}. After a new tar is written, archives with
         * the lowest block numbers are deleted until the count is at or under this
         * threshold. A value of {@code 0} keeps every archive indefinitely (operator
         * cleans up by hand). Mirrors {@code files.historic.blockRetentionThreshold}.
         */
        @ConfigProperty(defaultValue = "0") long historicArchiveRetentionCount,
        /*
         * Number of recent snapshot directories to retain under
         * {@code stateSnapshotRecentPath} as hot, ready-to-load dirs. The newest
         * snapshot is always kept; older ones beyond this count are first
         * archived to historic (tar) and then removed from recent. Default
         * {@code 3} keeps the current dir plus two prior for fast restart-roll-back
         * scenarios while still archiving older ones to historic.
         */
        @ConfigProperty(defaultValue = "3") int stateSnapshotRecentRetentionCount) {}
