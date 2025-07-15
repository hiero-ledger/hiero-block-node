// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import org.hiero.block.common.utils.Preconditions;
import org.hiero.block.node.base.Loggable;

/**
 * Configuration for the Backfill module.
 *
 * @param firstBlockAvailable The first block that this BN deploy wants to have available
 * @param lastBlockToStore For some historical-purpose–specific BNs, there could be a maximum number of blocks, -1 means no limit.
 * @param blockNodeSourcesPath File path for a yaml configuration for the BN sources.
 * @param scanIntervalMins Interval in minutes to scan for missing gaps (skips if the previous task is running)
 * @param maxRetries Maximum number of retries to fetch a missing block (with exponential back-off)
 * @param fetchBatchSize Number of blocks to fetch in a single gRPC call
 * @param coolDownTimeBetweenBatchesMs Cool down time in milliseconds between batches of blocks to fetch
 */
@ConfigData("backfill")
public record BackfillConfiguration(
        @Loggable @ConfigProperty(defaultValue = "0") long firstBlockAvailable,
        @Loggable @ConfigProperty(defaultValue = "-1") long lastBlockToStore,
        @Loggable @ConfigProperty(defaultValue = "") String blockNodeSourcesPath,
        @Loggable @ConfigProperty(defaultValue = "60") int scanIntervalMins,
        @Loggable @ConfigProperty(defaultValue = "3") int maxRetries,
        @Loggable @ConfigProperty(defaultValue = "100") int fetchBatchSize,
        @Loggable @ConfigProperty(defaultValue = "1000") int coolDownTimeBetweenBatchesMs) {

    /**
     * Constructs a new instance of {@link BackfillConfiguration}.
     *
     * @param firstBlockAvailable The first block that this BN deploy wants to have available
     * @param lastBlockToStore For some historical-purpose–specific BNs, there could be a maximum number of blocks, -1 means no limit.
     * @param blockNodeSourcesPath File path for a yaml configuration for the BN sources.
     * @param scanIntervalMins Interval in minutes to scan for missing gaps (skips if the previous task is running)
     * @param maxRetries Maximum number of retries to fetch a missing block (with exponential back-off)
     * @param fetchBatchSize Number of blocks to fetch in a single gRPC call
     */
    public BackfillConfiguration {
        Preconditions.requireWhole(firstBlockAvailable, "[FIRST_BLOCK_AVAILABLE] must be non-negative");
        Preconditions.requirePositive(scanIntervalMins, "[SCAN_INTERVAL_MINS] must be positive");
        Preconditions.requirePositive(maxRetries, "[MAX_RETRIES] must be positive");
        Preconditions.requirePositive(fetchBatchSize, "[FETCH_BATCH_SIZE] must be positive");
    }
}
