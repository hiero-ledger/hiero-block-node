// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Max;
import com.swirlds.config.api.validation.annotation.Min;
import org.hiero.block.node.base.Loggable;

/**
 * Configuration for the Backfill module.
 *
 * @param startBlock The first block that this BN deploy wants to have available
 * @param endBlock For some historical-purpose–specific BNs, there could be a maximum number of blocks, -1 means no limit.
 * @param blockNodeSourcesPath File path for a yaml configuration for the BN sources.
 * @param scanIntervalMs Interval in minutes to scan for missing gaps (skips if the previous task is running)
 * @param maxRetries Maximum number of retries to fetch a missing block (with exponential back-off)
 * @param initialRetryDelayMs Initial cooldown time between retries in milliseconds, will be multiplied by number of retry on each attempt
 * @param fetchBatchSize Number of blocks to fetch in a single gRPC call
 * @param delayBetweenBatchesMs Cool downtime in milliseconds between batches of blocks to fetch
 * @param initialDelayMs Initial delay in seconds before starting the backfill process, to give time for the system to stabilize
 */
@ConfigData("backfill")
public record BackfillConfiguration(
        @Loggable @ConfigProperty(defaultValue = "0") @Min(0) long startBlock,
        @Loggable @ConfigProperty(defaultValue = "-1") @Min(-1) long endBlock,
        @Loggable @ConfigProperty(defaultValue = "") String blockNodeSourcesPath,
        @Loggable @ConfigProperty(defaultValue = "60000") @Min(100) int scanIntervalMs,
        @Loggable @ConfigProperty(defaultValue = "3") @Min(0) @Max(10) int maxRetries,
        @Loggable @ConfigProperty(defaultValue = "5000") @Min(500) int initialRetryDelayMs,
        @Loggable @ConfigProperty(defaultValue = "100") @Min(1) @Max(10_000) int fetchBatchSize,
        @Loggable @ConfigProperty(defaultValue = "1000") @Min(100) int delayBetweenBatchesMs,
        @Loggable @ConfigProperty(defaultValue = "15000") @Min(5) int initialDelayMs) {}
