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
 * @param scanInterval Interval in minutes to scan for missing gaps (skips if the previous task is running)
 * @param maxRetries Maximum number of retries to fetch a missing block (with exponential back-off)
 * @param initialRetryDelay Initial cooldown time between retries in milliseconds, will be multiplied by number of retry on each attempt
 * @param fetchBatchSize Number of blocks to fetch in a single gRPC call
 * @param delayBetweenBatches Cool downtime in milliseconds between batches of blocks to fetch
 * @param initialDelay Initial delay in seconds before starting the backfill process, to give time for the system to stabilize
 * @param perBlockProcessingTimeout Timeout in milliseconds for processing each block, to avoid blocking the backfill
 *                                  process indefinitely in case something unexpected happens, this would allow for self-recovery
 * @param grpcOverallTimeout single timeout configuration for gRPC Client construction, connectTimeout, readTimeout and pollWaitTime
 * @param enableTLS if enabled will assume block-node client supports tls connection.
 */
@ConfigData("backfill")
public record BackfillConfiguration(
        @Loggable @ConfigProperty(defaultValue = "0") @Min(0) long startBlock,
        @Loggable @ConfigProperty(defaultValue = "-1") @Min(-1) long endBlock,
        @Loggable @ConfigProperty(defaultValue = "") String blockNodeSourcesPath,
        @Loggable @ConfigProperty(defaultValue = "60000") @Min(100) int scanInterval,
        @Loggable @ConfigProperty(defaultValue = "3") @Min(0) @Max(10) int maxRetries,
        @Loggable @ConfigProperty(defaultValue = "5000") @Min(500) int initialRetryDelay,
        @Loggable @ConfigProperty(defaultValue = "25") @Min(1) @Max(10_000) int fetchBatchSize,
        @Loggable @ConfigProperty(defaultValue = "1000") @Min(100) int delayBetweenBatches,
        @Loggable @ConfigProperty(defaultValue = "15000") @Min(5) int initialDelay,
        @Loggable @ConfigProperty(defaultValue = "1000") @Min(500) int perBlockProcessingTimeout,
        @Loggable @ConfigProperty(defaultValue = "30000") @Min(10000) int grpcOverallTimeout,
        @Loggable @ConfigProperty(defaultValue = "false") boolean enableTLS) {}
