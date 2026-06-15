// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.metrics;

import static org.hiero.block.node.spi.BlockNodePlugin.METRICS_CATEGORY;

import org.hiero.metrics.LongCounter;
import org.hiero.metrics.LongCounter.Measurement;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

/// Holder for metrics used by [org.hiero.block.node.block.verification.session.SessionResultHandler].
/// @param verificationBlocksVerified a [LongCounter.Measurement] of the number of blocks that passed verification
/// @param verificationBlocksFailed a [LongCounter.Measurement] of the number of blocks that failed verification
/// @param verificationBlocksError a [LongCounter.Measurement] of the number of internal errors during verification
public record SessionResultMetrics(
        Measurement verificationBlocksVerified,
        Measurement verificationBlocksFailed,
        Measurement verificationBlocksError) {
    /// Metric key for the number of blocks that passed verification
    private static final MetricKey<LongCounter> METRIC_VERIFICATION_BLOCKS_VERIFIED =
            MetricKey.of("verification_blocks_verified", LongCounter.class).addCategory(METRICS_CATEGORY);
    /// Metric key for the number of blocks that failed verification
    private static final MetricKey<LongCounter> METRIC_VERIFICATION_BLOCKS_FAILED =
            MetricKey.of("verification_blocks_failed", LongCounter.class).addCategory(METRICS_CATEGORY);
    /// Metric key for the number of internal errors during verification
    private static final MetricKey<LongCounter> METRIC_VERIFICATION_BLOCKS_ERROR =
            MetricKey.of("verification_blocks_error", LongCounter.class).addCategory(METRICS_CATEGORY);

    /// Initialize and return a new [SessionResultMetrics] instance.
    /// @param metricRegistry used to create and initialize metrics
    /// @return a new [SessionResultMetrics] instance fully initialized
    public static SessionResultMetrics create(final MetricRegistry metricRegistry) {
        final Measurement verificationBlocksVerified = metricRegistry
                .register(LongCounter.builder(METRIC_VERIFICATION_BLOCKS_VERIFIED)
                        .setDescription("Blocks that passed verification"))
                .getOrCreateNotLabeled();
        final Measurement verificationBlocksFailed = metricRegistry
                .register(LongCounter.builder(METRIC_VERIFICATION_BLOCKS_FAILED)
                        .setDescription("Blocks that failed verification"))
                .getOrCreateNotLabeled();
        final Measurement verificationBlocksError = metricRegistry
                .register(LongCounter.builder(METRIC_VERIFICATION_BLOCKS_ERROR)
                        .setDescription("Internal errors during verification"))
                .getOrCreateNotLabeled();
        return new SessionResultMetrics(verificationBlocksVerified, verificationBlocksFailed, verificationBlocksError);
    }
}
