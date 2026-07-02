// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.metrics;

import static org.hiero.block.node.spi.BlockNodePlugin.METRICS_CATEGORY;

import org.hiero.metrics.LongCounter;
import org.hiero.metrics.LongCounter.Measurement;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

/// Holder for metrics used by [org.hiero.block.node.verification.session.BlockSessionHandler].
/// @param verificationBlocksReceived a [LongCounter.Measurement] of the number of blocks received for verification
public record SessionHandlerMetrics(Measurement verificationBlocksReceived) {
    /// Metric key for the number of blocks received for verification.
    private static final MetricKey<LongCounter> METRIC_VERIFICATION_BLOCKS_RECEIVED =
            MetricKey.of("verification_blocks_received", LongCounter.class).addCategory(METRICS_CATEGORY);

    /// Initialize and return a new [SessionHandlerMetrics] instance.
    /// @param metricRegistry used to create and initialize metrics
    /// @return a new [SessionHandlerMetrics] instance fully initialized
    public static SessionHandlerMetrics create(final MetricRegistry metricRegistry) {
        final Measurement verificationBlocksReceived = metricRegistry
                .register(LongCounter.builder(METRIC_VERIFICATION_BLOCKS_RECEIVED)
                        .setDescription("Blocks received for verification"))
                .getOrCreateNotLabeled();
        return new SessionHandlerMetrics(verificationBlocksReceived);
    }
}
