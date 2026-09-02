// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.metrics;

import static org.hiero.block.node.spi.BlockNodePlugin.METRICS_CATEGORY;

import org.hiero.metrics.LongCounter;
import org.hiero.metrics.LongGauge;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

/// Holder for metrics used by [org.hiero.block.node.block.verification.session.BlockSessionHandler].
/// @param verificationBlocksReceived a [LongCounter.Measurement] of the number of blocks received for verification
/// @param verificationActiveSessions a [LongGauge.Measurement] of the current size of the active sessions buffer
public record SessionHandlerMetrics(
        LongCounter.Measurement verificationBlocksReceived, LongGauge.Measurement verificationActiveSessions) {
    /// Metric key for the number of blocks received for verification.
    private static final MetricKey<LongCounter> METRIC_VERIFICATION_BLOCKS_RECEIVED =
            MetricKey.of("verification_blocks_received", LongCounter.class).addCategory(METRICS_CATEGORY);
    /// Metric key for the current size of the active sessions buffer.
    private static final MetricKey<LongGauge> METRIC_VERIFICATION_ACTIVE_SESSIONS =
            MetricKey.of("verification_active_sessions", LongGauge.class).addCategory(METRICS_CATEGORY);

    /// Initialize and return a new [SessionHandlerMetrics] instance.
    /// @param metricRegistry used to create and initialize metrics
    /// @return a new [SessionHandlerMetrics] instance fully initialized
    public static SessionHandlerMetrics create(final MetricRegistry metricRegistry) {
        final LongCounter.Measurement verificationBlocksReceived = metricRegistry
                .register(LongCounter.builder(METRIC_VERIFICATION_BLOCKS_RECEIVED)
                        .setDescription("Blocks received for verification"))
                .getOrCreateNotLabeled();
        final LongGauge.Measurement verificationActiveSessions = metricRegistry
                .register(LongGauge.builder(METRIC_VERIFICATION_ACTIVE_SESSIONS)
                        .setDescription("Currently active verification sessions"))
                .getOrCreateNotLabeled();
        return new SessionHandlerMetrics(verificationBlocksReceived, verificationActiveSessions);
    }
}
