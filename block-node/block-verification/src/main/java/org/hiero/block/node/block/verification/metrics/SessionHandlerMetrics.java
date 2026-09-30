// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.metrics;

import static org.hiero.block.node.spi.BlockNodePlugin.METRICS_CATEGORY;

import org.hiero.metrics.LongCounter;
import org.hiero.metrics.LongGauge;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

/// Holder for metrics used by [org.hiero.block.node.block.verification.session.BlockSessionHandler].
/// @param verificationBlocksReceived a [LongCounter.Measurement] of the number of blocks received for verification
/// @param verificationActiveSessions a [LongGauge.Measurement] of the combined size of both lanes of the active
///     sessions buffer
/// @param verificationSessionsEvictedHighPriority a [LongCounter.Measurement] of the sessions evicted from the
///     high priority lane, `verification_sessions_evicted{lane="high_priority"}`
/// @param verificationSessionsEvictedLowPriority a [LongCounter.Measurement] of the sessions evicted from the
///     low priority lane, `verification_sessions_evicted{lane="low_priority"}`
public record SessionHandlerMetrics(
        LongCounter.Measurement verificationBlocksReceived,
        LongGauge.Measurement verificationActiveSessions,
        LongCounter.Measurement verificationSessionsEvictedHighPriority,
        LongCounter.Measurement verificationSessionsEvictedLowPriority) {
    /// Metric key for the number of blocks received for verification.
    private static final MetricKey<LongCounter> METRIC_VERIFICATION_BLOCKS_RECEIVED =
            MetricKey.of("verification_blocks_received", LongCounter.class).addCategory(METRICS_CATEGORY);
    /// Metric key for the combined size of the active sessions buffer.
    private static final MetricKey<LongGauge> METRIC_VERIFICATION_ACTIVE_SESSIONS =
            MetricKey.of("verification_active_sessions", LongGauge.class).addCategory(METRICS_CATEGORY);
    /// Metric key for the number of sessions evicted from the active sessions buffer.
    private static final MetricKey<LongCounter> METRIC_VERIFICATION_SESSIONS_EVICTED =
            MetricKey.of("verification_sessions_evicted", LongCounter.class).addCategory(METRICS_CATEGORY);
    /// Dynamic label name identifying the lane a session was evicted from.
    private static final String LABEL_LANE = "lane";
    /// Label value for the high priority lane.
    private static final String LANE_HIGH_PRIORITY = "high_priority";
    /// Label value for the low priority lane.
    private static final String LANE_LOW_PRIORITY = "low_priority";

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
        final LongCounter sessionsEvicted = metricRegistry.register(LongCounter.builder(
                        METRIC_VERIFICATION_SESSIONS_EVICTED)
                .setDescription("Sessions evicted from the active sessions buffer by lane (high_priority|low_priority)")
                .addDynamicLabelNames(LABEL_LANE));
        final LongCounter.Measurement evictedHighPriority =
                sessionsEvicted.getOrCreateLabeled(LABEL_LANE, LANE_HIGH_PRIORITY);
        final LongCounter.Measurement evictedLowPriority =
                sessionsEvicted.getOrCreateLabeled(LABEL_LANE, LANE_LOW_PRIORITY);
        return new SessionHandlerMetrics(
                verificationBlocksReceived, verificationActiveSessions, evictedHighPriority, evictedLowPriority);
    }
}
