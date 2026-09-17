// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.metrics;

import static org.hiero.block.node.spi.BlockNodePlugin.METRICS_CATEGORY;

import org.hiero.block.node.block.verification.session.SessionPriority;
import org.hiero.metrics.LongCounter;
import org.hiero.metrics.LongGauge;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

/// Holder for metrics used by [org.hiero.block.node.block.verification.session.BlockSessionHandler].
/// @param verificationBlocksReceived a [LongCounter.Measurement] of the number of blocks received for verification
/// @param verificationActiveSessions a [LongGauge.Measurement] of the current size of the active sessions buffer
/// @param verificationSessionsEvictedHigh `verification_sessions_evicted{priority="high"}`, high priority
///     sessions evicted from the active sessions buffer
/// @param verificationSessionsEvictedLow `verification_sessions_evicted{priority="low"}`, low priority
///     sessions evicted from the active sessions buffer
public record SessionHandlerMetrics(
        LongCounter.Measurement verificationBlocksReceived,
        LongGauge.Measurement verificationActiveSessions,
        LongCounter.Measurement verificationSessionsEvictedHigh,
        LongCounter.Measurement verificationSessionsEvictedLow) {
    /// Metric key for the number of blocks received for verification.
    private static final MetricKey<LongCounter> METRIC_VERIFICATION_BLOCKS_RECEIVED =
            MetricKey.of("verification_blocks_received", LongCounter.class).addCategory(METRICS_CATEGORY);
    /// Metric key for the current size of the active sessions buffer.
    private static final MetricKey<LongGauge> METRIC_VERIFICATION_ACTIVE_SESSIONS =
            MetricKey.of("verification_active_sessions", LongGauge.class).addCategory(METRICS_CATEGORY);
    /// Metric key for the number of sessions evicted from the active sessions buffer.
    private static final MetricKey<LongCounter> METRIC_VERIFICATION_SESSIONS_EVICTED =
            MetricKey.of("verification_sessions_evicted", LongCounter.class).addCategory(METRICS_CATEGORY);
    /// Dynamic label name identifying the priority of the evicted session.
    private static final String LABEL_PRIORITY = "priority";
    /// Label value for high priority sessions.
    private static final String PRIORITY_HIGH = "high";
    /// Label value for low priority sessions.
    private static final String PRIORITY_LOW = "low";

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
        final LongCounter evictedCounter = metricRegistry.register(LongCounter.builder(
                        METRIC_VERIFICATION_SESSIONS_EVICTED)
                .setDescription("Verification sessions evicted from the active sessions buffer by priority (high|low)")
                .addDynamicLabelNames(LABEL_PRIORITY));
        final LongCounter.Measurement evictedHigh = evictedCounter.getOrCreateLabeled(LABEL_PRIORITY, PRIORITY_HIGH);
        final LongCounter.Measurement evictedLow = evictedCounter.getOrCreateLabeled(LABEL_PRIORITY, PRIORITY_LOW);
        return new SessionHandlerMetrics(
                verificationBlocksReceived, verificationActiveSessions, evictedHigh, evictedLow);
    }

    /// The evicted sessions measurement for the given priority.
    /// @param priority the priority of the evicted session
    /// @return the matching measurement
    public LongCounter.Measurement verificationSessionsEvicted(final SessionPriority priority) {
        return switch (priority) {
            case HIGH -> verificationSessionsEvictedHigh;
            case LOW -> verificationSessionsEvictedLow;
        };
    }
}
