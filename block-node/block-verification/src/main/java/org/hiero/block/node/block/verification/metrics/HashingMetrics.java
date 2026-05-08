// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.metrics;

import static org.hiero.block.node.spi.BlockNodePlugin.METRICS_CATEGORY;

import org.hiero.metrics.LongCounter;
import org.hiero.metrics.LongCounter.Measurement;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

/// Holder for metrics used by [org.hiero.block.node.block.verification.hasher.BlockHasher]
/// @param hashingBlockTimeNs a [LongCounter.Measurement] of the time spent hashing a block
public record HashingMetrics(Measurement hashingBlockTimeNs) {
    /// Metric key for block hashing time.
    private static final MetricKey<LongCounter> METRIC_HASHING_BLOCK_TIME =
            MetricKey.of("hashing_block_time", LongCounter.class).addCategory(METRICS_CATEGORY);

    /// Initialize and return a new [HashingMetrics] instance.
    /// @param metricRegistry used to create and initialize metrics
    /// @return a new [HashingMetrics] instance fully initialized
    public static HashingMetrics create(final MetricRegistry metricRegistry) {
        final Measurement hashingBlockTimeNs = metricRegistry
                .register(LongCounter.builder(METRIC_HASHING_BLOCK_TIME).setDescription("Hashing time per block (ms)"))
                .getOrCreateNotLabeled();
        return new HashingMetrics(hashingBlockTimeNs);
    }
}
