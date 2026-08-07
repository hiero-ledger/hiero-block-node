// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.metrics;

import static org.hiero.block.node.spi.BlockNodePlugin.METRICS_CATEGORY;

import org.hiero.metrics.LongCounter;
import org.hiero.metrics.LongCounter.Measurement;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

/// Holder for metrics used by [org.hiero.block.node.block.verification.hasher.BlockHasher]
/// @param hashingBlockTimeNs a [LongCounter.Measurement] of the time spent hashing a block
/// @param futureItemsHashed a [LongCounter.Measurement] counting future (unknown to this
///     version) block item types placed into a subtree by the forward compatibility numbering rule
/// @param futureItemsNotHashed a [LongCounter.Measurement] counting future block item types
///     safely ignored as not hashed per the forward compatibility numbering rule
/// @param futureItemsRefused a [LongCounter.Measurement] counting future block item types that
///     caused a block to be refused because this version cannot process them (upgrade required)
public record HashingMetrics(
        Measurement hashingBlockTimeNs,
        Measurement futureItemsHashed,
        Measurement futureItemsNotHashed,
        Measurement futureItemsRefused) {
    /// Metric key for block hashing time.
    private static final MetricKey<LongCounter> METRIC_HASHING_BLOCK_TIME =
            MetricKey.of("hashing_block_time", LongCounter.class).addCategory(METRICS_CATEGORY);

    /// Metric key for future item types hashed via the forward compatibility numbering rule.
    private static final MetricKey<LongCounter> METRIC_FUTURE_ITEMS_HASHED =
            MetricKey.of("hashing_future_items_hashed", LongCounter.class).addCategory(METRICS_CATEGORY);

    /// Metric key for future item types ignored as not hashed.
    private static final MetricKey<LongCounter> METRIC_FUTURE_ITEMS_NOT_HASHED =
            MetricKey.of("hashing_future_items_not_hashed", LongCounter.class).addCategory(METRICS_CATEGORY);

    /// Metric key for future item types that caused a block to be refused.
    private static final MetricKey<LongCounter> METRIC_FUTURE_ITEMS_REFUSED =
            MetricKey.of("hashing_future_items_refused", LongCounter.class).addCategory(METRICS_CATEGORY);

    /// Initialize and return a new [HashingMetrics] instance.
    /// @param metricRegistry used to create and initialize metrics
    /// @return a new [HashingMetrics] instance fully initialized
    public static HashingMetrics create(final MetricRegistry metricRegistry) {
        final Measurement hashingBlockTimeNs = metricRegistry
                .register(LongCounter.builder(METRIC_HASHING_BLOCK_TIME).setDescription("Hashing time per block (ns)"))
                .getOrCreateNotLabeled();
        final Measurement futureItemsHashed = metricRegistry
                .register(LongCounter.builder(METRIC_FUTURE_ITEMS_HASHED)
                        .setDescription("Future block item types hashed into a subtree"
                                + " by the forward compatibility numbering rule"))
                .getOrCreateNotLabeled();
        final Measurement futureItemsNotHashed = metricRegistry
                .register(LongCounter.builder(METRIC_FUTURE_ITEMS_NOT_HASHED)
                        .setDescription("Future block item types safely ignored as not hashed"
                                + " per the forward compatibility numbering rule"))
                .getOrCreateNotLabeled();
        final Measurement futureItemsRefused = metricRegistry
                .register(LongCounter.builder(METRIC_FUTURE_ITEMS_REFUSED)
                        .setDescription("Future block item types that caused a block to be refused"
                                + " because this version cannot process them (upgrade required)"))
                .getOrCreateNotLabeled();
        return new HashingMetrics(hashingBlockTimeNs, futureItemsHashed, futureItemsNotHashed, futureItemsRefused);
    }
}
