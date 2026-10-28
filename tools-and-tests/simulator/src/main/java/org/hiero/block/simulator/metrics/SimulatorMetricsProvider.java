// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.metrics;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Collection;
import java.util.List;
import org.hiero.metrics.LongCounter;
import org.hiero.metrics.core.Metric;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricsRegistrationProvider;

/// Provides the metrics to be registered for the Block Stream Simulator.
///
/// This class implements the [MetricsRegistrationProvider] interface, allowing it to
/// supply a collection of metrics that should be registered with the metrics system. The metrics
/// provided here are specific to the simulator's operation, including counters for live block items
/// sent, live blocks sent, live blocks processed by the server, and live blocks consumed.
public class SimulatorMetricsProvider implements MetricsRegistrationProvider {

    private static final String CATEGORY = "blocknode_simulator";

    /// The number of live block items sent by the simulator
    public static final MetricKey<LongCounter> LIVE_BLOCK_ITEMS_SENT_KEY =
            LongCounter.key("live_block_items_sent").addCategory(CATEGORY);
    /// The number of live blocks sent by the simulator
    public static final MetricKey<LongCounter> LIVE_BLOCKS_SENT_KEY =
            LongCounter.key("live_blocks_sent").addCategory(CATEGORY);
    ///The number of live blocks processed by the simulator publish server
    public static final MetricKey<LongCounter> LIVE_BLOCKS_PROCESSED_KEY =
            LongCounter.key("live_blocks_processed_by_server").addCategory(CATEGORY);
    /// The number of live blocks consumed by the simulator
    public static final MetricKey<LongCounter> LIVE_BLOCKS_CONSUMED_KEY =
            LongCounter.key("live_blocks_consumed").addCategory(CATEGORY);

    @Override
    public @NonNull Collection<Metric.Builder<?, ?>> getMetricsToRegister() {
        return List.of(
                LongCounter.builder(LIVE_BLOCK_ITEMS_SENT_KEY).setDescription("Live Block Items Sent"),
                LongCounter.builder(LIVE_BLOCKS_SENT_KEY).setDescription("Live Blocks Sent"),
                LongCounter.builder(LIVE_BLOCKS_PROCESSED_KEY)
                        .setDescription("Live Blocks Processed by Publish Server"),
                LongCounter.builder(LIVE_BLOCKS_CONSUMED_KEY).setDescription("Live Blocks Consumed"));
    }
}
