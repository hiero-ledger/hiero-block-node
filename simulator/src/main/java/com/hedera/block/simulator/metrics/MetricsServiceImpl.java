// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.simulator.metrics;

import com.swirlds.metrics.api.Counter;
import com.swirlds.metrics.api.Metrics;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.EnumMap;
import javax.inject.Inject;

/**
 * Use member variables of this class to update metric data for the Hedera Block Node.
 *
 * <p>Metrics are updated by calling the appropriate method on the metric object instance. For
 * example, to increment a counter, call {@link Counter#increment()}.
 */
public class MetricsServiceImpl implements MetricsService {

    private static final String CATEGORY = "hiero_block_node_simulator";

    private final EnumMap<SimulatorMetricTypes.Counter, Counter> counters =
            new EnumMap<>(SimulatorMetricTypes.Counter.class);

    /**
     * Create singleton instance of metrics service to be used throughout the application.
     *
     * @param metrics the metrics instance
     */
    @Inject
    public MetricsServiceImpl(@NonNull final Metrics metrics) {
        // Initialize the counters
        for (SimulatorMetricTypes.Counter counter : SimulatorMetricTypes.Counter.values()) {
            counters.put(
                    counter,
                    metrics.getOrCreate(new Counter.Config(CATEGORY, counter.grafanaLabel())
                            .withDescription(counter.description())));
        }
    }

    /**
     * Use this method to get a specific counter for the given metric type.
     *
     * @param key to get a specific counter
     * @return the counter
     */
    @NonNull
    @Override
    public Counter get(@NonNull SimulatorMetricTypes.Counter key) {
        return counters.get(key);
    }
}
