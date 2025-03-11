// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.metrics;

import com.swirlds.metrics.api.Counter;
import com.swirlds.metrics.api.LongGauge;
import com.swirlds.metrics.api.Metrics;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.EnumMap;
import java.util.Objects;
import javax.inject.Inject;

/**
 * Use member variables of this class to update metric data for the Hedera Block Node.
 *
 * <p>Metrics are updated by calling the appropriate method on the metric object instance. For
 * example, to increment a counter, call {@link Counter#increment()}.
 */
public final class MetricsServiceImpl implements MetricsService {
    private static final String CATEGORY = "hiero_block_node";
    private final EnumMap<BlockNodeMetricTypes.Counter, Counter> counters =
            new EnumMap<>(BlockNodeMetricTypes.Counter.class);
    private final EnumMap<BlockNodeMetricTypes.Gauge, LongGauge> gauges =
            new EnumMap<>(BlockNodeMetricTypes.Gauge.class);

    /**
     * Create singleton instance of metrics service to be used throughout the application.
     *
     * @param metrics the metrics instance
     */
    @Inject
    public MetricsServiceImpl(@NonNull final Metrics metrics) {
        Objects.requireNonNull(metrics);
        // Initialize the counters
        for (final BlockNodeMetricTypes.Counter counter : BlockNodeMetricTypes.Counter.values()) {
            counters.put(
                    counter,
                    metrics.getOrCreate(new Counter.Config(CATEGORY, counter.grafanaLabel())
                            .withDescription(counter.description())));
        }

        // Initialize the gauges
        for (final BlockNodeMetricTypes.Gauge gauge : BlockNodeMetricTypes.Gauge.values()) {
            gauges.put(
                    gauge,
                    metrics.getOrCreate(
                            new LongGauge.Config(CATEGORY, gauge.grafanaLabel()).withDescription(gauge.description())));
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
    public Counter get(@NonNull final BlockNodeMetricTypes.Counter key) {
        return counters.get(Objects.requireNonNull(key));
    }

    /**
     * Use this method to get a specific gauge for the given metric type.
     *
     * @param key to get a specific gauge
     * @return the gauge
     */
    @NonNull
    @Override
    public LongGauge get(@NonNull final BlockNodeMetricTypes.Gauge key) {
        return gauges.get(Objects.requireNonNull(key));
    }
}
