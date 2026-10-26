// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.metrics;

import edu.umd.cs.findbugs.annotations.NonNull;
import org.hiero.metrics.LongCounter;
import org.hiero.metrics.core.Metric;
import org.hiero.metrics.core.MetricKey;

/**
 * A service for managing and retrieving metrics for the Hedera Block Node Simulator.
 */
public interface MetricsService {

    /**
     * Retrieves a metric by its key.
     *
     * @param key the metric key
     * @param <M> the type of the metric
     * @return the metric associated with the given key
     */
    @NonNull
    <M extends Metric> M getMetric(MetricKey<M> key);

    /**
     * Retrieves the value of a counter metric without labels.
     *
     * @param key the metric key for the counter
     * @return the current value of the counter
     */
    default long getCounterNoLabelsValue(MetricKey<LongCounter> key) {
        return getMetric(key).getOrCreateNotLabeled().get();
    }

    /**
     * Increments the value of a counter metric without labels by 1.
     *
     * @param key the metric key for the counter
     */
    default void incrementCounterNoLabels(MetricKey<LongCounter> key) {
        getMetric(key).getOrCreateNotLabeled().increment();
    }

    /**
     * Increments the value of a counter metric without labels by a specified amount.
     *
     * @param key         the metric key for the counter
     * @param incrementBy the amount to increment the counter by
     */
    default void incrementCounterNoLabels(MetricKey<LongCounter> key, long incrementBy) {
        getMetric(key).getOrCreateNotLabeled().increment(incrementBy);
    }
}
