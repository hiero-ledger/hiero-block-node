// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.metrics;

import com.swirlds.metrics.api.Counter;
import com.swirlds.metrics.api.LongGauge;
import edu.umd.cs.findbugs.annotations.NonNull;

/** Use member variables of this class to update metric data for the Hedera Block Node. */
public interface MetricsService {
    /**
     * Use this method to get a specific counter for the given metric type.
     *
     * @param key to get a specific counter
     * @return the counter
     */
    Counter get(@NonNull final BlockNodeMetricTypes.Counter key);

    /**
     * Use this method to get a specific gauge for the given metric type.
     *
     * @param key to get a specific gauge
     * @return the gauge
     */
    LongGauge get(@NonNull final BlockNodeMetricTypes.Gauge key);
}
