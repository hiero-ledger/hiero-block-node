// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.metrics;

import com.swirlds.metrics.api.Counter;
import edu.umd.cs.findbugs.annotations.NonNull;

/** Use member variables of this class to update metric data for the Hedera Block Node. */
public interface MetricsService {
    /**
     * Use this method to get a specific counter for the given metric type.
     *
     * @param key to get a specific counter
     * @return the counter
     */
    Counter get(@NonNull SimulatorMetricTypes.Counter key);
}
