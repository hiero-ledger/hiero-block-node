// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.metrics;

import edu.umd.cs.findbugs.annotations.NonNull;
import org.hiero.metrics.LongCounter;

/** Use member variables of this class to update metric data for the Hedera Block Node. */
public interface MetricsService {
    /**
     * Use this method to get a specific counter for the given metric type.
     *
     * @param key to get a specific counter
     * @return the counter
     */
    LongCounter.Measurement get(@NonNull SimulatorMetricTypes.Counter key);

    long getValue(@NonNull SimulatorMetricTypes.Counter key);
}
