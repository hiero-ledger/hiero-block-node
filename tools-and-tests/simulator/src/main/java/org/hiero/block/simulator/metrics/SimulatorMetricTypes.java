// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.metrics;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * The BlockNodeMetricNames class contains the names of the metrics used by the BlockNode.
 *
 * <p>These names are used to register the metrics with the metrics service.
 */
public final class SimulatorMetricTypes {
    private SimulatorMetricTypes() {}

    /**
     * Add new counting metrics to this enum to automatically register them with the metrics
     * service.
     *
     * <p>Each enum value should have a unique grafana label and meaningful description. These
     * counters can capture data on standard operations or errors.
     */
    public enum Counter implements SimulatorMetricMetadata {
        // Standard counters
        /** The number of live block items sent by the simulator . */
        LiveBlockItemsSent("live_block_items_sent", "Live Block Items Sent"),
        /** The number of live blocks sent by the simulator */
        LiveBlocksSent("live_blocks_sent", "Live Blocks Sent"),
        /** The number of live blocks processed by the simulator publish server */
        LiveBlocksProcessed("live_blocks_processed_by_server", "Live Blocks Processed by Publish Server"),
        /** The number of live blocks consumed by the simulator */
        LiveBlocksConsumed("live_blocks_consumed", "Live Blocks Consumed");

        private final String grafanaLabel;
        private final String description;

        Counter(String grafanaLabel, String description) {
            this.grafanaLabel = grafanaLabel;
            this.description = description;
        }

        @Override
        @NonNull
        public String grafanaLabel() {
            return grafanaLabel;
        }

        @Override
        @NonNull
        public String description() {
            return description;
        }
    }

    private interface SimulatorMetricMetadata {
        String grafanaLabel();

        String description();
    }
}
