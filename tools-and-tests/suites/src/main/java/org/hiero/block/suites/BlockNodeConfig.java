// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites;

/**
 * Configuration for a single Block Node instance in test suites.
 */
public class BlockNodeConfig {
    private final int port;
    private final String range;
    private final int metricPort;

    public BlockNodeConfig(int port, String range, int metricPort) {
        this.port = port;
        this.range = range;
        this.metricPort = metricPort;
    }

    public int getPort() {
        return port;
    }

    public String getRange() {
        return range;
    }

    public int getMetricPort() {
        return metricPort;
    }
}
