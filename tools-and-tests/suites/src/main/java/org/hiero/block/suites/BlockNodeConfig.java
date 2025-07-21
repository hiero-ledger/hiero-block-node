// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites;

/**
 * Configuration for a single Block Node instance in test suites.
 */
public class BlockNodeConfig {
    private final int port;
    private final int metricPort;

    public BlockNodeConfig(final int port, final int metricPort) {
        this.port = port;
        this.metricPort = metricPort;
    }

    public int getPort() {
        return port;
    }

    public int getMetricPort() {
        return metricPort;
    }
}
