// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.metrics;

import static org.hiero.block.simulator.fixtures.TestUtils.getTestConfiguration;
import static org.hiero.block.simulator.metrics.SimulatorMetricTypes.Counter.LiveBlockItemsSent;
import static org.hiero.block.simulator.metrics.SimulatorMetricTypes.Counter.LiveBlocksSent;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MetricsServiceTest {

    private MetricsService metricsService;

    @BeforeEach
    public void setUp() throws IOException {
        metricsService = new MetricsServiceImpl(getTestConfiguration());
    }

    @Test
    void MetricsService_verifyLiveBlockItemsSentCounter() {

        for (int i = 0; i < 10; i++) {
            metricsService.get(LiveBlockItemsSent).increment();
        }

        assertEquals(10, metricsService.getValue(LiveBlockItemsSent));
    }

    @Test
    void MetricsService_verifyLiveBlocksSentCounter() {

        for (int i = 0; i < 10; i++) {
            metricsService.get(LiveBlocksSent).increment();
        }

        assertEquals(10, metricsService.getValue(LiveBlocksSent));
    }
}
