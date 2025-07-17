// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.metrics;

import static org.hiero.block.simulator.fixtures.TestUtils.getTestMetrics;
import static org.hiero.block.simulator.metrics.SimulatorMetricTypes.Counter.LiveBlockItemsSent;
import static org.hiero.block.simulator.metrics.SimulatorMetricTypes.Counter.LiveBlocksSent;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.swirlds.config.api.Configuration;
import java.io.IOException;
import org.hiero.block.simulator.fixtures.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MetricsServiceTest {

    private MetricsService metricsService;

    @BeforeEach
    public void setUp() throws IOException {
        Configuration config = TestUtils.getTestConfiguration();
        metricsService = new MetricsServiceImpl(getTestMetrics(config));
    }

    @Test
    void MetricsService_verifyLiveBlockItemsSentCounter() {

        for (int i = 0; i < 10; i++) {
            metricsService.get(LiveBlockItemsSent).increment();
        }

        assertEquals(
                LiveBlockItemsSent.grafanaLabel(),
                metricsService.get(LiveBlockItemsSent).getName());
        assertEquals(
                LiveBlockItemsSent.description(),
                metricsService.get(LiveBlockItemsSent).getDescription());
        assertEquals(10, metricsService.get(LiveBlockItemsSent).get());
    }

    @Test
    void MetricsService_verifyLiveBlocksSentCounter() {

        for (int i = 0; i < 10; i++) {
            metricsService.get(LiveBlocksSent).increment();
        }

        assertEquals(
                LiveBlocksSent.grafanaLabel(),
                metricsService.get(LiveBlocksSent).getName());
        assertEquals(
                LiveBlocksSent.description(), metricsService.get(LiveBlocksSent).getDescription());
        assertEquals(10, metricsService.get(LiveBlocksSent).get());
    }
}
