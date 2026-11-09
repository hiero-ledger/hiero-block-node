// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.metrics;

import static org.hiero.block.simulator.fixtures.TestUtils.getTestConfiguration;
import static org.hiero.block.simulator.metrics.SimulatorMetricsProvider.LIVE_BLOCKS_SENT_KEY;
import static org.hiero.block.simulator.metrics.SimulatorMetricsProvider.LIVE_BLOCK_ITEMS_SENT_KEY;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MetricsServiceTest {

    private MetricsService metricsService;

    @BeforeEach
    public void setUp() throws IOException {
        metricsService = new MetricsServiceImpl(getTestConfiguration());
    }

    @AfterEach
    public void tearDown() throws IOException {
        metricsService.close();
    }

    @Test
    void MetricsService_verifyLiveBlockItemsSentCounter() {

        for (int i = 0; i < 10; i++) {
            metricsService.incrementCounterNoLabels(LIVE_BLOCK_ITEMS_SENT_KEY);
        }

        assertEquals(10, metricsService.getCounterNoLabelsValue(LIVE_BLOCK_ITEMS_SENT_KEY));
    }

    @Test
    void MetricsService_verifyLiveBlocksSentCounter() {

        for (int i = 0; i < 10; i++) {
            metricsService.incrementCounterNoLabels(LIVE_BLOCKS_SENT_KEY);
        }

        assertEquals(10, metricsService.getCounterNoLabelsValue(LIVE_BLOCKS_SENT_KEY));
    }

    @Test
    void MetricsService_closeIsIdempotentAndKeepsValuesReadable() throws IOException {
        metricsService.incrementCounterNoLabels(LIVE_BLOCKS_SENT_KEY);

        metricsService.close();
        assertDoesNotThrow(metricsService::close);

        assertEquals(1, metricsService.getCounterNoLabelsValue(LIVE_BLOCKS_SENT_KEY));
    }
}
