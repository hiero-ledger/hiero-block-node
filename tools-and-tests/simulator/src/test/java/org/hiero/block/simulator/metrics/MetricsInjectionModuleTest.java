// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.metrics;

import static org.hiero.block.simulator.fixtures.TestUtils.getTestConfiguration;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import org.junit.jupiter.api.Test;

public class MetricsInjectionModuleTest {

    @Test
    void testBindMetricsService() throws IOException {
        MetricsService metricsService = new MetricsServiceImpl(getTestConfiguration());

        assertNotNull(metricsService);
    }
}
