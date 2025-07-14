// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.metrics;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.swirlds.config.api.Configuration;
import com.swirlds.metrics.api.Metrics;
import java.io.IOException;
import org.hiero.block.simulator.fixtures.TestUtils;
import org.junit.jupiter.api.Test;

public class MetricsInjectionModuleTest {

    @Test
    void testProvideMetrics() throws IOException {
        Configuration configuration = TestUtils.getTestConfiguration();

        // Call the method under test
        Metrics providedMetrics = MetricsInjectionModule.provideMetrics(configuration);

        assertNotNull(providedMetrics);
    }
}
