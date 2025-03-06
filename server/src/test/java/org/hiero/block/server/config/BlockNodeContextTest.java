// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.config;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.swirlds.config.api.Configuration;
import org.hiero.block.server.metrics.MetricsService;
import org.hiero.block.server.metrics.MetricsServiceImpl;
import org.junit.jupiter.api.Test;

class BlockNodeContextTest {

    @Test
    void BlockNodeContext_initializesWithMetricsAndConfiguration() {
        Configuration configuration = mock(Configuration.class);
        MetricsService metricsService = mock(MetricsServiceImpl.class);

        BlockNodeContext context = new BlockNodeContext(metricsService, configuration);

        assertEquals(metricsService, context.metricsService());
        assertEquals(configuration, context.configuration());
    }
}
