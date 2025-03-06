// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.metrics;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.swirlds.config.api.Configuration;
import com.swirlds.metrics.api.Metrics;
import java.io.IOException;
import org.hiero.block.server.config.BlockNodeContext;
import org.hiero.block.server.util.TestConfigUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MetricsInjectionModuleTest {

    @Test
    void testProvideMetrics() throws IOException {
        BlockNodeContext context = TestConfigUtil.getTestBlockNodeContext();
        Configuration configuration = context.configuration();

        // Call the method under test
        Metrics providedMetrics = MetricsInjectionModule.provideMetrics(configuration);

        assertNotNull(providedMetrics);
    }
}
