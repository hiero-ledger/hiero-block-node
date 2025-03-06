// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.metrics;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.hedera.block.server.util.TestConfigUtil;
import com.swirlds.config.api.Configuration;
import com.swirlds.metrics.api.Metrics;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MetricsInjectionModuleTest {

    @Test
    void testProvideMetrics() throws IOException {
        Configuration configuration = TestConfigUtil.getTestBlockNodeConfiguration();

        // Call the method under test
        Metrics providedMetrics = MetricsInjectionModule.provideMetrics(configuration);

        assertNotNull(providedMetrics);
    }
}
