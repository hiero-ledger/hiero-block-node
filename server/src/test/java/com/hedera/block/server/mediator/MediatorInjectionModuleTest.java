// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.mediator;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.hedera.block.server.metrics.MetricsService;
import com.hedera.block.server.service.ServiceStatus;
import com.hedera.block.server.util.TestConfigUtil;
import com.hedera.hapi.block.BlockItemUnparsed;
import com.swirlds.config.api.Configuration;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MediatorInjectionModuleTest {

    @Mock
    private ServiceStatus serviceStatus;

    @BeforeEach
    void setup() {
        // Any setup before each test can be done here
    }

    @Test
    void testProvidesStreamMediator() throws IOException {

        Configuration configuration = TestConfigUtil.getTestBlockNodeConfiguration();
        MetricsService metricsService = TestConfigUtil.getTestBlockNodeMetricsService();

        // Call the method under test
        StreamMediator<List<BlockItemUnparsed>, List<BlockItemUnparsed>> streamMediator =
                MediatorInjectionModule.providesLiveStreamMediator(
                        configuration.getConfigData(MediatorConfig.class), metricsService, serviceStatus);

        // Verify that the streamMediator is correctly instantiated
        assertNotNull(streamMediator);
        assertInstanceOf(LiveStreamMediatorImpl.class, streamMediator);
    }

    @Test
    void testNoOpProvidesStreamMediator() throws IOException {

        Map<String, String> properties = Map.of("mediator.type", MediatorConfig.MediatorType.NO_OP.toString());
        Configuration configuration = TestConfigUtil.getTestBlockNodeConfiguration(properties);
        MetricsService metricsService = TestConfigUtil.getTestBlockNodeMetricsService(configuration);

        // Call the method under test
        StreamMediator<List<BlockItemUnparsed>, List<BlockItemUnparsed>> streamMediator =
                MediatorInjectionModule.providesLiveStreamMediator(
                        configuration.getConfigData(MediatorConfig.class), metricsService, serviceStatus);

        // Verify that the streamMediator is correctly instantiated
        assertNotNull(streamMediator);
        assertInstanceOf(NoOpLiveStreamMediator.class, streamMediator);
    }
}
