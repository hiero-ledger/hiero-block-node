// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server;

import com.hedera.hapi.block.SubscribeStreamResponse;
import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.swirlds.config.api.Configuration;
import io.helidon.webserver.WebServerConfig;
import java.io.IOException;
import org.hiero.block.server.config.BlockNodeContext;
import org.hiero.block.server.events.ObjectEvent;
import org.hiero.block.server.mediator.StreamMediator;
import org.hiero.block.server.metrics.MetricsService;
import org.hiero.block.server.persistence.storage.read.BlockReader;
import org.hiero.block.server.service.ServiceStatus;
import org.hiero.block.server.util.TestConfigUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

class BlockNodeAppInjectionModuleTest {

    @Mock
    private StreamMediator<BlockItem, ObjectEvent<SubscribeStreamResponse>> streamMediator;

    @Mock
    private BlockReader<Block> blockReader;

    @Mock
    private ServiceStatus serviceStatus;

    private BlockNodeContext blockNodeContext;

    @BeforeEach
    void setUp() throws IOException {
        blockNodeContext = TestConfigUtil.getTestBlockNodeContext();
    }

    @Test
    void testProvideBlockNodeContext() {
        Configuration configuration = blockNodeContext.configuration();
        MetricsService metricsService = blockNodeContext.metricsService();

        BlockNodeContext providedBlockNodeContext =
                BlockNodeAppInjectionModule.provideBlockNodeContext(configuration, metricsService);

        Assertions.assertEquals(blockNodeContext, providedBlockNodeContext);
        Assertions.assertEquals(blockNodeContext.configuration(), providedBlockNodeContext.configuration());
        Assertions.assertEquals(blockNodeContext.metricsService(), providedBlockNodeContext.metricsService());
    }

    @Test
    void testProvideWebServerConfigBuilder() {
        WebServerConfig.Builder webServerConfigBuilder = BlockNodeAppInjectionModule.provideWebServerConfigBuilder();

        Assertions.assertNotNull(webServerConfigBuilder);
    }
}
