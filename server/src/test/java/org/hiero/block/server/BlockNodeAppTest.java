// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.hapi.block.BlockUnparsed;
import com.hedera.pbj.grpc.helidon.PbjRouting;
import com.hedera.pbj.grpc.helidon.config.PbjConfig;
import com.swirlds.config.api.Configuration;
import io.helidon.webserver.ConnectionConfig;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.WebServerConfig;
import io.helidon.webserver.http.HttpRouting;
import java.io.IOException;
import java.util.List;
import org.hiero.block.server.config.logging.ConfigurationLogging;
import org.hiero.block.server.consumer.ConsumerConfig;
import org.hiero.block.server.events.BlockNodeEventHandler;
import org.hiero.block.server.events.ObjectEvent;
import org.hiero.block.server.health.HealthService;
import org.hiero.block.server.mediator.LiveStreamMediator;
import org.hiero.block.server.metrics.MetricsService;
import org.hiero.block.server.notifier.Notifier;
import org.hiero.block.server.pbj.PbjBlockAccessServiceProxy;
import org.hiero.block.server.pbj.PbjBlockStreamServiceProxy;
import org.hiero.block.server.persistence.storage.read.BlockReader;
import org.hiero.block.server.producer.ProducerConfig;
import org.hiero.block.server.service.ServiceStatus;
import org.hiero.block.server.util.TestConfigUtil;
import org.hiero.block.server.verification.StreamVerificationHandlerImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BlockNodeAppTest {

    @Mock
    private ServiceStatus serviceStatus;

    @Mock
    private HealthService healthService;

    @Mock
    private WebServerConfig.Builder webServerBuilder;

    @Mock
    private WebServer webServer;

    @Mock
    private LiveStreamMediator liveStreamMediator;

    @Mock
    private BlockReader<BlockUnparsed> blockReader;

    @Mock
    private BlockNodeEventHandler<ObjectEvent<List<BlockItemUnparsed>>> blockNodeEventHandler;

    @Mock
    private StreamVerificationHandlerImpl streamVerificationHandler;

    @Mock
    private Notifier notifier;

    @Mock
    private MetricsService metricsService;

    @Mock
    private ConfigurationLogging configurationLogging;

    ServerConfig serverConfig;

    private BlockNodeApp blockNodeApp;

    @BeforeEach
    void setup() throws IOException {

        Configuration config = TestConfigUtil.getTestBlockNodeConfiguration();
        ConsumerConfig consumerConfig = config.getConfigData(ConsumerConfig.class);
        ProducerConfig producerConfig = config.getConfigData(ProducerConfig.class);

        serverConfig = new ServerConfig(4_194_304, 32_768, 32_768, 8080);

        blockNodeApp = new BlockNodeApp(
                serviceStatus,
                healthService,
                new PbjBlockStreamServiceProxy(
                        liveStreamMediator,
                        serviceStatus,
                        blockNodeEventHandler,
                        streamVerificationHandler,
                        blockReader,
                        notifier,
                        metricsService,
                        consumerConfig,
                        producerConfig),
                new PbjBlockAccessServiceProxy(serviceStatus, blockReader, metricsService),
                webServerBuilder,
                serverConfig,
                configurationLogging);

        when(webServerBuilder.port(8080)).thenReturn(webServerBuilder);
        when(webServerBuilder.addProtocol(any(PbjConfig.class))).thenReturn(webServerBuilder);
        when(webServerBuilder.addRouting(any(PbjRouting.Builder.class))).thenReturn(webServerBuilder);
        when(webServerBuilder.addRouting(any(HttpRouting.Builder.class))).thenReturn(webServerBuilder);
        when(webServerBuilder.connectionConfig(any(ConnectionConfig.class))).thenReturn(webServerBuilder);
        when(webServerBuilder.build()).thenReturn(webServer);
        when(healthService.getHealthRootPath()).thenReturn("/health");
    }

    @Test
    void testStartServer() throws IOException {
        // Act
        blockNodeApp.start();

        // Assert
        verify(serviceStatus).setWebServer(webServer);
        verify(webServer).start();
        verify(healthService).getHealthRootPath();
        verify(webServerBuilder).port(8080);
        verify(webServerBuilder).addRouting(any(PbjRouting.Builder.class));
        verify(webServerBuilder).addRouting(any(HttpRouting.Builder.class));
        verify(webServerBuilder).addProtocol(any(PbjConfig.class));
        verify(webServerBuilder).build();
    }
}
