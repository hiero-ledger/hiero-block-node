// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.metrics;

import static org.hiero.block.server.metrics.BlockNodeMetricTypes.Counter.BlocksPersisted;
import static org.hiero.block.server.metrics.BlockNodeMetricTypes.Counter.LiveBlockItems;
import static org.hiero.block.server.metrics.BlockNodeMetricTypes.Counter.LiveBlockItemsConsumed;
import static org.hiero.block.server.metrics.BlockNodeMetricTypes.Counter.SingleBlocksRetrieved;
import static org.hiero.block.server.metrics.BlockNodeMetricTypes.Gauge.Consumers;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.swirlds.config.api.Configuration;
import dagger.BindsInstance;
import dagger.Component;
import java.io.IOException;
import javax.inject.Singleton;
import org.hiero.block.server.config.BlockNodeContext;
import org.hiero.block.server.util.TestConfigUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MetricsServiceTest {

    @Singleton
    @Component(modules = {MetricsInjectionModule.class})
    public interface MetricsServiceTestComponent {

        MetricsService getMetricsService();

        @Component.Factory
        interface Factory {
            MetricsServiceTestComponent create(@BindsInstance Configuration configuration);
        }
    }

    private MetricsService metricsService;

    @BeforeEach
    public void setUp() throws IOException {
        final BlockNodeContext context = TestConfigUtil.getTestBlockNodeContext();
        final Configuration configuration = context.configuration();
        final MetricsServiceTestComponent testComponent =
                DaggerMetricsServiceTest_MetricsServiceTestComponent.factory().create(configuration);
        this.metricsService = testComponent.getMetricsService();
    }

    @Test
    void MetricsService_verifyLiveBlockItemsCounter() {

        for (int i = 0; i < 10; i++) {
            metricsService.get(LiveBlockItems).increment();
        }

        assertEquals(
                LiveBlockItems.grafanaLabel(),
                metricsService.get(LiveBlockItems).getName());
        assertEquals(
                LiveBlockItems.description(), metricsService.get(LiveBlockItems).getDescription());
        assertEquals(10, metricsService.get(LiveBlockItems).get());
    }

    @Test
    void MetricsService_verifyBlocksPersistedCounter() {

        for (int i = 0; i < 10; i++) {
            metricsService.get(BlocksPersisted).increment();
        }

        assertEquals(
                BlocksPersisted.grafanaLabel(),
                metricsService.get(BlocksPersisted).getName());
        assertEquals(
                BlocksPersisted.description(),
                metricsService.get(BlocksPersisted).getDescription());
        assertEquals(10, metricsService.get(BlocksPersisted).get());
    }

    @Test
    void MetricsService_verifySingleBlocksRetrievedCounter() {

        for (int i = 0; i < 10; i++) {
            metricsService.get(SingleBlocksRetrieved).increment();
        }

        assertEquals(
                SingleBlocksRetrieved.grafanaLabel(),
                metricsService.get(SingleBlocksRetrieved).getName());
        assertEquals(
                SingleBlocksRetrieved.description(),
                metricsService.get(SingleBlocksRetrieved).getDescription());
        assertEquals(10, metricsService.get(SingleBlocksRetrieved).get());
    }

    @Test
    void MetricsService_verifyLiveBlockItemsConsumedCounter() {

        for (int i = 0; i < 10; i++) {
            metricsService.get(LiveBlockItemsConsumed).increment();
        }

        assertEquals(
                LiveBlockItemsConsumed.grafanaLabel(),
                metricsService.get(LiveBlockItemsConsumed).getName());
        assertEquals(
                LiveBlockItemsConsumed.description(),
                metricsService.get(LiveBlockItemsConsumed).getDescription());
        assertEquals(10, metricsService.get(LiveBlockItemsConsumed).get());
    }

    @Test
    void MetricsService_verifySubscribersGauge() {

        assertEquals(Consumers.grafanaLabel(), metricsService.get(Consumers).getName());
        assertEquals(Consumers.description(), metricsService.get(Consumers).getDescription());

        // Set the subscribers to various values and verify
        metricsService.get(Consumers).set(10);
        assertEquals(10, metricsService.get(Consumers).get());

        metricsService.get(Consumers).set(3);
        assertEquals(3, metricsService.get(Consumers).get());

        metricsService.get(Consumers).set(0);
        assertEquals(0, metricsService.get(Consumers).get());
    }
}
