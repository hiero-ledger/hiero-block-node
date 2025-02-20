// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.config;

import static org.junit.jupiter.api.Assertions.*;

import com.hedera.block.server.ServerConfig;
import com.hedera.block.server.config.logging.ConfigurationLogging;
import com.hedera.block.server.consumer.ConsumerConfig;
import com.hedera.block.server.mediator.MediatorConfig;
import com.hedera.block.server.notifier.NotifierConfig;
import com.hedera.block.server.persistence.storage.PersistenceStorageConfig;
import com.hedera.block.server.producer.ProducerConfig;
import com.hedera.block.server.util.TestConfigUtil;
import com.hedera.block.server.verification.VerificationConfig;
import com.swirlds.common.metrics.config.MetricsConfig;
import com.swirlds.common.metrics.platform.prometheus.PrometheusConfig;
import com.swirlds.config.api.Configuration;
import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConfigInjectionModuleTest {
    private BlockNodeContext testContext;

    @BeforeEach
    void setUp() throws IOException {
        testContext = TestConfigUtil.getTestBlockNodeContext();
    }

    @Test
    void testProvidePersistenceStorageConfig() {
        final Configuration configuration = testContext.configuration();
        final PersistenceStorageConfig persistenceStorageConfig =
                configuration.getConfigData(PersistenceStorageConfig.class);
        final PersistenceStorageConfig providedConfig =
                ConfigInjectionModule.providePersistenceStorageConfig(configuration);
        assertNotNull(providedConfig);
        assertSame(persistenceStorageConfig, providedConfig);
    }

    @Test
    void testProvideMetricsConfig() {
        final Configuration configuration = testContext.configuration();
        final MetricsConfig metricsConfig = configuration.getConfigData(MetricsConfig.class);
        final MetricsConfig providedConfig = ConfigInjectionModule.provideMetricsConfig(configuration);
        assertNotNull(providedConfig);
        assertSame(metricsConfig, providedConfig);
    }

    @Test
    void testProvidePrometheusConfig() {
        final Configuration configuration = testContext.configuration();
        final PrometheusConfig prometheusConfig = configuration.getConfigData(PrometheusConfig.class);
        final PrometheusConfig providedConfig = ConfigInjectionModule.providePrometheusConfig(configuration);
        assertNotNull(providedConfig);
        assertSame(prometheusConfig, providedConfig);
    }

    @Test
    void testProvideConsumerConfig() {
        final Configuration configuration = testContext.configuration();
        final ConsumerConfig consumerConfig = configuration.getConfigData(ConsumerConfig.class);
        final ConsumerConfig providedConfig = ConfigInjectionModule.provideConsumerConfig(configuration);
        assertNotNull(providedConfig);
        assertSame(consumerConfig, providedConfig);
    }

    @Test
    void testProvideMediatorConfig() {
        final Configuration configuration = testContext.configuration();
        final MediatorConfig mediatorConfig = configuration.getConfigData(MediatorConfig.class);
        final MediatorConfig providedConfig = ConfigInjectionModule.provideMediatorConfig(configuration);
        assertNotNull(providedConfig);
        assertSame(mediatorConfig, providedConfig);
    }

    @Test
    void testProvideNotifierConfig() {
        final Configuration configuration = testContext.configuration();
        final NotifierConfig notifierConfig = configuration.getConfigData(NotifierConfig.class);
        final NotifierConfig providedConfig = ConfigInjectionModule.provideNotifierConfig(configuration);
        assertNotNull(providedConfig);
        assertSame(notifierConfig, providedConfig);
    }

    @Test
    void testServerConfig() {
        final Configuration configuration = testContext.configuration();
        final ServerConfig serverConfig = configuration.getConfigData(ServerConfig.class);
        final ServerConfig providedConfig = ConfigInjectionModule.provideServerConfig(configuration);
        assertNotNull(providedConfig);
        assertSame(serverConfig, providedConfig);
    }

    @Test
    void testVerificationConfig() {
        final Configuration configuration = testContext.configuration();
        final VerificationConfig verificationConfig = configuration.getConfigData(VerificationConfig.class);
        final VerificationConfig providedConfig = ConfigInjectionModule.provideVerificationConfig(configuration);
        assertNotNull(providedConfig);
        assertSame(verificationConfig, providedConfig);
    }

    @Test
    void testProducerConfig() {
        final Configuration configuration = testContext.configuration();
        final ProducerConfig producerConfig = configuration.getConfigData(ProducerConfig.class);
        final ProducerConfig providedConfig = ConfigInjectionModule.provideProducerConfig(configuration);
        assertNotNull(providedConfig);
        assertSame(producerConfig, providedConfig);
    }

    @Test
    void testConfigurationLogging() {
        final Configuration configuration = testContext.configuration();
        final ConfigurationLogging providedConfigurationLogging =
                ConfigInjectionModule.provideConfigurationLogging(configuration);
        assertNotNull(providedConfigurationLogging);
    }
}
