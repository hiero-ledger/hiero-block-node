// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.config;

import static org.junit.jupiter.api.Assertions.*;

import com.swirlds.common.metrics.config.MetricsConfig;
import com.swirlds.common.metrics.platform.prometheus.PrometheusConfig;
import com.swirlds.config.api.Configuration;
import java.io.IOException;
import org.hiero.block.server.ServerConfig;
import org.hiero.block.server.config.logging.ConfigurationLogging;
import org.hiero.block.server.consumer.ConsumerConfig;
import org.hiero.block.server.mediator.MediatorConfig;
import org.hiero.block.server.notifier.NotifierConfig;
import org.hiero.block.server.persistence.storage.PersistenceStorageConfig;
import org.hiero.block.server.producer.ProducerConfig;
import org.hiero.block.server.util.TestConfigUtil;
import org.hiero.block.server.verification.VerificationConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConfigInjectionModuleTest {
    private Configuration configuration;

    @BeforeEach
    void setUp() throws IOException {
        configuration = TestConfigUtil.getTestBlockNodeConfiguration();
    }

    @Test
    void testProvidePersistenceStorageConfig() {
        final PersistenceStorageConfig persistenceStorageConfig =
                configuration.getConfigData(PersistenceStorageConfig.class);
        final PersistenceStorageConfig providedConfig =
                ConfigInjectionModule.providePersistenceStorageConfig(configuration);
        assertNotNull(providedConfig);
        assertSame(persistenceStorageConfig, providedConfig);
    }

    @Test
    void testProvideMetricsConfig() {
        final MetricsConfig metricsConfig = configuration.getConfigData(MetricsConfig.class);
        final MetricsConfig providedConfig = ConfigInjectionModule.provideMetricsConfig(configuration);
        assertNotNull(providedConfig);
        assertSame(metricsConfig, providedConfig);
    }

    @Test
    void testProvidePrometheusConfig() {
        final PrometheusConfig prometheusConfig = configuration.getConfigData(PrometheusConfig.class);
        final PrometheusConfig providedConfig = ConfigInjectionModule.providePrometheusConfig(configuration);
        assertNotNull(providedConfig);
        assertSame(prometheusConfig, providedConfig);
    }

    @Test
    void testProvideConsumerConfig() {
        final ConsumerConfig consumerConfig = configuration.getConfigData(ConsumerConfig.class);
        final ConsumerConfig providedConfig = ConfigInjectionModule.provideConsumerConfig(configuration);
        assertNotNull(providedConfig);
        assertSame(consumerConfig, providedConfig);
    }

    @Test
    void testProvideMediatorConfig() {
        final MediatorConfig mediatorConfig = configuration.getConfigData(MediatorConfig.class);
        final MediatorConfig providedConfig = ConfigInjectionModule.provideMediatorConfig(configuration);
        assertNotNull(providedConfig);
        assertSame(mediatorConfig, providedConfig);
    }

    @Test
    void testProvideNotifierConfig() {
        final NotifierConfig notifierConfig = configuration.getConfigData(NotifierConfig.class);
        final NotifierConfig providedConfig = ConfigInjectionModule.provideNotifierConfig(configuration);
        assertNotNull(providedConfig);
        assertSame(notifierConfig, providedConfig);
    }

    @Test
    void testServerConfig() {
        final ServerConfig serverConfig = configuration.getConfigData(ServerConfig.class);
        final ServerConfig providedConfig = ConfigInjectionModule.provideServerConfig(configuration);
        assertNotNull(providedConfig);
        assertSame(serverConfig, providedConfig);
    }

    @Test
    void testVerificationConfig() {
        final VerificationConfig verificationConfig = configuration.getConfigData(VerificationConfig.class);
        final VerificationConfig providedConfig = ConfigInjectionModule.provideVerificationConfig(configuration);
        assertNotNull(providedConfig);
        assertSame(verificationConfig, providedConfig);
    }

    @Test
    void testProducerConfig() {
        final ProducerConfig producerConfig = configuration.getConfigData(ProducerConfig.class);
        final ProducerConfig providedConfig = ConfigInjectionModule.provideProducerConfig(configuration);
        assertNotNull(providedConfig);
        assertSame(producerConfig, providedConfig);
    }

    @Test
    void testConfigurationLogging() {
        final ConfigurationLogging providedConfigurationLogging =
                ConfigInjectionModule.provideConfigurationLogging(configuration);
        assertNotNull(providedConfigurationLogging);
    }
}
