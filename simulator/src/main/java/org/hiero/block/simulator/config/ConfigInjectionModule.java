// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.config;

import com.swirlds.common.metrics.platform.prometheus.PrometheusConfig;
import com.swirlds.config.api.Configuration;
import dagger.Module;
import dagger.Provides;
import javax.inject.Singleton;
import org.hiero.block.simulator.config.data.BlockGeneratorConfig;
import org.hiero.block.simulator.config.data.BlockStreamConfig;
import org.hiero.block.simulator.config.data.ConsumerConfig;
import org.hiero.block.simulator.config.data.GrpcConfig;
import org.hiero.block.simulator.config.data.SimulatorStartupDataConfig;
import org.hiero.block.simulator.config.logging.ConfigurationLogging;
import org.hiero.block.simulator.config.logging.SimulatorConfigurationLogger;

/** The module used to inject the configuration data into the application. */
@Module
public interface ConfigInjectionModule {

    /**
     * Provides the block stream configuration.
     *
     * @param configuration the configuration to be used by the block stream
     * @return the block stream configuration
     */
    @Singleton
    @Provides
    static BlockStreamConfig provideBlockStreamConfig(final Configuration configuration) {
        return configuration.getConfigData(BlockStreamConfig.class);
    }

    /**
     * Provides the consumer configuration.
     *
     * @param configuration the configuration to be used by the block consumer
     * @return the block consumer configuration
     */
    @Singleton
    @Provides
    static ConsumerConfig provideConsumerConfig(final Configuration configuration) {
        return configuration.getConfigData(ConsumerConfig.class);
    }

    /**
     * Provides the gRPC configuration.
     *
     * @param configuration the configuration to be used by the gRPC
     * @return the gRPC configuration
     */
    @Singleton
    @Provides
    static GrpcConfig provideGrpcConfig(final Configuration configuration) {
        return configuration.getConfigData(GrpcConfig.class);
    }

    /**
     * Provides the block generator configuration.
     *
     * @param configuration the configuration to be used by the block generator
     * @return the block generator configuration
     */
    @Singleton
    @Provides
    static BlockGeneratorConfig provideBlockGeneratorConfig(final Configuration configuration) {
        return configuration.getConfigData(BlockGeneratorConfig.class);
    }

    /**
     * Provides a Prometheus configuration singleton using the configuration.
     *
     * @param configuration is the configuration singleton
     * @return a Prometheus configuration singleton
     */
    @Singleton
    @Provides
    static PrometheusConfig providePrometheusConfig(final Configuration configuration) {
        return configuration.getConfigData(PrometheusConfig.class);
    }

    @Singleton
    @Provides
    static ConfigurationLogging providesConfigurationLogging(final Configuration configuration) {
        return new SimulatorConfigurationLogger(configuration);
    }

    @Singleton
    @Provides
    static SimulatorStartupDataConfig providesSimulatorStartupDataConfig(final Configuration configuration) {
        return configuration.getConfigData(SimulatorStartupDataConfig.class);
    }
}
