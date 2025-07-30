// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.swirlds.common.metrics.platform.prometheus.PrometheusConfig;
import com.swirlds.config.api.Configuration;
import java.io.IOException;
import java.util.Map;
import org.hiero.block.simulator.config.data.BlockGeneratorConfig;
import org.hiero.block.simulator.config.data.BlockStreamConfig;
import org.hiero.block.simulator.config.data.GrpcConfig;
import org.hiero.block.simulator.config.data.SimulatorStartupDataConfig;
import org.hiero.block.simulator.config.data.UnorderedStreamConfig;
import org.hiero.block.simulator.config.logging.ConfigurationLogging;
import org.hiero.block.simulator.fixtures.TestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The test class for the {@link ConfigInjectionModule}.
 */
class ConfigInjectionModuleTest {
    static Configuration configuration;

    @BeforeAll
    static void setUpAll() throws IOException {
        configuration = TestUtils.getTestConfiguration(
                Map.of("generator.managerImplementation", "BlockAsFileBlockStreamManager"));
    }

    /**
     * This test aims to assert that the
     * {@link ConfigInjectionModule#provideBlockStreamConfig(Configuration)}
     * returns a non-null {@link BlockStreamConfig} object and no exception is
     */
    @Test
    void provideBlockStreamConfig() {
        final BlockStreamConfig blockStreamConfig = ConfigInjectionModule.provideBlockStreamConfig(configuration);
        assertNotNull(blockStreamConfig);
        assertEquals(1000, blockStreamConfig.blockItemsBatchSize());
    }

    /**
     * This test aims to assert that the
     * {@link ConfigInjectionModule#provideGrpcConfig(Configuration)}
     * returns a non-null {@link GrpcConfig} object and no exception is thrown.
     */
    @Test
    void provideGrpcConfig() {
        final GrpcConfig grpcConfig = ConfigInjectionModule.provideGrpcConfig(configuration);
        assertNotNull(grpcConfig);
        assertEquals("localhost", grpcConfig.serverAddress());
        assertEquals(40840, grpcConfig.port());
    }

    /**
     * This test aims to assert that the
     * {@link ConfigInjectionModule#provideBlockGeneratorConfig(Configuration)}
     * returns a non-null {@link BlockGeneratorConfig} object and no exception
     * is thrown.
     */
    @Test
    void provideBlockGeneratorConfig() {
        final BlockGeneratorConfig blockGeneratorConfig =
                ConfigInjectionModule.provideBlockGeneratorConfig(configuration);
        assertNotNull(blockGeneratorConfig);
        assertEquals("BlockAsFileBlockStreamManager", blockGeneratorConfig.managerImplementation());
    }

    /**
     * This test aims to assert that the
     * {@link ConfigInjectionModule#providePrometheusConfig(Configuration)}
     * returns a non-null {@link PrometheusConfig} object and no exception is
     * thrown.
     */
    @Test
    void providePrometheusConfig() {
        final PrometheusConfig prometheusConfig = ConfigInjectionModule.providePrometheusConfig(configuration);
        assertNotNull(prometheusConfig);
        assertFalse(prometheusConfig.endpointEnabled());
        assertEquals(16007, prometheusConfig.endpointPortNumber());
    }

    /**
     * This test aims to assert that the
     * {@link ConfigInjectionModule#providesConfigurationLogging(Configuration)}
     * returns a non-null {@link ConfigurationLogging} object and no exception
     * is thrown.
     */
    @Test
    void testProvidesConfigurationLogging() {
        final ConfigurationLogging configurationLogging =
                ConfigInjectionModule.providesConfigurationLogging(configuration);
        assertNotNull(configurationLogging);
    }

    @Test
    void testProvidesSimulatorStartupDataConfig() {
        final SimulatorStartupDataConfig startupDataConfig =
                ConfigInjectionModule.providesSimulatorStartupDataConfig(configuration);
        assertNotNull(startupDataConfig);
        assertFalse(startupDataConfig.enabled());
        assertEquals(
                "/opt/simulator/data/latestAckBlockNumber",
                startupDataConfig.latestAckBlockNumberPath().toString());
        assertEquals(
                "/opt/simulator/data/latestAckBlockHash",
                startupDataConfig.latestAckBlockHashPath().toString());
    }

    /**
     * Verifies that the {@link ConfigInjectionModule#provideUnorderedStreamConfig} method
     * successfully returns a non-null {@link UnorderedStreamConfig} instance when provided
     * with a valid configuration.
     */
    @Test
    void provideUnorderedStreamConfig() {
        final UnorderedStreamConfig unorderedStreamConfig =
                ConfigInjectionModule.provideUnorderedStreamConfig(configuration);
        assertNotNull(unorderedStreamConfig);
    }
}
