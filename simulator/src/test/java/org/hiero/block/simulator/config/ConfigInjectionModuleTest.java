// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.swirlds.common.metrics.platform.prometheus.PrometheusConfig;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.config.extensions.sources.ClasspathFileConfigSource;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import org.hiero.block.simulator.TestUtils;
import org.hiero.block.simulator.config.data.BlockGeneratorConfig;
import org.hiero.block.simulator.config.data.BlockStreamConfig;
import org.hiero.block.simulator.config.data.GrpcConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ConfigInjectionModuleTest {

    static Configuration configuration;

    @BeforeAll
    static void setUpAll() throws IOException {
        configuration = ConfigurationBuilder.create()
                .withSource(new ClasspathFileConfigSource(Path.of("app.properties")))
                .autoDiscoverExtensions()
                .build();
        configuration = TestUtils.getTestConfiguration(
                Map.of("generator.managerImplementation", "BlockAsFileBlockStreamManager"));
    }

    @Test
    void provideBlockStreamConfig() {

        BlockStreamConfig blockStreamConfig = ConfigInjectionModule.provideBlockStreamConfig(configuration);

        assertNotNull(blockStreamConfig);
        assertEquals(1000, blockStreamConfig.blockItemsBatchSize());
    }

    @Test
    void provideGrpcConfig() {
        GrpcConfig grpcConfig = ConfigInjectionModule.provideGrpcConfig(configuration);

        assertNotNull(grpcConfig);
        assertEquals("localhost", grpcConfig.serverAddress());
        assertEquals(8080, grpcConfig.port());
    }

    @Test
    void provideBlockGeneratorConfig() {
        BlockGeneratorConfig blockGeneratorConfig = ConfigInjectionModule.provideBlockGeneratorConfig(configuration);

        assertNotNull(blockGeneratorConfig);
        assertEquals("BlockAsFileBlockStreamManager", blockGeneratorConfig.managerImplementation());
    }

    @Test
    void providePrometheusConfig() {
        PrometheusConfig prometheusConfig = ConfigInjectionModule.providePrometheusConfig(configuration);

        assertNotNull(prometheusConfig);
        assertFalse(prometheusConfig.endpointEnabled());
        assertEquals(9998, prometheusConfig.endpointPortNumber());
    }
}
