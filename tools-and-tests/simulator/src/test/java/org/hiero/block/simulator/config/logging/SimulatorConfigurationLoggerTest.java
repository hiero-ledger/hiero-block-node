// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.config.logging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.config.extensions.sources.ClasspathFileConfigSource;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import org.hiero.block.simulator.config.data.ConsumerConfig;
import org.junit.jupiter.api.Test;

/**
 * Tests for the {@link SimulatorConfigurationLogger} class.
 */
class SimulatorConfigurationLoggerTest {
    @Test
    void testCurrentAppProperties() throws IOException {
        final Configuration configuration = getTestConfig(Collections.emptyMap());
        final SimulatorConfigurationLogger configurationLogging = new SimulatorConfigurationLogger(configuration);
        final Map<String, Object> config = configurationLogging.collectConfig(configuration);
        assertNotNull(config);
        assertEquals(50, config.size());
        for (final Map.Entry<String, Object> entry : config.entrySet()) {
            String value = entry.getValue().toString();
            if (value.contains("*")) {
                fail(
                        "Current configuration not expected to contain any sensitive data. Change this test if we add sensitive data.");
            }
        }
    }

    @Test
    void testWithMockedSensitiveProperty() throws IOException {
        final Configuration configuration = getTestConfigWithSecret();
        final SimulatorConfigurationLogger configurationLogging = new SimulatorConfigurationLogger(configuration);
        final Map<String, Object> config = configurationLogging.collectConfig(configuration);
        assertNotNull(config);
        assertEquals(52, config.size());
        assertEquals("*****", config.get("test.secret").toString());
        assertEquals("", config.get("test.emptySecret").toString());
    }

    @Test
    public void testMaxLineLength() {
        final Map<String, Object> testMap = Map.of("key1", "valueLongerString", "key2", "value2", "key3", "value3");
        final int length = SimulatorConfigurationLogger.calculateMaxLineLength(testMap);
        assertEquals(21, length);
    }

    private static Configuration getTestConfig(@NonNull Map<String, String> customProperties) throws IOException {
        // create test configuration
        ConfigurationBuilder testConfigBuilder = ConfigurationBuilder.create()
                .autoDiscoverExtensions()
                .withSource(new ClasspathFileConfigSource(Path.of("app.properties")));

        for (final Map.Entry<String, String> entry : customProperties.entrySet()) {
            final String key = entry.getKey();
            final String value = entry.getValue();
            testConfigBuilder = testConfigBuilder.withValue(key, value);
        }
        testConfigBuilder = testConfigBuilder.withConfigDataType(ConsumerConfig.class);
        return testConfigBuilder.build();
    }

    private static Configuration getTestConfigWithSecret() throws IOException {
        return ConfigurationBuilder.create()
                .autoDiscoverExtensions()
                .withSource(new ClasspathFileConfigSource(Path.of("app.properties")))
                .withConfigDataType(TestSecretConfig.class)
                .build();
    }
}
