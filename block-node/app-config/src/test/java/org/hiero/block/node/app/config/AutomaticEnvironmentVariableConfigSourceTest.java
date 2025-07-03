// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import org.hiero.block.node.app.logging.ConfigLogger;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for logging.
 */
public class AutomaticEnvironmentVariableConfigSourceTest {
    /**
     * Set up the test environment by initializing the logger and adding a custom log handler so that we can capture
     * log messages.
     */
    @Test
    void testNotSettingEnv() {
        final Configuration config = ConfigurationBuilder.create()
                .autoDiscoverExtensions()
                .withSource(new AutomaticEnvironmentVariableConfigSource(
                        List.of(TestSecretConfig.class, ServerConfig.class), System::getenv))
                .withConfigDataType(ServerConfig.class)
                .build();
        assertEquals(8080, config.getConfigData(ServerConfig.class).port());
        ConfigLogger.log(config);
    }

    /**
     * Test to check that when a environment variable is set, it overrides the default value in the config class.
     */
    @Test
    void testSettingEnv() {
        // create a AutomaticEnvironmentVariableConfigSource with a modified environment variable source
        final AutomaticEnvironmentVariableConfigSource automaticEnvironmentVariableConfigSource =
                new AutomaticEnvironmentVariableConfigSource(
                        List.of(TestSecretConfig.class, ServerConfig.class),
                        varName -> "SERVER_PORT".equals(varName) ? "1234" : System.getenv(varName));
        // check that we can get the correct environment variable from the config source
        assertEquals("1234", automaticEnvironmentVariableConfigSource.getValue("server.port"));
        // check that the environment variable is not a valid property name
        assertThrows(
                NoSuchElementException.class, () -> automaticEnvironmentVariableConfigSource.getValue("SERVER_PORT"));
        // create a configuration with the modified environment variable source
        final Configuration config = ConfigurationBuilder.create()
                .withSource(automaticEnvironmentVariableConfigSource)
                .withConfigDataType(ServerConfig.class)
                .build();
        assertEquals("1234", config.getValue("server.port"));
        assertEquals(1234, config.getConfigData(ServerConfig.class).port());
    }

    /**
     * Test the simple source methods.
     */
    @Test
    void testOtherSourceMethods() {
        final AutomaticEnvironmentVariableConfigSource automaticEnvironmentVariableConfigSource =
                new AutomaticEnvironmentVariableConfigSource(
                        List.of(TestSecretConfig.class, ServerConfig.class), System::getenv);
        assertEquals(300, automaticEnvironmentVariableConfigSource.getOrdinal());
        assertEquals(
                AutomaticEnvironmentVariableConfigSource.class.getSimpleName(),
                automaticEnvironmentVariableConfigSource.getName());
        assertEquals(Collections.emptyList(), automaticEnvironmentVariableConfigSource.getListValue(""));
    }
}
