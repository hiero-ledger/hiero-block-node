// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.config.api.ConfigurationExtension;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Stream;
import org.hiero.block.node.spi.ServiceLoaderFunction;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for logging.
 */
public class AutomaticEnvironmentVariableConfigSourceTest {
    /** The config types that {@link #testServiceLoader()} reports as available, standing in for what would normally
     * be discovered from every module's {@link ConfigurationExtension}. */
    private static final Set<Class<? extends Record>> TEST_CONFIG_TYPES =
            Set.of(TestSecretConfig.class, ServerConfig.class);

    /**
     * A {@link ServiceLoaderFunction} that reports a single {@link ConfigurationExtension} exposing
     * {@link #TEST_CONFIG_TYPES}, so tests do not depend on what is actually registered via {@code provides} on the
     * real module path.
     */
    private static ServiceLoaderFunction testServiceLoader() {
        return new ServiceLoaderFunction() {
            @SuppressWarnings("unchecked")
            @Override
            public <C> Stream<? extends C> loadServices(Class<C> serviceClass) {
                if (serviceClass == ConfigurationExtension.class) {
                    final ConfigurationExtension extension = new ConfigurationExtension() {
                        @Override
                        public Set<Class<? extends Record>> getConfigDataTypes() {
                            return TEST_CONFIG_TYPES;
                        }
                    };
                    return Stream.of(extension).map(e -> (C) e);
                }
                return Stream.empty();
            }
        };
    }

    /**
     * Set up the test environment by initializing the logger and adding a custom log handler so that we can capture
     * log messages.
     */
    @Test
    void testNotSettingEnv() {
        final Configuration config = ConfigurationBuilder.create()
                .withSource(new AutomaticEnvironmentVariableConfigSource(testServiceLoader(), System::getenv))
                .withConfigDataType(ServerConfig.class)
                .build();
        assertEquals(40840, config.getConfigData(ServerConfig.class).port());
    }

    /**
     * Test to check that when a environment variable is set, it overrides the default value in the config class.
     */
    @Test
    void testSettingEnv() {
        // create a AutomaticEnvironmentVariableConfigSource with a modified environment variable source
        final AutomaticEnvironmentVariableConfigSource automaticEnvironmentVariableConfigSource =
                new AutomaticEnvironmentVariableConfigSource(
                        testServiceLoader(),
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
                new AutomaticEnvironmentVariableConfigSource(testServiceLoader(), System::getenv);
        assertEquals(300, automaticEnvironmentVariableConfigSource.getOrdinal());
        assertEquals(
                AutomaticEnvironmentVariableConfigSource.class.getSimpleName(),
                automaticEnvironmentVariableConfigSource.getName());
        assertEquals(Collections.emptyList(), automaticEnvironmentVariableConfigSource.getListValue(""));
    }
}
