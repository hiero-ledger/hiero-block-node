// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.config.extensions.sources.ClasspathFileConfigSource;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.hiero.block.node.app.ServerConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@link ConfigurationLogging} class.
 */
public class ConfigurationLoggingImplTest {
    /** The custom log handler used to capture log messages. */
    private TestLogHandler logHandler;

    /**
     * Set up the test environment by initializing the logger and adding a custom log handler so that we can capture
     * log messages.
     */
    @BeforeEach
    void setUp() {
        final Logger logger = java.util.logging.Logger.getLogger(ConfigurationLogging.class.getName());
        System.out.println("logger = " + logger);
        logHandler = new TestLogHandler();
        logger.addHandler(logHandler);
        logger.setLevel(java.util.logging.Level.ALL);
    }

    /**
     * Test to verify that a log message is captured in testing
     */
    @Test
    public void testCurrentAppProperties() throws IOException {
        final Configuration configuration = getTestConfig(Collections.emptyMap());
        ConfigurationLogging.log(configuration);
        final String logMessages = logHandler.getLogMessages();
        System.out.println("logHandler = " + logHandler.getLogMessages());

        final Map<String, String> config = ConfigurationLogging.collectConfig(configuration);
        assertNotNull(config);
        for (Map.Entry<String, String> entry : config.entrySet()) {
            final String loggedProperty = entry.getKey() + "=" + entry.getValue();
            // check that entry was logged
            assertTrue(logMessages.contains(loggedProperty), "Log message should be captured: " + loggedProperty);
            if (entry.getValue().contains("*")) {
                fail("Current configuration not expected to contain any sensitive data. "
                        + "Change this test if we add sensitive data.");
            }
        }
    }

    /**
     * Test to verify that secret properties are redacted in log messages
     */
    @Test
    public void testWithMockedSensitiveProperty() throws IOException {
        final Configuration configuration = getTestConfigWithSecret();
        ConfigurationLogging.log(configuration);
        final String logMessages = logHandler.getLogMessages();
        System.out.println("logHandler = " + logMessages);
        final Map<String, String> config = ConfigurationLogging.collectConfig(configuration);
        assertNotNull(config);
        assertEquals("*****", config.get("test.secret"));
        assertEquals("", config.get("test.emptySecret"));
        assertTrue(logMessages.contains("test.secret=*****"), "Log message should be captured: \"test.secret=*****\"");
        assertTrue(
                logMessages.contains("test.emptySecret=\n"), "Log message should be captured: \"test.emptySecret=\"");
    }

    /**
     * Build a test configuration with the given custom properties as well as those loaded from "app.properties". The
     * configuration has a single data type of ServerConfig.
     *
     * @param customProperties the custom properties to add to the configuration
     * @return the test configuration
     * @throws IOException if an error occurs while loading the configuration
     */
    private static Configuration getTestConfig(@NonNull Map<String, String> customProperties) throws IOException {
        // create test configuration
        ConfigurationBuilder testConfigBuilder =
                ConfigurationBuilder.create().withSource(new ClasspathFileConfigSource(Path.of("app.properties")));
        for (Map.Entry<String, String> entry : customProperties.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            testConfigBuilder = testConfigBuilder.withValue(key, value);
        }
        testConfigBuilder = testConfigBuilder.withConfigDataType(ServerConfig.class);
        return testConfigBuilder.build();
    }

    /**
     * Build a test configuration with properties loaded from "app.properties". The only data type is TestSecretConfig.
     *
     * @return the test configuration
     * @throws IOException if an error occurs while loading the configuration
     */
    private static Configuration getTestConfigWithSecret() throws IOException {
        ConfigurationBuilder testConfigBuilder = ConfigurationBuilder.create()
                .autoDiscoverExtensions()
                .withSource(new ClasspathFileConfigSource(Path.of("app.properties")));
        testConfigBuilder = testConfigBuilder.withConfigDataType(TestSecretConfig.class);
        return testConfigBuilder.build();
    }

    /**
     * Custom log handler to capture log messages for testing.
     */
    private static class TestLogHandler extends Handler {
        private final StringBuilder logMessages = new StringBuilder();

        @Override
        public void publish(LogRecord record) {
            logMessages.append(record.getMessage()).append("\n");
            System.out.println("############ Log message: " + record.getMessage());
        }

        @Override
        public void flush() {
            // No-op
        }

        @Override
        public void close() throws SecurityException {
            // No-op
        }

        public String getLogMessages() {
            return logMessages.toString();
        }
    }
}
