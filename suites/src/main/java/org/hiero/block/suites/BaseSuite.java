// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites;

import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.config.extensions.sources.ClasspathFileConfigSource;
import com.swirlds.config.extensions.sources.SimpleConfigSource;
import com.swirlds.config.extensions.sources.SystemEnvironmentConfigSource;
import com.swirlds.config.extensions.sources.SystemPropertiesConfigSource;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import org.hiero.block.simulator.BlockStreamSimulatorApp;
import org.hiero.block.simulator.BlockStreamSimulatorInjectionComponent;
import org.hiero.block.simulator.DaggerBlockStreamSimulatorInjectionComponent;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * BaseSuite is an abstract class that provides common setup and teardown functionality for test
 * suites using Testcontainers to manage a Docker container for the Block Node server.
 *
 * <p>This class is responsible for:
 *
 * <ul>
 *   <li>Starting a Docker container running the Block Node Application with a specified version.
 *   <li>Stopping the container after tests have been executed.
 * </ul>
 *
 * <p>The Block Node Application version is retrieved from the system property 'block.node.version'.
 */
public abstract class BaseSuite {
    /** Container running the Block Node Application */
    protected static GenericContainer<?> blockNodeContainer;

    /** Port that is used by the Block Node Application */
    protected static int blockNodePort;

    /** Executor service for managing threads */
    protected static ErrorLoggingExecutor executorService;

    /**
     * Default constructor for the BaseSuite class.
     *
     * <p>This constructor can be used by subclasses or the testing framework to initialize the
     * BaseSuite. It does not perform any additional setup.
     */
    public BaseSuite() {
        // No additional setup required
    }

    /**
     * Setup method to be executed before all tests.
     *
     * <p>This method initializes the Block Node server container using Testcontainers.
     */
    @BeforeAll
    public static void setup() {
        blockNodeContainer = createContainer();
        blockNodeContainer.start();
        executorService = new ErrorLoggingExecutor();
    }

    /**
     * Teardown method to be executed after all tests.
     *
     * <p>This method stops the Block Node server container if it is running. It ensures that
     * resources are cleaned up after the test suite execution is complete.
     */
    @AfterAll
    public static void teardown() {
        if (blockNodeContainer != null) {
            blockNodeContainer.stop();
            blockNodeContainer.close();
        }
        if (executorService != null) {
            executorService.shutdownNow();
        }
    }

    /**
     * Initialize container with the default configuration and returns it.
     *
     * <p>This method initializes the Block Node container with the version retrieved from the .env
     * file. It configures the container and returns it.
     *
     * <p>Specific configuration steps include:
     *
     * <ul>
     *   <li>Setting the environment variable "VERSION" from the .env file.
     *   <li>Exposing the default gRPC port (8080).
     *   <li>Using the Testcontainers health check mechanism to ensure the container is ready.
     * </ul>
     *
     * @return a configured {@link GenericContainer} instance for the Block Node server
     */
    protected static GenericContainer<?> createContainer() {
        String blockNodeVersion = BaseSuite.getBlockNodeVersion();
        blockNodePort = 8080;
        List<String> portBindings = new ArrayList<>();
        portBindings.add(String.format("%d:%2d", blockNodePort, blockNodePort));
        blockNodeContainer = new GenericContainer<>(DockerImageName.parse("block-node-server:" + blockNodeVersion))
                .withExposedPorts(blockNodePort)
                .withEnv("VERSION", blockNodeVersion)
                .waitingFor(Wait.forListeningPort())
                .waitingFor(Wait.forHealthcheck());
        blockNodeContainer.setPortBindings(portBindings);
        return blockNodeContainer;
    }

    /**
     * Starts the block stream simulator in a separate thread.
     *
     * @param blockStreamSimulatorAppInstance the block stream simulator app instance
     * @return a {@link Future} representing the asynchronous execution of the block stream simulator
     */
    protected Future<?> startSimulatorInThread(BlockStreamSimulatorApp blockStreamSimulatorAppInstance) {
        return executorService.submit(() -> {
            try {
                blockStreamSimulatorAppInstance.start();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Creates a new instance of the block stream simulator with custom configuration.
     *
     * @param customConfiguration the custom configuration which will be applied to simulator upon startup
     * @return a new instance of the block stream simulator
     * @throws IOException if an I/O error occurs
     */
    protected BlockStreamSimulatorApp createBlockSimulator(@NonNull final Map<String, String> customConfiguration)
            throws IOException {
        BlockStreamSimulatorInjectionComponent DIComponent = DaggerBlockStreamSimulatorInjectionComponent.factory()
                .create(loadSimulatorConfiguration(customConfiguration));
        return DIComponent.getBlockStreamSimulatorApp();
    }

    /**
     * Creates a new instance of the block stream simulator with default configuration.
     *
     * @return a new instance of the block stream simulator
     * @throws IOException if an I/O error occurs
     */
    protected BlockStreamSimulatorApp createBlockSimulator() throws IOException {
        BlockStreamSimulatorInjectionComponent DIComponent = DaggerBlockStreamSimulatorInjectionComponent.factory()
                .create(loadSimulatorConfiguration(Collections.emptyMap()));
        return DIComponent.getBlockStreamSimulatorApp();
    }

    /**
     * Builds the desired block simulator configuration
     *
     * @return block simulator configuration
     * @throws IOException if an I/O error occurs
     */
    protected static Configuration loadSimulatorConfiguration(@NonNull final Map<String, String> customProperties)
            throws IOException {
        final ConfigurationBuilder configurationBuilder = ConfigurationBuilder.create()
                .withSource(SystemEnvironmentConfigSource.getInstance())
                .withSource(SystemPropertiesConfigSource.getInstance())
                .withSource(new ClasspathFileConfigSource(Path.of("app.properties")))
                .autoDiscoverExtensions();

        for (Map.Entry<String, String> entry : customProperties.entrySet()) {
            final String key = entry.getKey();
            final String value = entry.getValue();
            configurationBuilder.withSource(new SimpleConfigSource(key, value).withOrdinal(500));
        }

        return configurationBuilder.build();
    }

    /**
     * Retrieves the Block Node server version from the system property.
     *
     * @return the version of the Block Node server as a string
     */
    private static String getBlockNodeVersion() {
        String version = System.getProperty("block.node.version");
        if (version == null) {
            throw new IllegalStateException(
                    "block.node.version system property is not set. This should be set by Gradle.");
        }
        return version;
    }
}
