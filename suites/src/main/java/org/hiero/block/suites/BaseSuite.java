// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites;

import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.config.extensions.sources.ClasspathFileConfigSource;
import com.swirlds.config.extensions.sources.SimpleConfigSource;
import com.swirlds.config.extensions.sources.SystemEnvironmentConfigSource;
import com.swirlds.config.extensions.sources.SystemPropertiesConfigSource;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.hiero.block.api.protoc.BlockAccessServiceGrpc;
import org.hiero.block.api.protoc.BlockRequest;
import org.hiero.block.api.protoc.BlockResponse;
import org.hiero.block.simulator.BlockStreamSimulatorApp;
import org.hiero.block.simulator.BlockStreamSimulatorInjectionComponent;
import org.hiero.block.simulator.DaggerBlockStreamSimulatorInjectionComponent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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

    /** Port that is used by the Block Node Application for metrics */
    protected static int blockNodeMetricsPort;

    /** Executor service for managing threads */
    protected static ErrorLoggingExecutor executorService;

    /** gRPC channel for connecting to Block Node */
    protected static ManagedChannel channel;

    /** gRPC client stub for BlockAccessService */
    protected static BlockAccessServiceGrpc.BlockAccessServiceBlockingStub blockAccessStub;

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
     * Setup method to be executed before each test.
     *
     * <p>This method initializes the Block Node server container using Testcontainers.
     */
    @BeforeEach
    public void setup() {
        blockNodeContainer = createContainer();
        blockNodeContainer.start();
        executorService = new ErrorLoggingExecutor();
        blockAccessStub = initializeBlockAccessGrpcClient();
    }

    /**
     * Teardown method to be executed after each test.
     *
     * <p>This method stops the Block Node server container if it is running. It ensures that
     * resources are cleaned up after the test suite execution is complete.
     */
    @AfterEach
    public void teardown() throws InterruptedException {
        if (blockNodeContainer != null) {
            blockNodeContainer.stop();
            blockNodeContainer.close();
        }
        if (executorService != null) {
            executorService.shutdownNow();
        }
        if (channel != null) {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
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
        blockNodeMetricsPort = 9999;
        List<String> portBindings = new ArrayList<>();
        portBindings.add(String.format("%d:%2d", blockNodePort, blockNodePort));
        portBindings.add(String.format("%d:%2d", blockNodeMetricsPort, blockNodeMetricsPort));
        blockNodeContainer = new GenericContainer<>(DockerImageName.parse("block-node-server:" + blockNodeVersion))
                .withExposedPorts(blockNodePort)
                .withEnv("VERSION", blockNodeVersion)
                .waitingFor(Wait.forListeningPort())
                .waitingFor(Wait.forHealthcheck());
        blockNodeContainer.setPortBindings(portBindings);
        return blockNodeContainer;
    }

    /**
     * Initializes the gRPC client for connecting to the Block Node with BlockAccessStub for requesting single blocks.
     */
    protected static BlockAccessServiceGrpc.BlockAccessServiceBlockingStub initializeBlockAccessGrpcClient() {
        String host = blockNodeContainer.getHost();
        int port = blockNodePort;

        channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext() // For testing only
                .build();

        return BlockAccessServiceGrpc.newBlockingStub(channel);
    }

    /**
     * Creates a BlockRequest to retrieve a specific block.
     *
     * @param blockNumber The block number to retrieve
     * @param latest Whether to retrieve the latest block
     * @return A BlockRequest object
     */
    protected BlockRequest createBlockRequest(long blockNumber, boolean latest) {
        return BlockRequest.newBuilder()
                .setBlockNumber(blockNumber)
                .setRetrieveLatest(latest)
                .setAllowUnverified(true)
                .build();
    }

    /**
     * Retrieves a single block using the Block Node API.
     *
     * @param blockNumber The block number to retrieve
     * @param allowUnverified A flag to indicate that the requested block may be sent without
     *   verifying its `BlockProof`
     * @return The BlockResponse from the API
     */
    protected BlockResponse getBlock(final long blockNumber, final boolean allowUnverified) {
        BlockRequest request = BlockRequest.newBuilder()
                .setBlockNumber(blockNumber)
                .setAllowUnverified(allowUnverified)
                .build();
        return blockAccessStub.getBlock(request);
    }

    /**
     * Retrieves a single block using the Block Node API.
     *
     * @param allowUnverified A flag to indicate that the requested block may be sent without
     * verifying its `BlockProof`
     * @return The BlockResponse from the API
     */
    protected BlockResponse getLatestBlock(final boolean allowUnverified) {
        BlockRequest request = BlockRequest.newBuilder()
                .setBlockNumber(-1)
                .setRetrieveLatest(true)
                .setAllowUnverified(allowUnverified)
                .build();
        return blockAccessStub.getBlock(request);
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
