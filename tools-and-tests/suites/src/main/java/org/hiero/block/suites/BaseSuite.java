// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.hiero.block.api.protoc.BlockAccessServiceGrpc;
import org.hiero.block.api.protoc.BlockNodeServiceGrpc;
import org.hiero.block.simulator.BlockStreamSimulatorApp;
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

    /** gRPC client stub for BlockNodeService */
    protected static BlockNodeServiceGrpc.BlockNodeServiceBlockingStub blockServiceStub;

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
        blockServiceStub = initializeBlockNodeServiceGrpcClient();
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

    protected static BlockNodeServiceGrpc.BlockNodeServiceBlockingStub initializeBlockNodeServiceGrpcClient() {
        final String host = blockNodeContainer.getHost();
        final int port = blockNodePort;
        channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        return BlockNodeServiceGrpc.newBlockingStub(channel);
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
