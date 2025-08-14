// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.hiero.block.api.protoc.BlockAccessServiceGrpc;
import org.hiero.block.api.protoc.BlockNodeServiceGrpc;
import org.hiero.block.simulator.BlockStreamSimulatorApp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
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

    /** List of containers running Block Node Applications */
    protected List<GenericContainer<?>> blockNodeContainers = new ArrayList<>();

    /** Port that is used by the Block Node Application */
    protected static int blockNodePort;

    /** Port that is used by the Block Node Application for metrics */
    protected static int blockNodeMetricsPort;

    /** Executor service for managing threads */
    protected static ErrorLoggingExecutor executorService;

    /** gRPC channel for connecting to Block Node */
    protected static ManagedChannel channel;

    /** Map from port to gRPC channel */
    protected Map<Integer, ManagedChannel> channels = new LinkedHashMap<>();

    /** gRPC client stub for BlockAccessService */
    protected static BlockAccessServiceGrpc.BlockAccessServiceBlockingStub blockAccessStub;

    /** gRPC client stub for BlockNodeService */
    protected static BlockNodeServiceGrpc.BlockNodeServiceBlockingStub blockServiceStub;

    /** Map from port to BlockAccessService stub */
    protected Map<Integer, BlockAccessServiceGrpc.BlockAccessServiceBlockingStub> blockAccessStubs =
            new LinkedHashMap<>();
    /** Map from port to BlockNodeService stub */
    protected Map<Integer, BlockNodeServiceGrpc.BlockNodeServiceBlockingStub> blockServiceStubs = new LinkedHashMap<>();

    private static final String PORT_BINDING_FORMAT = "%d:%d";

    private static Network network;

    /**
     * Default constructor for the BaseSuite class.
     *
     * <p>This constructor can be used by subclasses or the testing framework to initialize the
     * BaseSuite. It does not perform any additional setup.
     */
    public BaseSuite() {
        network = Network.newNetwork();
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
     * Teardown all Block Node containers and channels.
     */
    protected void teardownBlockNodes() throws InterruptedException {
        for (GenericContainer<?> container : blockNodeContainers) {
            container.stop();
            container.close();
        }
        blockNodeContainers.clear();

        for (ManagedChannel ch : channels.values()) {
            ch.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
        channels.clear();
        blockAccessStubs.clear();
        blockServiceStubs.clear();
    }

    /**
     * Launch multiple Block Node containers with the given configs.
     * Each BN will be started on its own port.
     * @param configs List of BlockNodeConfig specifying port and metricPort for each BN
     */
    protected void launchBlockNodes(List<BlockNodeContainerConfig> configs) {
        for (BlockNodeContainerConfig config : configs) {
            GenericContainer<?> container = createContainer(config);
            container.start();
            blockNodeContainers.add(container);
            ManagedChannel ch = ManagedChannelBuilder.forAddress(container.getHost(), config.port())
                    .usePlaintext()
                    .build();
            channels.put(config.port(), ch);
            blockAccessStubs.put(config.port(), BlockAccessServiceGrpc.newBlockingStub(ch));
            blockServiceStubs.put(config.port(), BlockNodeServiceGrpc.newBlockingStub(ch));
        }
    }

    protected void deleteBlocks(int blockNode, long blocks) throws IOException, InterruptedException {
        GenericContainer<?> genericContainer = blockNodeContainers.get(blockNode);

        for (long block = 0; block < blocks; block++) {
            String blockFileName =
                    String.format("/opt/hiero/block-node/data/live/000/000/000/000/000/0/%019d.blk.zstd", block);
            genericContainer.execInContainer("rm", "-f", blockFileName);
        }
    }

    protected void restartBlockNode(int blockNode) {
        GenericContainer<?> genericContainer = blockNodeContainers.get(blockNode);
        genericContainer
                .getDockerClient()
                .restartContainerCmd(genericContainer.getContainerId())
                .exec();
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
     *   <li>Exposing the default gRPC port (40840).
     *   <li>Using the Testcontainers health check mechanism to ensure the container is ready.
     * </ul>
     *
     * @return a configured {@link GenericContainer} instance for the Block Node server
     */
    protected static GenericContainer<?> createContainer() {
        String blockNodeVersion = BaseSuite.getBlockNodeVersion();
        blockNodePort = 40840;
        blockNodeMetricsPort = 16007;
        List<String> portBindings = new ArrayList<>();
        portBindings.add(String.format("%d:%2d", blockNodePort, blockNodePort));
        portBindings.add(String.format("%d:%2d", blockNodeMetricsPort, blockNodeMetricsPort));
        blockNodeContainer = new GenericContainer<>(DockerImageName.parse("block-node-server:" + blockNodeVersion))
                .withExposedPorts(blockNodePort)
                .withNetworkAliases("block-node-source")
                .withNetwork(network)
                .withEnv("VERSION", blockNodeVersion)
                .waitingFor(Wait.forListeningPort())
                .waitingFor(Wait.forHealthcheck());
        blockNodeContainer.setPortBindings(portBindings);
        return blockNodeContainer;
    }

    protected static GenericContainer<?> createContainer(BlockNodeContainerConfig config) {
        String blockNodeVersion = BaseSuite.getBlockNodeVersion();
        int blockNodePort = config.port();
        int blockNodeMetricsPort = config.metricPort();
        List<String> portBindings = new ArrayList<>();
        portBindings.add(PORT_BINDING_FORMAT.formatted(blockNodePort, blockNodePort));
        portBindings.add(PORT_BINDING_FORMAT.formatted(blockNodeMetricsPort, blockNodeMetricsPort));

        GenericContainer<?> container = new GenericContainer<>(
                        DockerImageName.parse("block-node-server:" + blockNodeVersion))
                .withExposedPorts(blockNodePort)
                .withNetwork(network)
                .withEnv("VERSION", blockNodeVersion)
                .withEnv("BACKFILL_BLOCK_NODE_SOURCES_PATH", config.backfillSourcePath())
                .withEnv("BACKFILL_INITIAL_DELAY", "5")
                .withEnv("SERVER_PORT", String.valueOf(blockNodePort))
                .withFileSystemBind(
                        Paths.get("src/main/resources/block-nodes.json")
                                .toAbsolutePath()
                                .toString(),
                        "/resources/block-nodes.json")
                .waitingFor(Wait.forListeningPort());

        container.setPortBindings(portBindings);
        return container;
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
