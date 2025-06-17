//// SPDX-License-Identifier: Apache-2.0
//package org.hiero.block.simulator;
//
//import static org.hiero.block.simulator.TestUtils.getTestMetrics;
//import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
//import static org.junit.jupiter.api.Assertions.assertEquals;
//import static org.junit.jupiter.api.Assertions.assertFalse;
//import static org.junit.jupiter.api.Assertions.assertIterableEquals;
//import static org.junit.jupiter.api.Assertions.assertNotNull;
//import static org.junit.jupiter.api.Assertions.assertTrue;
//import static org.mockito.ArgumentMatchers.any;
//import static org.mockito.Mockito.atLeast;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.verify;
//import static org.mockito.Mockito.when;
//
//import com.hedera.hapi.block.stream.output.protoc.BlockHeader;
//import com.hedera.hapi.block.stream.protoc.Block;
//import com.hedera.hapi.block.stream.protoc.BlockItem;
//import com.swirlds.config.api.Configuration;
//import java.io.IOException;
//import java.nio.file.Paths;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//import java.util.logging.Handler;
//import java.util.logging.LogRecord;
//import java.util.logging.Logger;
//import org.hiero.block.simulator.config.data.BlockStreamConfig;
//import org.hiero.block.simulator.config.data.StreamStatus;
//import org.hiero.block.simulator.config.logging.ConfigurationLogging;
//import org.hiero.block.simulator.exception.BlockSimulatorParsingException;
//import org.hiero.block.simulator.generator.BlockStreamManager;
//import org.hiero.block.simulator.grpc.ConsumerStreamGrpcClient;
//import org.hiero.block.simulator.grpc.PublishStreamGrpcClient;
//import org.hiero.block.simulator.grpc.PublishStreamGrpcServer;
//import org.hiero.block.simulator.metrics.MetricsService;
//import org.hiero.block.simulator.metrics.MetricsServiceImpl;
//import org.hiero.block.simulator.mode.SimulatorModeHandler;
//import org.hiero.block.simulator.mode.impl.ConsumerModeHandler;
//import org.hiero.block.simulator.mode.impl.PublisherClientModeHandler;
//import org.junit.jupiter.api.AfterEach;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.extension.ExtendWith;
//import org.mockito.Mock;
//import org.mockito.junit.jupiter.MockitoExtension;
//
//@ExtendWith(MockitoExtension.class)
//class BlockStreamSimulatorTest {
//
//    @Mock
//    private BlockStreamManager blockStreamManager;
//
//    @Mock
//    private PublishStreamGrpcClient publishStreamGrpcClient;
//
//    @Mock
//    private PublishStreamGrpcServer publishStreamGrpcServer;
//
//    @Mock
//    private ConsumerStreamGrpcClient consumerStreamGrpcClient;
//
//    @Mock
//    private SimulatorModeHandler simulatorModeHandler;
//
//    @Mock
//    private ConfigurationLogging configurationLoggingMock;
//
//    private BlockStreamSimulatorApp blockStreamSimulator;
//    private MetricsService metricsService;
//
//    @BeforeEach
//    void setUp() throws IOException {
//
//        Configuration configuration = TestUtils.getTestConfiguration(
//                Map.of("blockStream.maxBlockItemsToStream", "100", "blockStream.streamingMode", "CONSTANT_RATE"));
//
//        metricsService = new MetricsServiceImpl(getTestMetrics(configuration));
//        blockStreamSimulator = new BlockStreamSimulatorApp(
//                configuration,
//                blockStreamManager,
//                publishStreamGrpcClient,
//                publishStreamGrpcServer,
//                consumerStreamGrpcClient,
//                simulatorModeHandler,
//                configurationLoggingMock);
//    }
//
//    @AfterEach
//    void tearDown() throws InterruptedException {
//        try {
//            blockStreamSimulator.stop();
//        } catch (UnsupportedOperationException e) {
//            // @todo (121) Implement consumer logic in the Simulator, which will fix this
//        }
//    }
//
//    @Test
//    void start_logsStartedMessage() throws InterruptedException, BlockSimulatorParsingException, IOException {
//        blockStreamSimulator.start();
//        assertTrue(blockStreamSimulator.isRunning());
//    }
//
//    @Test
//    void startPublishing_logsStartedMessage() throws InterruptedException, BlockSimulatorParsingException, IOException {
//        blockStreamSimulator.start();
//        assertTrue(blockStreamSimulator.isRunning());
//    }
//
//    @Test
//    void startConsuming() throws IOException, BlockSimulatorParsingException, InterruptedException {
//        SimulatorModeHandler consumerModeHandler = new ConsumerModeHandler(consumerStreamGrpcClient);
//        Configuration configuration = TestUtils.getTestConfiguration(Map.of("blockStream.simulatorMode", "CONSUMER"));
//
//        metricsService = new MetricsServiceImpl(getTestMetrics(configuration));
//        blockStreamSimulator = new BlockStreamSimulatorApp(
//                configuration,
//                blockStreamManager,
//                publishStreamGrpcClient,
//                publishStreamGrpcServer,
//                consumerStreamGrpcClient,
//                consumerModeHandler,
//                configurationLoggingMock);
//        blockStreamSimulator.start();
//
//        verify(consumerStreamGrpcClient).init();
//        verify(consumerStreamGrpcClient).requestBlocks();
//        assertTrue(blockStreamSimulator.isRunning());
//    }
//
//    @Test
//    void start_constantRateStreaming() throws InterruptedException, BlockSimulatorParsingException, IOException {
//        BlockItem blockItem = BlockItem.newBuilder()
//                .setBlockHeader(BlockHeader.newBuilder().setNumber(1L).build())
//                .build();
//
//        Block block1 = Block.newBuilder().addItems(blockItem).build();
//        Block block2 = Block.newBuilder()
//                .addItems(blockItem)
//                .addItems(blockItem)
//                .addItems(blockItem)
//                .build();
//
//        BlockStreamManager blockStreamManager = mock(BlockStreamManager.class);
//        BlockStreamConfig blockStreamConfig = mock(BlockStreamConfig.class);
//        SimulatorModeHandler publisherClientModeHandler = new PublisherClientModeHandler(
//                blockStreamConfig, publishStreamGrpcClient, blockStreamManager, metricsService);
//
//        when(blockStreamManager.getNextBlock()).thenReturn(block1, block2, null);
//        Configuration configuration = TestUtils.getTestConfiguration(Map.of(
//                "blockStream.simulatorMode",
//                "PUBLISHER_CLIENT",
//                "blockStream.maxBlockItemsToStream",
//                "2",
//                "generator.managerImplementation",
//                "BlockAsFileLargeDataSets",
//                "generator.rootPath",
//                getAbsoluteFolder("build/resources/test/block-0.0.3-blk/"),
//                "blockStream.streamingMode",
//                "CONSTANT_RATE",
//                "blockStream.blockItemsBatchSize",
//                "2"));
//
//        BlockStreamSimulatorApp blockStreamSimulator = new BlockStreamSimulatorApp(
//                configuration,
//                blockStreamManager,
//                publishStreamGrpcClient,
//                publishStreamGrpcServer,
//                consumerStreamGrpcClient,
//                publisherClientModeHandler,
//                configurationLoggingMock);
//
//        blockStreamSimulator.start();
//        assertTrue(blockStreamSimulator.isRunning());
//    }
//
//    private String getAbsoluteFolder(String relativePath) {
//        return Paths.get(relativePath).toAbsolutePath().toString();
//    }
//
//    @Test
//    void stopPublishing_doesNotThrowException() throws InterruptedException, IOException {
//        BlockStreamConfig blockStreamConfig = mock(BlockStreamConfig.class);
//        SimulatorModeHandler publisherClientModeHandler = new PublisherClientModeHandler(
//                blockStreamConfig, publishStreamGrpcClient, blockStreamManager, metricsService);
//        Configuration configuration =
//                TestUtils.getTestConfiguration(Map.of("blockStream.simulatorMode", "PUBLISHER_CLIENT"));
//
//        metricsService = new MetricsServiceImpl(getTestMetrics(configuration));
//        blockStreamSimulator = new BlockStreamSimulatorApp(
//                configuration,
//                blockStreamManager,
//                publishStreamGrpcClient,
//                publishStreamGrpcServer,
//                consumerStreamGrpcClient,
//                publisherClientModeHandler,
//                configurationLoggingMock);
//
//        assertDoesNotThrow(() -> blockStreamSimulator.stop());
//        assertFalse(blockStreamSimulator.isRunning());
//        verify(publishStreamGrpcClient, atLeast(1)).shutdown();
//    }
//
//    @Test
//    void stopConsuming_doesNotThrowException() throws InterruptedException, IOException {
//        SimulatorModeHandler consumerModeHandler = new ConsumerModeHandler(consumerStreamGrpcClient);
//        Configuration configuration = TestUtils.getTestConfiguration(Map.of("blockStream.simulatorMode", "CONSUMER"));
//
//        metricsService = new MetricsServiceImpl(getTestMetrics(configuration));
//        blockStreamSimulator = new BlockStreamSimulatorApp(
//                configuration,
//                blockStreamManager,
//                publishStreamGrpcClient,
//                publishStreamGrpcServer,
//                consumerStreamGrpcClient,
//                consumerModeHandler,
//                configurationLoggingMock);
//        assertDoesNotThrow(() -> blockStreamSimulator.stop());
//        assertFalse(blockStreamSimulator.isRunning());
//        verify(consumerStreamGrpcClient, atLeast(1)).completeStreaming();
//    }
//
//    @Test
//    void start_millisPerBlockStreaming() throws InterruptedException, IOException, BlockSimulatorParsingException {
//        BlockStreamManager blockStreamManager = mock(BlockStreamManager.class);
//        BlockStreamConfig blockStreamConfig = mock(BlockStreamConfig.class);
//        SimulatorModeHandler publisherClientModeHandler = new PublisherClientModeHandler(
//                blockStreamConfig, publishStreamGrpcClient, blockStreamManager, metricsService);
//
//        BlockItem blockItem = BlockItem.newBuilder()
//                .setBlockHeader(BlockHeader.newBuilder().setNumber(1L).build())
//                .build();
//        Block block = Block.newBuilder().addItems(blockItem).build();
//        when(blockStreamManager.getNextBlock()).thenReturn(block, block, null);
//
//        Configuration configuration = TestUtils.getTestConfiguration(Map.of(
//                "blockStream.simulatorMode",
//                "PUBLISHER_CLIENT",
//                "blockStream.maxBlockItemsToStream",
//                "2",
//                "generator.managerImplementation",
//                "BlockAsFileLargeDataSets",
//                "generator.rootPath",
//                getAbsoluteFolder("build/resources/test/block-0.0.3-blk/"),
//                "blockStream.streamingMode",
//                "MILLIS_PER_BLOCK"));
//
//        BlockStreamSimulatorApp blockStreamSimulator = new BlockStreamSimulatorApp(
//                configuration,
//                blockStreamManager,
//                publishStreamGrpcClient,
//                publishStreamGrpcServer,
//                consumerStreamGrpcClient,
//                publisherClientModeHandler,
//                configurationLoggingMock);
//
//        blockStreamSimulator.start();
//        assertTrue(blockStreamSimulator.isRunning());
//    }
//
//    @Test
//    void start_millisPerSecond_streamingLagVerifyWarnLog()
//            throws InterruptedException, IOException, BlockSimulatorParsingException {
//        BlockStreamManager blockStreamManager = mock(BlockStreamManager.class);
//        BlockItem blockItem = BlockItem.newBuilder()
//                .setBlockHeader(BlockHeader.newBuilder().setNumber(1L).build())
//                .build();
//        Block block = Block.newBuilder().addItems(blockItem).build();
//        when(blockStreamManager.getNextBlock()).thenReturn(block, block, null);
//        PublishStreamGrpcClient publishStreamGrpcClient = mock(PublishStreamGrpcClient.class);
//
//        // simulate that the first block takes 15ms to stream, when the limit is 10, to
//        // force to go
//        // over WARN Path.
//        when(publishStreamGrpcClient.streamBlock(any()))
//                .thenAnswer(invocation -> {
//                    Thread.sleep(15);
//                    return true;
//                })
//                .thenReturn(true);
//
//        Configuration configuration = TestUtils.getTestConfiguration(Map.of(
//                "blockStream.simulatorMode",
//                "PUBLISHER_CLIENT",
//                "generator.managerImplementation",
//                "BlockAsFileBlockStreamManager",
//                "generator.rootPath",
//                getAbsoluteFolder("build/resources/test/block-0.0.3-blk/"),
//                "blockStream.maxBlockItemsToStream",
//                "2",
//                "blockStream.streamingMode",
//                "MILLIS_PER_BLOCK",
//                "blockStream.millisecondsPerBlock",
//                "10",
//                "blockStream.blockItemsBatchSize",
//                "1"));
//        BlockStreamConfig blockStreamConfig = configuration.getConfigData(BlockStreamConfig.class);
//        SimulatorModeHandler publisherClientModeHandler = new PublisherClientModeHandler(
//                blockStreamConfig, publishStreamGrpcClient, blockStreamManager, metricsService);
//        BlockStreamSimulatorApp blockStreamSimulator = new BlockStreamSimulatorApp(
//                configuration,
//                blockStreamManager,
//                publishStreamGrpcClient,
//                publishStreamGrpcServer,
//                consumerStreamGrpcClient,
//                publisherClientModeHandler,
//                configurationLoggingMock);
//        List<LogRecord> logRecords = captureLogs();
//
//        blockStreamSimulator.start();
//        assertTrue(blockStreamSimulator.isRunning());
//
//        // Assert log exists
//        boolean found_log = logRecords.stream()
//                .anyMatch(logRecord -> logRecord.getMessage().contains("Block Server is running behind"));
//        assertTrue(found_log);
//    }
//
//    @Test
//    void testGetStreamStatus() {
//        long expectedPublishedBlocks = 5;
//        List<String> expectedLastKnownStatuses = List.of("Status1", "Status2");
//
//        when(publishStreamGrpcClient.getPublishedBlocks()).thenReturn(expectedPublishedBlocks);
//        when(publishStreamGrpcClient.getLastKnownStatuses()).thenReturn(expectedLastKnownStatuses);
//
//        StreamStatus streamStatus = blockStreamSimulator.getStreamStatus();
//
//        assertNotNull(streamStatus, "StreamStatus should not be null");
//        assertEquals(expectedPublishedBlocks, streamStatus.publishedBlocks(), "Published blocks should match");
//        assertIterableEquals(
//                expectedLastKnownStatuses,
//                streamStatus.lastKnownPublisherClientStatuses(),
//                "Last known statuses should match");
//        assertEquals(0, streamStatus.consumedBlocks(), "Consumed blocks should be 0 by default");
//        assertEquals(
//                0,
//                streamStatus.lastKnownConsumersStatuses().size(),
//                "Last known consumers statuses should be empty by default");
//    }
//
//    private List<LogRecord> captureLogs() {
//        // Capture logs
//        Logger logger = Logger.getLogger(PublisherClientModeHandler.class.getName());
//        final List<LogRecord> logRecords = new ArrayList<>();
//
//        // Custom handler to capture logs
//        Handler handler = new Handler() {
//            @Override
//            public void publish(LogRecord record) {
//                logRecords.add(record);
//            }
//
//            @Override
//            public void flush() {}
//
//            @Override
//            public void close() throws SecurityException {}
//        };
//
//        // Add handler to logger
//        logger.addHandler(handler);
//
//        return logRecords;
//    }
//}
