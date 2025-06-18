//// SPDX-License-Identifier: Apache-2.0
//package org.hiero.block.simulator.grpc.impl;
//
//import static org.hiero.block.simulator.TestUtils.findFreePort;
//import static org.hiero.block.simulator.TestUtils.getTestMetrics;
//import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
//import static org.junit.jupiter.api.Assertions.assertEquals;
//import static org.junit.jupiter.api.Assertions.assertThrows;
//import static org.junit.jupiter.api.Assertions.assertTrue;
//import static org.mockito.Mockito.when;
//
//import com.hedera.hapi.block.stream.output.protoc.BlockHeader;
//import com.hedera.hapi.block.stream.protoc.Block;
//import com.hedera.hapi.block.stream.protoc.BlockItem;
//import com.hedera.hapi.block.stream.protoc.BlockProof;
//import com.hedera.hapi.platform.event.legacy.EventTransaction;
//import com.swirlds.config.api.Configuration;
//import io.grpc.Server;
//import io.grpc.ServerBuilder;
//import io.grpc.stub.StreamObserver;
//import java.io.IOException;
//import java.util.List;
//import java.util.concurrent.atomic.AtomicBoolean;
//import org.hiero.block.api.protoc.BlockItemSet;
//import org.hiero.block.api.protoc.BlockStreamPublishServiceGrpc;
//import org.hiero.block.api.protoc.PublishStreamRequest;
//import org.hiero.block.api.protoc.PublishStreamResponse;
//import org.hiero.block.api.protoc.PublishStreamResponse.EndOfStream.Code;
//import org.hiero.block.simulator.TestUtils;
//import org.hiero.block.simulator.config.data.BlockStreamConfig;
//import org.hiero.block.simulator.config.data.GrpcConfig;
//import org.hiero.block.simulator.config.types.MidBlockFailType;
//import org.hiero.block.simulator.grpc.PublishStreamGrpcClient;
//import org.hiero.block.simulator.metrics.MetricsService;
//import org.hiero.block.simulator.metrics.MetricsServiceImpl;
//import org.hiero.block.simulator.startup.SimulatorStartupData;
//import org.junit.jupiter.api.AfterEach;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import org.mockito.Mock;
//import org.mockito.MockitoAnnotations;
//
//class PublishStreamGrpcClientImplTest {
//    private MetricsService metricsService;
//    private PublishStreamGrpcClient publishStreamGrpcClient;
//    private boolean isShutdownCalled = false;
//
//    @Mock
//    private GrpcConfig grpcConfig;
//
//    @Mock
//    private SimulatorStartupData startupDataMock;
//
//    private BlockStreamConfig blockStreamConfig;
//    private AtomicBoolean streamEnabled;
//    private Server server;
//
//    @BeforeEach
//    void setUp() throws IOException {
//        MockitoAnnotations.openMocks(this);
//
//        streamEnabled = new AtomicBoolean(true);
//
//        final int serverPort = findFreePort();
//        server = ServerBuilder.forPort(serverPort)
//                .addService(new BlockStreamPublishServiceGrpc.BlockStreamPublishServiceImplBase() {
//                    @Override
//                    public StreamObserver<PublishStreamRequest> publishBlockStream(
//                            StreamObserver<PublishStreamResponse> responseObserver) {
//                        return new StreamObserver<>() {
//                            private long lastBlockNumber = 0;
//
//                            @Override
//                            public void onNext(PublishStreamRequest request) {
//                                BlockItemSet blockItems = request.getBlockItems();
//                                List<BlockItem> items = blockItems.getBlockItemsList();
//                                // Simulate processing of block items
//                                for (BlockItem item : items) {
//                                    // Assume that the first BlockItem is a BlockHeader
//                                    if (item.hasBlockHeader()) {
//                                        lastBlockNumber = item.getBlockHeader().getNumber();
//                                    }
//                                    // Assume that the last BlockItem is a BlockProof
//                                    if (item.hasBlockProof()) {
//                                        // Send BlockAcknowledgement
//                                        PublishStreamResponse.BlockAcknowledgement acknowledgement =
//                                                PublishStreamResponse.BlockAcknowledgement.newBuilder()
//                                                        .setBlockNumber(lastBlockNumber)
//                                                        .build();
//                                        responseObserver.onNext(PublishStreamResponse.newBuilder()
//                                                .setAcknowledgement(acknowledgement)
//                                                .build());
//                                    }
//                                }
//                            }
//
//                            @Override
//                            public void onError(Throwable t) {
//                                // handle onError
//                            }
//
//                            @Override
//                            public void onCompleted() {
//                                PublishStreamResponse.EndOfStream endOfStream =
//                                        PublishStreamResponse.EndOfStream.newBuilder()
//                                                .setStatus(Code.SUCCESS)
//                                                .setBlockNumber(lastBlockNumber)
//                                                .build();
//                                responseObserver.onNext(PublishStreamResponse.newBuilder()
//                                        .setEndStream(endOfStream)
//                                        .build());
//                                responseObserver.onCompleted();
//                            }
//                        };
//                    }
//                })
//                .build()
//                .start();
//        blockStreamConfig = BlockStreamConfig.builder().blockItemsBatchSize(2).build();
//
//        Configuration config = TestUtils.getTestConfiguration();
//        metricsService = new MetricsServiceImpl(getTestMetrics(config));
//        streamEnabled = new AtomicBoolean(true);
//
//        when(grpcConfig.serverAddress()).thenReturn("localhost");
//        when(grpcConfig.port()).thenReturn(serverPort);
//
//        publishStreamGrpcClient = new PublishStreamGrpcClientImpl(
//                grpcConfig, blockStreamConfig, metricsService, streamEnabled, startupDataMock);
//    }
//
//    @AfterEach
//    void teardown() throws InterruptedException {
//        if (!isShutdownCalled) {
//            publishStreamGrpcClient.shutdown();
//        }
//
//        if (server != null) {
//            server.shutdown();
//        }
//    }
//
//    @Test
//    public void testInit() {
//        publishStreamGrpcClient.init();
//        // Verify that lastKnownStatuses is cleared
//        assertTrue(publishStreamGrpcClient.getLastKnownStatuses().isEmpty());
//    }
//
//    @Test
//    void testStreamBlock_Success() throws InterruptedException {
//        publishStreamGrpcClient.init();
//        final int streamedBlocks = 3;
//
//        for (int i = 0; i < streamedBlocks; i++) {
//            BlockItem blockItemHeader = BlockItem.newBuilder()
//                    .setBlockHeader(BlockHeader.newBuilder().setNumber(i).build())
//                    .build();
//            BlockItem blockItemProof = BlockItem.newBuilder()
//                    .setBlockProof(BlockProof.newBuilder().setBlock(i).build())
//                    .build();
//            Block block = Block.newBuilder()
//                    .addItems(blockItemHeader)
//                    .addItems(blockItemProof)
//                    .build();
//
//            final boolean result = publishStreamGrpcClient.streamBlock(block);
//            assertTrue(result);
//        }
//
//        // we use simple retry mechanism here, because sometimes server takes some time to receive the stream
//        long retryNumber = 1;
//        long waitTime = 500;
//
//        while (retryNumber < 3) {
//            if (!publishStreamGrpcClient.getLastKnownStatuses().isEmpty()) {
//                break;
//            }
//            Thread.sleep(retryNumber * waitTime);
//            retryNumber++;
//        }
//
//        assertEquals(streamedBlocks, publishStreamGrpcClient.getPublishedBlocks());
//        assertEquals(
//                streamedBlocks, publishStreamGrpcClient.getLastKnownStatuses().size());
//    }
//
//    @Test
//    void testStreamBlock_RejectsAfterShutdown() throws InterruptedException {
//        publishStreamGrpcClient.init();
//        final int streamedBlocks = 3;
//
//        for (int i = 0; i < streamedBlocks; i++) {
//            final Block block = constructBlock(i, false);
//            final boolean result = publishStreamGrpcClient.streamBlock(block);
//            assertTrue(result);
//        }
//
//        // we use simple retry mechanism here, because sometimes server takes some time to receive the stream
//        long retryNumber = 1;
//        long waitTime = 500;
//
//        while (retryNumber < 3) {
//            if (!publishStreamGrpcClient.getLastKnownStatuses().isEmpty()) {
//                break;
//            }
//            Thread.sleep(retryNumber * waitTime);
//            retryNumber++;
//        }
//
//        assertEquals(streamedBlocks, publishStreamGrpcClient.getPublishedBlocks());
//        assertEquals(
//                streamedBlocks, publishStreamGrpcClient.getLastKnownStatuses().size());
//        publishStreamGrpcClient.shutdown();
//        isShutdownCalled = true;
//
//        final Block block = constructBlock(0, false);
//        IllegalStateException exception =
//                assertThrows(IllegalStateException.class, () -> publishStreamGrpcClient.streamBlock(block));
//        assertEquals("Stream is already completed, no further calls are allowed", exception.getMessage());
//    }
//
//    @Test
//    public void handleMidBlockFailIfSetNone() {
//        blockStreamConfig = BlockStreamConfig.builder()
//                .midBlockFailType(MidBlockFailType.NONE)
//                .midBlockFailOffset(0L)
//                .build();
//        publishStreamGrpcClient = new PublishStreamGrpcClientImpl(
//                grpcConfig, blockStreamConfig, metricsService, streamEnabled, startupDataMock);
//
//        publishStreamGrpcClient.init();
//        assertDoesNotThrow(() -> publishStreamGrpcClient.streamBlock(constructBlock(0, true)));
//        assertDoesNotThrow(() -> publishStreamGrpcClient.streamBlock(constructBlock(1, true)));
//    }
//
//    @Test
//    public void handleMidBlockFailIfSetAbrupt() {
//        blockStreamConfig = BlockStreamConfig.builder()
//                .midBlockFailType(MidBlockFailType.ABRUPT)
//                .midBlockFailOffset(1L)
//                .build();
//        publishStreamGrpcClient = new PublishStreamGrpcClientImpl(
//                grpcConfig, blockStreamConfig, metricsService, streamEnabled, startupDataMock);
//
//        publishStreamGrpcClient.init();
//        assertDoesNotThrow(() -> publishStreamGrpcClient.streamBlock(constructBlock(0, true)));
//        RuntimeException ex = assertThrows(
//                RuntimeException.class, () -> publishStreamGrpcClient.streamBlock(constructBlock(1, true)));
//        assertEquals("Configured abrupt disconnection occurred", ex.getMessage());
//    }
//
//    @Test
//    public void handleMidBlockFailIfSetEos() {
//        blockStreamConfig = BlockStreamConfig.builder()
//                .midBlockFailType(MidBlockFailType.EOS)
//                .midBlockFailOffset(1L)
//                .build();
//        publishStreamGrpcClient = new PublishStreamGrpcClientImpl(
//                grpcConfig, blockStreamConfig, metricsService, streamEnabled, startupDataMock);
//
//        publishStreamGrpcClient.init();
//        assertDoesNotThrow(() -> publishStreamGrpcClient.streamBlock(constructBlock(0, true)));
//        assertThrows(IllegalStateException.class, () -> publishStreamGrpcClient.streamBlock(constructBlock(1, true)));
//        isShutdownCalled = true; // to avoid calling onCompleted after onError
//    }
//
//    @Test
//    public void handleMidBlockFailIfSetStreamingBatchTooSmall() {
//        blockStreamConfig = BlockStreamConfig.builder()
//                .midBlockFailType(MidBlockFailType.ABRUPT)
//                .midBlockFailOffset(0L)
//                .build();
//        publishStreamGrpcClient = new PublishStreamGrpcClientImpl(
//                grpcConfig, blockStreamConfig, metricsService, streamEnabled, startupDataMock);
//        publishStreamGrpcClient.init();
//        assertDoesNotThrow(() -> publishStreamGrpcClient.streamBlock(constructBlock(1, false)));
//    }
//
//    private Block constructBlock(long number, boolean withItems) {
//        BlockItem blockItemHeader = BlockItem.newBuilder()
//                .setBlockHeader(BlockHeader.newBuilder().setNumber(number).build())
//                .build();
//        BlockItem blockItemProof = BlockItem.newBuilder()
//                .setBlockProof(BlockProof.newBuilder().setBlock(number).build())
//                .build();
//        if (withItems) {
//            BlockItem blockItemEventTransaction1 = BlockItem.newBuilder()
//                    .setEventTransaction(EventTransaction.newBuilder().build())
//                    .build();
//            BlockItem blockItemEventTransaction2 = BlockItem.newBuilder()
//                    .setEventTransaction(EventTransaction.newBuilder().build())
//                    .build();
//            return Block.newBuilder()
//                    .addItems(blockItemHeader)
//                    .addItems(blockItemEventTransaction1)
//                    .addItems(blockItemEventTransaction2)
//                    .addItems(blockItemProof)
//                    .build();
//        } else {
//            return Block.newBuilder()
//                    .addItems(blockItemHeader)
//                    .addItems(blockItemProof)
//                    .build();
//        }
//    }
//}
