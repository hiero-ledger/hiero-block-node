// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.grpc.impl;

import static org.hiero.block.simulator.TestUtils.findFreePort;
import static org.hiero.block.simulator.TestUtils.getTestMetrics;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.hedera.hapi.block.protoc.BlockItemSet;
import com.hedera.hapi.block.protoc.BlockStreamServiceGrpc;
import com.hedera.hapi.block.protoc.PublishStreamRequest;
import com.hedera.hapi.block.protoc.PublishStreamResponse;
import com.hedera.hapi.block.protoc.PublishStreamResponseCode;
import com.hedera.hapi.block.stream.output.protoc.BlockHeader;
import com.hedera.hapi.block.stream.protoc.Block;
import com.hedera.hapi.block.stream.protoc.BlockItem;
import com.hedera.hapi.block.stream.protoc.BlockProof;
import com.swirlds.config.api.Configuration;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.hiero.block.simulator.TestUtils;
import org.hiero.block.simulator.config.data.BlockStreamConfig;
import org.hiero.block.simulator.config.data.GrpcConfig;
import org.hiero.block.simulator.config.types.RecoveryMode;
import org.hiero.block.simulator.generator.BlockStreamManager;
import org.hiero.block.simulator.grpc.PublishStreamGrpcClient;
import org.hiero.block.simulator.metrics.MetricsService;
import org.hiero.block.simulator.metrics.MetricsServiceImpl;
import org.hiero.block.simulator.startup.SimulatorStartupData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class PublishStreamGrpcClientImplTest {
    private MetricsService metricsService;
    private PublishStreamGrpcClient publishStreamGrpcClient;
    private boolean isShutdownCalled = false;

    @Mock
    private GrpcConfig grpcConfig;

    @Mock
    private SimulatorStartupData startupDataMock;

    @Mock
    private BlockStreamManager blockStreamManagerMock;

    private BlockStreamConfig blockStreamConfig;
    private AtomicBoolean streamEnabled;
    private Server server;

    @BeforeEach
    void setUp() throws IOException {
        MockitoAnnotations.openMocks(this);

        streamEnabled = new AtomicBoolean(true);

        final int serverPort = findFreePort();
        server = ServerBuilder.forPort(serverPort)
                .addService(new BlockStreamServiceGrpc.BlockStreamServiceImplBase() {
                    @Override
                    public StreamObserver<PublishStreamRequest> publishBlockStream(
                            StreamObserver<PublishStreamResponse> responseObserver) {
                        return new StreamObserver<>() {
                            private long lastBlockNumber = 0;

                            @Override
                            public void onNext(PublishStreamRequest request) {
                                BlockItemSet blockItems = request.getBlockItems();
                                List<BlockItem> items = blockItems.getBlockItemsList();
                                // Simulate processing of block items
                                for (BlockItem item : items) {
                                    // Assume that the first BlockItem is a BlockHeader
                                    if (item.hasBlockHeader()) {
                                        lastBlockNumber = item.getBlockHeader().getNumber();
                                    }
                                    // Assume that the last BlockItem is a BlockProof
                                    if (item.hasBlockProof()) {
                                        // Send BlockAcknowledgement
                                        PublishStreamResponse.Acknowledgement acknowledgement =
                                                PublishStreamResponse.Acknowledgement.newBuilder()
                                                        .setBlockAck(
                                                                PublishStreamResponse.BlockAcknowledgement.newBuilder()
                                                                        .setBlockNumber(lastBlockNumber)
                                                                        .build())
                                                        .build();
                                        responseObserver.onNext(PublishStreamResponse.newBuilder()
                                                .setAcknowledgement(acknowledgement)
                                                .build());
                                    }
                                }
                            }

                            @Override
                            public void onError(Throwable t) {
                                // handle onError
                            }

                            @Override
                            public void onCompleted() {
                                PublishStreamResponse.EndOfStream endOfStream =
                                        PublishStreamResponse.EndOfStream.newBuilder()
                                                .setStatus(PublishStreamResponseCode.STREAM_ITEMS_SUCCESS)
                                                .setBlockNumber(lastBlockNumber)
                                                .build();
                                responseObserver.onNext(PublishStreamResponse.newBuilder()
                                        .setStatus(endOfStream)
                                        .build());
                                responseObserver.onCompleted();
                            }
                        };
                    }
                })
                .build()
                .start();
        blockStreamConfig = BlockStreamConfig.builder()
                .recoveryMode(RecoveryMode.RESEND_LAST)
                .blockItemsBatchSize(2)
                .build();

        Configuration config = TestUtils.getTestConfiguration();
        metricsService = new MetricsServiceImpl(getTestMetrics(config));
        streamEnabled = new AtomicBoolean(true);

        when(grpcConfig.serverAddress()).thenReturn("localhost");
        when(grpcConfig.port()).thenReturn(serverPort);

        publishStreamGrpcClient = new PublishStreamGrpcClientImpl(
                grpcConfig, blockStreamConfig, metricsService, streamEnabled, startupDataMock, blockStreamManagerMock);
    }

    @AfterEach
    void teardown() throws InterruptedException {
        if (!isShutdownCalled) {
            publishStreamGrpcClient.shutdown();
        }

        if (server != null) {
            server.shutdown();
        }
    }

    @Test
    public void testInit() {
        publishStreamGrpcClient.init();
        // Verify that lastKnownStatuses is cleared
        assertTrue(publishStreamGrpcClient.getLastKnownStatuses().isEmpty());
    }

    @Test
    void testStreamBlockItem_Success() {
        publishStreamGrpcClient.init();

        BlockItem blockItem = BlockItem.newBuilder()
                .setBlockHeader(BlockHeader.newBuilder().setNumber(0).build())
                .build();

        List<BlockItem> blockItems = List.of(blockItem);

        final boolean result = publishStreamGrpcClient.streamBlockItem(blockItems);
        assertTrue(result);
    }

    @Test
    void testStreamBlock_Success() throws InterruptedException {
        publishStreamGrpcClient.init();
        final int streamedBlocks = 3;

        for (int i = 0; i < streamedBlocks; i++) {
            BlockItem blockItemHeader = BlockItem.newBuilder()
                    .setBlockHeader(BlockHeader.newBuilder().setNumber(i).build())
                    .build();
            BlockItem blockItemProof = BlockItem.newBuilder()
                    .setBlockProof(BlockProof.newBuilder().setBlock(i).build())
                    .build();
            Block block = Block.newBuilder()
                    .addItems(blockItemHeader)
                    .addItems(blockItemProof)
                    .build();

            final boolean result = publishStreamGrpcClient.streamBlock(block);
            assertTrue(result);
        }

        // we use simple retry mechanism here, because sometimes server takes some time to receive the stream
        long retryNumber = 1;
        long waitTime = 500;

        while (retryNumber < 3) {
            if (!publishStreamGrpcClient.getLastKnownStatuses().isEmpty()) {
                break;
            }
            Thread.sleep(retryNumber * waitTime);
            retryNumber++;
        }

        assertEquals(streamedBlocks, publishStreamGrpcClient.getPublishedBlocks());
        assertEquals(
                streamedBlocks, publishStreamGrpcClient.getLastKnownStatuses().size());
    }

    @Test
    void testRecoverStream_ResendLast() throws Exception {
        when(startupDataMock.getLatestAckBlockNumber()).thenReturn(5L);
        when(grpcConfig.rollbackDistance()).thenReturn(1L);
        Block dummyBlock = constructBlock(5);

        when(blockStreamManagerMock.getLastBlock()).thenReturn(dummyBlock);

        publishStreamGrpcClient.init();
        publishStreamGrpcClient.recoverStream();

        assertTrue(streamEnabled.get(), "Stream should be re-enabled after recovery with RESEND_LAST");
        assertEquals(1, publishStreamGrpcClient.getPublishedBlocks());
    }

    @Test
    void testRecoverStream_Rollback() throws Exception {
        when(startupDataMock.getLatestAckBlockNumber()).thenReturn(3L);
        when(grpcConfig.rollbackDistance()).thenReturn(2L);
        for (long i = 1; i <= 3; i++) {
            when(blockStreamManagerMock.getBlockByNumber(i)).thenReturn(constructBlock(i));
        }

        publishStreamGrpcClient.init();
        publishStreamGrpcClient.recoverStream();

        assertTrue(streamEnabled.get(), "Stream should be re-enabled after recovery with ROLLBACK");
        assertEquals(3, publishStreamGrpcClient.getPublishedBlocks());
    }

    @Test
    void testRecoverStream_SkipAhead() throws Exception {
        when(startupDataMock.getLatestAckBlockNumber()).thenReturn(10L);
        Block nextBlock = constructBlock(11);
        when(blockStreamManagerMock.getNextBlock()).thenReturn(nextBlock);

        publishStreamGrpcClient.init();
        publishStreamGrpcClient.recoverStream();

        assertTrue(streamEnabled.get(), "Stream should be re-enabled after SKIP_AHEAD");
        assertEquals(1, publishStreamGrpcClient.getPublishedBlocks());
    }

    @Test
    void testRecoverStreamFailsAfterMaxRetries() throws Exception {
        when(startupDataMock.getLatestAckBlockNumber()).thenReturn(0L);
        when(grpcConfig.rollbackDistance()).thenReturn(1L);
        // force an exception during block access
        when(blockStreamManagerMock.getLastBlock()).thenThrow(new IOException("Simulated failure"));

        publishStreamGrpcClient.init();
        publishStreamGrpcClient.recoverStream();

        assertTrue(!streamEnabled.get(), "Stream should be disabled after max retries");
    }

    @Test
    void testStreamBlock_RejectsAfterShutdown() throws InterruptedException {
        publishStreamGrpcClient.init();
        final int streamedBlocks = 3;

        for (int i = 0; i < streamedBlocks; i++) {
            final Block block = constructBlock(i);
            final boolean result = publishStreamGrpcClient.streamBlock(block);
            assertTrue(result);
        }

        // we use simple retry mechanism here, because sometimes server takes some time to receive the stream
        long retryNumber = 1;
        long waitTime = 500;

        while (retryNumber < 3) {
            if (!publishStreamGrpcClient.getLastKnownStatuses().isEmpty()) {
                break;
            }
            Thread.sleep(retryNumber * waitTime);
            retryNumber++;
        }

        assertEquals(streamedBlocks, publishStreamGrpcClient.getPublishedBlocks());
        assertEquals(
                streamedBlocks, publishStreamGrpcClient.getLastKnownStatuses().size());
        publishStreamGrpcClient.shutdown();
        isShutdownCalled = true;

        final Block block = constructBlock(0);
        IllegalStateException exception =
                assertThrows(IllegalStateException.class, () -> publishStreamGrpcClient.streamBlock(block));
        assertEquals("Stream is already completed, no further calls are allowed", exception.getMessage());
    }

    private Block constructBlock(long number) {
        BlockItem blockItemHeader = BlockItem.newBuilder()
                .setBlockHeader(BlockHeader.newBuilder().setNumber(number).build())
                .build();
        BlockItem blockItemProof = BlockItem.newBuilder()
                .setBlockProof(BlockProof.newBuilder().setBlock(number).build())
                .build();
        return Block.newBuilder()
                .addItems(blockItemHeader)
                .addItems(blockItemProof)
                .build();
    }
}
