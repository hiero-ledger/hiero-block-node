// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.grpc.impl;

import static org.hiero.block.simulator.fixtures.TestUtils.findFreePort;
import static org.hiero.block.simulator.fixtures.TestUtils.getTestMetrics;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.hedera.hapi.block.stream.output.protoc.BlockHeader;
import com.hedera.hapi.block.stream.protoc.Block;
import com.hedera.hapi.block.stream.protoc.BlockItem;
import com.hedera.hapi.block.stream.protoc.BlockProof;
import com.hedera.hapi.platform.event.legacy.EventTransaction;
import com.swirlds.config.api.Configuration;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.hiero.block.api.protoc.BlockItemSet;
import org.hiero.block.api.protoc.BlockStreamPublishServiceGrpc.BlockStreamPublishServiceImplBase;
import org.hiero.block.api.protoc.PublishStreamRequest;
import org.hiero.block.api.protoc.PublishStreamResponse;
import org.hiero.block.api.protoc.PublishStreamResponse.BlockAcknowledgement;
import org.hiero.block.api.protoc.PublishStreamResponse.EndOfStream;
import org.hiero.block.api.protoc.PublishStreamResponse.EndOfStream.Code;
import org.hiero.block.api.protoc.PublishStreamResponse.ResendBlock;
import org.hiero.block.simulator.config.data.BlockStreamConfig;
import org.hiero.block.simulator.config.data.GrpcConfig;
import org.hiero.block.simulator.config.types.EndStreamMode;
import org.hiero.block.simulator.config.types.MidBlockFailType;
import org.hiero.block.simulator.fixtures.TestUtils;
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

    private BlockStreamConfig blockStreamConfig;
    private AtomicBoolean streamEnabled;
    private Server server;

    private boolean isResendBlockEnabled = false;
    private boolean isSkipBlockEnabled = false;

    @BeforeEach
    void setUp() throws IOException {
        MockitoAnnotations.openMocks(this);

        streamEnabled = new AtomicBoolean(true);

        final int serverPort = findFreePort();
        server = ServerBuilder.forPort(serverPort)
                .addService(new BlockStreamPublishServiceImplBase() {
                    @Override
                    public StreamObserver<PublishStreamRequest> publishBlockStream(
                            StreamObserver<PublishStreamResponse> responseObserver) {
                        return new StreamObserver<>() {
                            private long lastBlockNumber = 0;

                            @Override
                            public void onNext(PublishStreamRequest request) {
                                BlockItemSet blockItems = request.getBlockItems();
                                List<BlockItem> items = blockItems.getBlockItemsList();

                                if (isResendBlockEnabled) {
                                    responseObserver.onNext(PublishStreamResponse.newBuilder()
                                            .setResendBlock(ResendBlock.newBuilder()
                                                    .setBlockNumber(1)
                                                    .build())
                                            .build());
                                    return;
                                }

                                if (isSkipBlockEnabled) {
                                    responseObserver.onNext(PublishStreamResponse.newBuilder()
                                            .setSkipBlock(PublishStreamResponse.SkipBlock.newBuilder()
                                                    .setBlockNumber(1)
                                                    .build())
                                            .build());
                                    return;
                                }

                                if (request.hasEndStream()) {
                                    server.shutdown();
                                }
                                // Simulate processing of block items
                                for (BlockItem item : items) {
                                    // Assume that the first BlockItem is a BlockHeader
                                    if (item.hasBlockHeader()) {
                                        lastBlockNumber = item.getBlockHeader().getNumber();
                                    }
                                    // Assume that the last BlockItem is a BlockProof
                                    if (item.hasBlockProof()) {
                                        // Send BlockAcknowledgement
                                        BlockAcknowledgement acknowledgement = BlockAcknowledgement.newBuilder()
                                                .setBlockNumber(lastBlockNumber)
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
                                EndOfStream endOfStream = EndOfStream.newBuilder()
                                        .setStatus(Code.SUCCESS)
                                        .setBlockNumber(lastBlockNumber)
                                        .build();
                                responseObserver.onNext(PublishStreamResponse.newBuilder()
                                        .setEndStream(endOfStream)
                                        .build());
                                responseObserver.onCompleted();
                            }
                        };
                    }
                })
                .build()
                .start();
        blockStreamConfig = BlockStreamConfig.builder().blockItemsBatchSize(2).build();

        Configuration config = TestUtils.getTestConfiguration();
        metricsService = new MetricsServiceImpl(getTestMetrics(config));
        streamEnabled = new AtomicBoolean(true);

        when(grpcConfig.serverAddress()).thenReturn("localhost");
        when(grpcConfig.port()).thenReturn(serverPort);

        publishStreamGrpcClient = new PublishStreamGrpcClientImpl(
                grpcConfig, blockStreamConfig, metricsService, streamEnabled, startupDataMock);
    }

    @AfterEach
    void teardown() throws InterruptedException {
        if (!isShutdownCalled) {
            publishStreamGrpcClient.shutdown();
        }

        if (server != null) {
            server.shutdown();
        }
        isResendBlockEnabled = false;
        isSkipBlockEnabled = false;
    }

    @Test
    public void testInit() {
        publishStreamGrpcClient.init();
        // Verify that lastKnownStatuses is cleared
        assertTrue(publishStreamGrpcClient.getLastKnownStatuses().isEmpty());
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

            final AtomicReference<PublishStreamResponse> publishStreamResponseAtomicReference = new AtomicReference<>();
            final Consumer<PublishStreamResponse> publishStreamResponseConsumer =
                    publishStreamResponseAtomicReference::set;
            final boolean result = publishStreamGrpcClient.streamBlock(block, publishStreamResponseConsumer);
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
    void testStreamBlock_RejectsAfterShutdown() throws InterruptedException {
        publishStreamGrpcClient.init();
        final int streamedBlocks = 3;
        final AtomicReference<PublishStreamResponse> publishStreamResponseAtomicReference = new AtomicReference<>();
        final Consumer<PublishStreamResponse> publishStreamResponseConsumer = publishStreamResponseAtomicReference::set;

        for (int i = 0; i < streamedBlocks; i++) {
            final Block block = constructBlock(i, false);
            final boolean result = publishStreamGrpcClient.streamBlock(block, publishStreamResponseConsumer);
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

        final Block block = constructBlock(0, false);
        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> publishStreamGrpcClient.streamBlock(block, publishStreamResponseConsumer));
        assertEquals("Stream is already completed, no further calls are allowed", exception.getMessage());
    }

    @Test
    public void testSteamBlockWithResendBlock() throws InterruptedException {
        isResendBlockEnabled = true;
        publishStreamGrpcClient.init();
        final AtomicReference<PublishStreamResponse> publishStreamResponseAtomicReference = new AtomicReference<>();
        final Consumer<PublishStreamResponse> publishStreamResponseConsumer = publishStreamResponseAtomicReference::set;

        Block block = constructBlock(0, true);
        publishStreamGrpcClient.streamBlock(block, publishStreamResponseConsumer);

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

        assertTrue(
                publishStreamGrpcClient.getLastKnownStatuses().getFirst().contains("resend_block"),
                "lastKnownStatuses should contain the resend block message");
    }

    @Test
    public void testSteamBlockWithSkipBlock() throws InterruptedException {
        isSkipBlockEnabled = true;
        publishStreamGrpcClient.init();
        final AtomicReference<PublishStreamResponse> publishStreamResponseAtomicReference = new AtomicReference<>();
        final Consumer<PublishStreamResponse> publishStreamResponseConsumer = publishStreamResponseAtomicReference::set;

        Block block = constructBlock(0, true);
        publishStreamGrpcClient.streamBlock(block, publishStreamResponseConsumer);

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

        assertTrue(
                publishStreamGrpcClient.getLastKnownStatuses().getFirst().contains("skip_block"),
                "lastKnownStatuses should contain the resend block message");
    }

    @Test
    public void handleMidBlockFailIfSetNone() {
        final AtomicReference<PublishStreamResponse> publishStreamResponseAtomicReference = new AtomicReference<>();
        final Consumer<PublishStreamResponse> publishStreamResponseConsumer = publishStreamResponseAtomicReference::set;
        blockStreamConfig = BlockStreamConfig.builder()
                .midBlockFailType(MidBlockFailType.NONE)
                .midBlockFailOffset(0L)
                .build();
        publishStreamGrpcClient = new PublishStreamGrpcClientImpl(
                grpcConfig, blockStreamConfig, metricsService, streamEnabled, startupDataMock);

        publishStreamGrpcClient.init();
        assertDoesNotThrow(
                () -> publishStreamGrpcClient.streamBlock(constructBlock(0, true), publishStreamResponseConsumer));
        assertDoesNotThrow(
                () -> publishStreamGrpcClient.streamBlock(constructBlock(1, true), publishStreamResponseConsumer));
    }

    @Test
    public void handleMidBlockFailIfSetAbrupt() {
        final AtomicReference<PublishStreamResponse> publishStreamResponseAtomicReference = new AtomicReference<>();
        final Consumer<PublishStreamResponse> publishStreamResponseConsumer = publishStreamResponseAtomicReference::set;
        blockStreamConfig = BlockStreamConfig.builder()
                .midBlockFailType(MidBlockFailType.ABRUPT)
                .midBlockFailOffset(1L)
                .build();
        publishStreamGrpcClient = new PublishStreamGrpcClientImpl(
                grpcConfig, blockStreamConfig, metricsService, streamEnabled, startupDataMock);

        publishStreamGrpcClient.init();
        assertDoesNotThrow(
                () -> publishStreamGrpcClient.streamBlock(constructBlock(0, true), publishStreamResponseConsumer));
        RuntimeException ex = assertThrows(
                RuntimeException.class,
                () -> publishStreamGrpcClient.streamBlock(constructBlock(1, true), publishStreamResponseConsumer));
        assertEquals("Configured abrupt disconnection occurred", ex.getMessage());
    }

    @Test
    public void handleMidBlockFailIfSetEos() {
        final AtomicReference<PublishStreamResponse> publishStreamResponseAtomicReference = new AtomicReference<>();
        final Consumer<PublishStreamResponse> publishStreamResponseConsumer = publishStreamResponseAtomicReference::set;
        blockStreamConfig = BlockStreamConfig.builder()
                .midBlockFailType(MidBlockFailType.EOS)
                .midBlockFailOffset(1L)
                .build();
        publishStreamGrpcClient = new PublishStreamGrpcClientImpl(
                grpcConfig, blockStreamConfig, metricsService, streamEnabled, startupDataMock);

        publishStreamGrpcClient.init();
        assertDoesNotThrow(
                () -> publishStreamGrpcClient.streamBlock(constructBlock(0, true), publishStreamResponseConsumer));
        assertThrows(
                IllegalStateException.class,
                () -> publishStreamGrpcClient.streamBlock(constructBlock(1, true), publishStreamResponseConsumer));
        isShutdownCalled = true; // to avoid calling onCompleted after onError
    }

    @Test
    public void handleMidBlockFailIfSetStreamingBatchTooSmall() {
        final AtomicReference<PublishStreamResponse> publishStreamResponseAtomicReference = new AtomicReference<>();
        final Consumer<PublishStreamResponse> publishStreamResponseConsumer = publishStreamResponseAtomicReference::set;
        blockStreamConfig = BlockStreamConfig.builder()
                .midBlockFailType(MidBlockFailType.ABRUPT)
                .midBlockFailOffset(0L)
                .build();
        publishStreamGrpcClient = new PublishStreamGrpcClientImpl(
                grpcConfig, blockStreamConfig, metricsService, streamEnabled, startupDataMock);
        publishStreamGrpcClient.init();
        assertDoesNotThrow(
                () -> publishStreamGrpcClient.streamBlock(constructBlock(1, false), publishStreamResponseConsumer));
    }

    @Test
    public void handleEndStreamTooFarBehind() throws InterruptedException {
        blockStreamConfig = BlockStreamConfig.builder()
                .endStreamMode(EndStreamMode.TOO_FAR_BEHIND)
                .build();
        publishStreamGrpcClient = new PublishStreamGrpcClientImpl(
                grpcConfig, blockStreamConfig, metricsService, streamEnabled, startupDataMock);
        publishStreamGrpcClient.init();
        publishStreamGrpcClient.handleEndStreamModeIfSet();
        Thread.sleep(200);
        assertTrue(server.isShutdown());
    }

    @Test
    public void handleEndStreamNone() throws InterruptedException {
        blockStreamConfig =
                BlockStreamConfig.builder().endStreamMode(EndStreamMode.NONE).build();
        publishStreamGrpcClient = new PublishStreamGrpcClientImpl(
                grpcConfig, blockStreamConfig, metricsService, streamEnabled, startupDataMock);
        publishStreamGrpcClient.init();
        publishStreamGrpcClient.handleEndStreamModeIfSet();
        Thread.sleep(200);
        assertFalse(server.isShutdown());
    }

    private Block constructBlock(long number, boolean withItems) {
        BlockItem blockItemHeader = BlockItem.newBuilder()
                .setBlockHeader(BlockHeader.newBuilder().setNumber(number).build())
                .build();
        BlockItem blockItemProof = BlockItem.newBuilder()
                .setBlockProof(BlockProof.newBuilder().setBlock(number).build())
                .build();
        if (withItems) {
            BlockItem blockItemEventTransaction1 = BlockItem.newBuilder()
                    .setEventTransaction(EventTransaction.newBuilder().build())
                    .build();
            BlockItem blockItemEventTransaction2 = BlockItem.newBuilder()
                    .setEventTransaction(EventTransaction.newBuilder().build())
                    .build();
            return Block.newBuilder()
                    .addItems(blockItemHeader)
                    .addItems(blockItemEventTransaction1)
                    .addItems(blockItemEventTransaction2)
                    .addItems(blockItemProof)
                    .build();
        } else {
            return Block.newBuilder()
                    .addItems(blockItemHeader)
                    .addItems(blockItemProof)
                    .build();
        }
    }
}
