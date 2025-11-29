// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.grpc.impl;

import static org.hiero.block.simulator.fixtures.TestUtils.findFreePort;
import static org.hiero.block.simulator.fixtures.TestUtils.getTestMetrics;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.hedera.hapi.block.stream.output.protoc.BlockHeader;
import com.hedera.hapi.block.stream.protoc.BlockItem;
import com.hedera.hapi.block.stream.protoc.BlockProof;
import com.swirlds.config.api.Configuration;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import org.hiero.block.api.protoc.BlockEnd;
import org.hiero.block.api.protoc.BlockItemSet;
import org.hiero.block.api.protoc.BlockNodeServiceGrpc;
import org.hiero.block.api.protoc.BlockStreamSubscribeServiceGrpc;
import org.hiero.block.api.protoc.ServerStatusRequest;
import org.hiero.block.api.protoc.ServerStatusResponse;
import org.hiero.block.api.protoc.SubscribeStreamRequest;
import org.hiero.block.api.protoc.SubscribeStreamResponse;
import org.hiero.block.api.protoc.SubscribeStreamResponse.Code;
import org.hiero.block.simulator.config.data.BlockStreamConfig;
import org.hiero.block.simulator.config.data.ConsumerConfig;
import org.hiero.block.simulator.config.data.GrpcConfig;
import org.hiero.block.simulator.config.types.SlowDownType;
import org.hiero.block.simulator.fixtures.TestUtils;
import org.hiero.block.simulator.grpc.ConsumerStreamGrpcClient;
import org.hiero.block.simulator.metrics.MetricsService;
import org.hiero.block.simulator.metrics.MetricsServiceImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ConsumerStreamGrpcClientImplTest {
    @Mock
    private GrpcConfig grpcConfig;

    @Mock
    private BlockStreamConfig blockStreamConfig;

    private ConsumerStreamGrpcClient consumerStreamGrpcClientImpl;
    private Server server;

    @BeforeEach
    void setUp() throws IOException {
        MockitoAnnotations.openMocks(this);
        final int serverPort = findFreePort();
        server = ServerBuilder.forPort(serverPort)
                .addService(new BlockStreamSubscribeServiceGrpc.BlockStreamSubscribeServiceImplBase() {
                    @Override
                    public void subscribeBlockStream(
                            SubscribeStreamRequest request, StreamObserver<SubscribeStreamResponse> responseObserver) {

                        // Simulate streaming blocks
                        long startBlock = request.getStartBlockNumber();
                        long endBlock = request.getEndBlockNumber();

                        for (long i = startBlock; i < endBlock; i++) {
                            // Simulate block items
                            BlockItem blockItemHeader = BlockItem.newBuilder()
                                    .setBlockHeader(BlockHeader.newBuilder()
                                            .setNumber(i)
                                            .build())
                                    .build();
                            BlockItem blockItemProof = BlockItem.newBuilder()
                                    .setBlockProof(
                                            BlockProof.newBuilder().setBlock(i).build())
                                    .build();

                            BlockItemSet blockItems = BlockItemSet.newBuilder()
                                    .addBlockItems(blockItemHeader)
                                    .addBlockItems(blockItemProof)
                                    .build();

                            responseObserver.onNext(SubscribeStreamResponse.newBuilder()
                                    .setBlockItems(blockItems)
                                    .build());
                            final BlockEnd endOfBlock =
                                    BlockEnd.newBuilder().setBlockNumber(i).build();
                            responseObserver.onNext(SubscribeStreamResponse.newBuilder()
                                    .setEndOfBlock(endOfBlock)
                                    .build());
                        }

                        // Send success status code at the end
                        responseObserver.onNext(SubscribeStreamResponse.newBuilder()
                                .setStatus(Code.SUCCESS)
                                .build());
                        responseObserver.onCompleted();
                    }
                })
                .addService(new BlockNodeServiceGrpc.BlockNodeServiceImplBase() {
                    @Override
                    public void serverStatus(
                            ServerStatusRequest request, StreamObserver<ServerStatusResponse> responseObserver) {
                        responseObserver.onNext(ServerStatusResponse.newBuilder()
                                .setFirstAvailableBlock(10)
                                .setLastAvailableBlock(20)
                                .build());
                        responseObserver.onCompleted();
                    }
                })
                .build()
                .start();

        when(grpcConfig.serverAddress()).thenReturn("localhost");
        when(grpcConfig.port()).thenReturn(serverPort);
        when(blockStreamConfig.lastKnownStatusesCapacity()).thenReturn(10);

        final Configuration config = TestUtils.getTestConfiguration();
        ConsumerConfig consumerConfig = config.getConfigData(ConsumerConfig.class);
        final MetricsService metricsService = new MetricsServiceImpl(getTestMetrics(config));
        consumerStreamGrpcClientImpl =
                new ConsumerStreamGrpcClientImpl(grpcConfig, blockStreamConfig, consumerConfig, metricsService);
        consumerStreamGrpcClientImpl.init();
    }

    @AfterEach
    public void tearDown() throws InterruptedException {
        consumerStreamGrpcClientImpl.completeStreaming();
        server.shutdownNow();
    }

    @Test
    public void testInit() {
        assertTrue(consumerStreamGrpcClientImpl.getLastKnownStatuses().isEmpty());
    }

    @Test
    void requestBlocks_Success() throws InterruptedException {
        final long startBlock = 0;
        final long endBlock = 5;

        assertEquals(startBlock, consumerStreamGrpcClientImpl.getConsumedBlocks());
        assertTrue(consumerStreamGrpcClientImpl.getLastKnownStatuses().isEmpty());

        consumerStreamGrpcClientImpl.requestBlocks(startBlock, endBlock);

        // We check if the final status matches what we have send from the server.
        final String lastStatus =
                consumerStreamGrpcClientImpl.getLastKnownStatuses().getLast();
        assertTrue(lastStatus.contains("status: %s".formatted(Code.SUCCESS.name())));

        assertEquals(endBlock, consumerStreamGrpcClientImpl.getConsumedBlocks());
    }

    @Test
    void requestBlocksAutoDiscovery() throws InterruptedException {
        // Create a real ConsumerConfig with desired values
        ConsumerConfig realConfig = new ConsumerConfig(-1L, 15L, true, SlowDownType.NONE, 2L, "10-30");
        
        // Re-init client with real config
        consumerStreamGrpcClientImpl = new ConsumerStreamGrpcClientImpl(grpcConfig, blockStreamConfig, realConfig, new MetricsServiceImpl(getTestMetrics(TestUtils.getTestConfiguration())));
        consumerStreamGrpcClientImpl.init();
        
        consumerStreamGrpcClientImpl.requestBlocks();
        
        // Should have consumed 10, 11, 12, 13, 14 (5 blocks)
        // because start=10 (from serverStatus), end=15.
        assertEquals(5, consumerStreamGrpcClientImpl.getConsumedBlocks());
    }

    @Test
    void requestBlocks_InvalidStartBlock() {
        final long startBlock = -1;
        final long endBlock = 5;

        assertThrows(
                IllegalArgumentException.class, () -> consumerStreamGrpcClientImpl.requestBlocks(startBlock, endBlock));
    }

    @Test
    void requestBlocks_InvalidEndBlock() {
        final long startBlock = 0;
        final long endBlock = -1;

        assertThrows(
                IllegalArgumentException.class, () -> consumerStreamGrpcClientImpl.requestBlocks(startBlock, endBlock));
    }

    @Test
    void completeStreaming_Success() throws InterruptedException {
        final long startBlock = 0;
        final long endBlock = 5;

        consumerStreamGrpcClientImpl.requestBlocks(startBlock, endBlock);
        consumerStreamGrpcClientImpl.completeStreaming();
    }
}
