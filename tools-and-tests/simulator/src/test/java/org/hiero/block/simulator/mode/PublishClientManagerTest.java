// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.mode;

import static org.hiero.block.simulator.fixtures.TestUtils.getTestConfiguration;
import static org.hiero.block.simulator.fixtures.TestUtils.getTestMetrics;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.hedera.hapi.block.stream.protoc.Block;
import com.swirlds.config.api.Configuration;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.hiero.block.api.protoc.PublishStreamResponse;
import org.hiero.block.api.protoc.PublishStreamResponse.EndOfStream.Code;
import org.hiero.block.simulator.config.data.BlockGeneratorConfig;
import org.hiero.block.simulator.config.data.BlockStreamConfig;
import org.hiero.block.simulator.config.data.GrpcConfig;
import org.hiero.block.simulator.config.data.SimulatorStartupDataConfig;
import org.hiero.block.simulator.config.data.UnorderedStreamConfig;
import org.hiero.block.simulator.generator.BlockStreamManager;
import org.hiero.block.simulator.generator.CraftBlockStreamManager;
import org.hiero.block.simulator.grpc.PublishStreamGrpcClient;
import org.hiero.block.simulator.metrics.MetricsService;
import org.hiero.block.simulator.metrics.MetricsServiceImpl;
import org.hiero.block.simulator.mode.impl.PublishClientManager;
import org.hiero.block.simulator.mode.impl.PublisherClientModeHandler;
import org.hiero.block.simulator.startup.SimulatorStartupData;
import org.hiero.block.simulator.startup.impl.SimulatorStartupDataImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class PublishClientManagerTest {

    @Mock
    PublishStreamGrpcClient publishStreamGrpcClient;

    private BlockStreamManager blockStreamManager;
    private PublishClientManager publishClientManager;

    @BeforeEach
    void setUp() throws IOException {
        MockitoAnnotations.openMocks(this);
        final AtomicBoolean streamEnabled = new AtomicBoolean(true);
        final Configuration configuration = getTestConfiguration();
        final GrpcConfig grpcConfig = configuration.getConfigData(GrpcConfig.class);
        final BlockStreamConfig blockStreamConfig = configuration.getConfigData(BlockStreamConfig.class);
        final SimulatorStartupDataConfig simulatorStartupDataConfig =
                configuration.getConfigData(SimulatorStartupDataConfig.class);
        final BlockGeneratorConfig blockGeneratorConfig = configuration.getConfigData(BlockGeneratorConfig.class);
        final UnorderedStreamConfig unorderedStreamConfig = configuration.getConfigData(UnorderedStreamConfig.class);

        final MetricsService metricsService = new MetricsServiceImpl(getTestMetrics(configuration));
        final SimulatorStartupData startupData =
                new SimulatorStartupDataImpl(simulatorStartupDataConfig, blockGeneratorConfig);
        blockStreamManager = new CraftBlockStreamManager(blockGeneratorConfig, startupData, unorderedStreamConfig);
        final PublisherClientModeHandler publisherClientModeHandler = new PublisherClientModeHandler(
                blockStreamConfig, publishStreamGrpcClient, blockStreamManager, metricsService);

        publishClientManager = new PublishClientManager(
                grpcConfig,
                blockStreamConfig,
                blockStreamManager,
                metricsService,
                startupData,
                streamEnabled,
                publishStreamGrpcClient,
                publisherClientModeHandler);
    }

    @Test
    void handleEndOfStreamOnSuccess() throws Exception {
        Block nextBlock = mock(Block.class);
        PublishStreamResponse response = mock(PublishStreamResponse.class);
        PublishStreamResponse.EndOfStream endOfStream = mock(PublishStreamResponse.EndOfStream.class);

        when(response.getEndStream()).thenReturn(endOfStream);
        when(endOfStream.getBlockNumber()).thenReturn(5L);
        when(endOfStream.getStatus()).thenReturn(Code.SUCCESS);
        when(response.hasEndStream()).thenReturn(true);

        publishClientManager.handleResponse(nextBlock, response);

        assertTrue(
                blockStreamManager.getNextBlock().getItems(0).getBlockHeader().getNumber() > 5L);
    }

    @Test
    void handleEndOfStreamOnBehind() throws Exception {
        Block nextBlock = mock(Block.class);
        PublishStreamResponse response = mock(PublishStreamResponse.class);
        PublishStreamResponse.EndOfStream endOfStream = mock(PublishStreamResponse.EndOfStream.class);

        when(response.getEndStream()).thenReturn(endOfStream);
        when(endOfStream.getBlockNumber()).thenReturn(5L);
        when(endOfStream.getStatus()).thenReturn(Code.BEHIND);
        when(response.hasEndStream()).thenReturn(true);

        publishClientManager.handleResponse(nextBlock, response);

        assertTrue(
                blockStreamManager.getNextBlock().getItems(0).getBlockHeader().getNumber() > 5L);
    }

    @Test
    void handleEndOfStreamOnDuplicateBlock() throws Exception {
        Block nextBlock = mock(Block.class);
        PublishStreamResponse response = mock(PublishStreamResponse.class);
        PublishStreamResponse.EndOfStream endOfStream = mock(PublishStreamResponse.EndOfStream.class);

        when(response.getEndStream()).thenReturn(endOfStream);
        when(endOfStream.getBlockNumber()).thenReturn(5L);
        when(endOfStream.getStatus()).thenReturn(Code.DUPLICATE_BLOCK);
        when(response.hasEndStream()).thenReturn(true);

        publishClientManager.handleResponse(nextBlock, response);

        assertTrue(
                blockStreamManager.getNextBlock().getItems(0).getBlockHeader().getNumber() > 5L);
    }

    @Test
    void handleEndOfStreamOnTimeout() throws Exception {
        Block nextBlock = mock(Block.class);
        PublishStreamResponse response = mock(PublishStreamResponse.class);
        PublishStreamResponse.EndOfStream endOfStream = mock(PublishStreamResponse.EndOfStream.class);

        when(response.getEndStream()).thenReturn(endOfStream);
        when(endOfStream.getBlockNumber()).thenReturn(5L);
        when(endOfStream.getStatus()).thenReturn(Code.TIMEOUT);
        when(response.hasEndStream()).thenReturn(true);

        publishClientManager.handleResponse(nextBlock, response);

        assertTrue(
                blockStreamManager.getNextBlock().getItems(0).getBlockHeader().getNumber() > 5L);
    }

    @Test
    void handleEndOfStreamOnBadBlockProof() throws Exception {
        Block nextBlock = mock(Block.class);
        PublishStreamResponse response = mock(PublishStreamResponse.class);
        PublishStreamResponse.EndOfStream endOfStream = mock(PublishStreamResponse.EndOfStream.class);

        when(response.getEndStream()).thenReturn(endOfStream);
        when(endOfStream.getBlockNumber()).thenReturn(5L);
        when(endOfStream.getStatus()).thenReturn(Code.BAD_BLOCK_PROOF);
        when(response.hasEndStream()).thenReturn(true);

        publishClientManager.handleResponse(nextBlock, response);

        assertTrue(
                blockStreamManager.getNextBlock().getItems(0).getBlockHeader().getNumber() > 5L);
    }

    @Test
    void handleEndOfStreamOnInternalError() throws Exception {
        Block nextBlock = mock(Block.class);
        PublishStreamResponse response = mock(PublishStreamResponse.class);
        PublishStreamResponse.EndOfStream endOfStream = mock(PublishStreamResponse.EndOfStream.class);

        when(response.getEndStream()).thenReturn(endOfStream);
        when(endOfStream.getBlockNumber()).thenReturn(5L);
        when(endOfStream.getStatus()).thenReturn(Code.INTERNAL_ERROR);
        when(response.hasEndStream()).thenReturn(true);

        publishClientManager.handleResponse(nextBlock, response);

        assertTrue(
                blockStreamManager.getNextBlock().getItems(0).getBlockHeader().getNumber() > 5L);
    }

    @Test
    void handleEndOfStreamOnPersistenceFailed() throws Exception {
        Block nextBlock = mock(Block.class);
        PublishStreamResponse response = mock(PublishStreamResponse.class);
        PublishStreamResponse.EndOfStream endOfStream = mock(PublishStreamResponse.EndOfStream.class);

        when(response.getEndStream()).thenReturn(endOfStream);
        when(endOfStream.getBlockNumber()).thenReturn(5L);
        when(endOfStream.getStatus()).thenReturn(Code.PERSISTENCE_FAILED);
        when(response.hasEndStream()).thenReturn(true);

        publishClientManager.handleResponse(nextBlock, response);

        assertTrue(
                blockStreamManager.getNextBlock().getItems(0).getBlockHeader().getNumber() > 5L);
    }
}
