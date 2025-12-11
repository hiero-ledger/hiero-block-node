// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.mode;

import static org.hiero.block.simulator.config.types.EndStreamMode.ERROR;
import static org.hiero.block.simulator.config.types.EndStreamMode.RESET;
import static org.hiero.block.simulator.config.types.EndStreamMode.TIMEOUT;
import static org.hiero.block.simulator.config.types.EndStreamMode.TOO_FAR_BEHIND;
import static org.hiero.block.simulator.fixtures.TestUtils.getTestConfiguration;
import static org.hiero.block.simulator.fixtures.TestUtils.getTestMetrics;
import static org.hiero.block.simulator.fixtures.blocks.BlockBuilder.createBlocks;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hedera.hapi.block.stream.protoc.Block;
import com.hedera.pbj.runtime.ParseException;
import com.swirlds.config.api.Configuration;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.hiero.block.api.protoc.PublishStreamRequest.EndStream;
import org.hiero.block.api.protoc.PublishStreamResponse;
import org.hiero.block.api.protoc.PublishStreamResponse.EndOfStream.Code;
import org.hiero.block.simulator.config.data.BlockGeneratorConfig;
import org.hiero.block.simulator.config.data.BlockStreamConfig;
import org.hiero.block.simulator.config.data.GrpcConfig;
import org.hiero.block.simulator.config.data.SimulatorStartupDataConfig;
import org.hiero.block.simulator.config.data.UnorderedStreamConfig;
import org.hiero.block.simulator.config.types.EndStreamMode;
import org.hiero.block.simulator.exception.BlockSimulatorParsingException;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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
    void handleEndOfStreamOnSuccess()
            throws BlockSimulatorParsingException, IOException, InterruptedException, ParseException {
        Block nextBlock = createBlocks(0, 1);
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
    void handleNodeBehindPublisher()
            throws BlockSimulatorParsingException, IOException, InterruptedException, ParseException {
        Block nextBlock = createBlocks(0, 1);
        PublishStreamResponse response = mock(PublishStreamResponse.class);
        PublishStreamResponse.BehindPublisher behindPublisher = mock(PublishStreamResponse.BehindPublisher.class);

        when(response.getNodeBehindPublisher()).thenReturn(behindPublisher);
        when(behindPublisher.getBlockNumber()).thenReturn(5L);
        when(response.hasNodeBehindPublisher()).thenReturn(true);

        publishClientManager.handleResponse(nextBlock, response);

        assertTrue(
                blockStreamManager.getNextBlock().getItems(0).getBlockHeader().getNumber() > 5L);
    }

    @Test
    void handleEndOfStreamOnDuplicateBlock()
            throws BlockSimulatorParsingException, IOException, InterruptedException, ParseException {
        Block nextBlock = createBlocks(0, 1);
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
    void handleEndOfStreamOnTimeout()
            throws BlockSimulatorParsingException, IOException, InterruptedException, ParseException {
        Block nextBlock = createBlocks(0, 1);
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
    void handleEndOfStreamOnBadBlockProof()
            throws BlockSimulatorParsingException, IOException, InterruptedException, ParseException {
        Block nextBlock = createBlocks(0, 1);
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
    void handleEndOfStreamOnInternalError()
            throws BlockSimulatorParsingException, IOException, InterruptedException, ParseException {
        Block nextBlock = createBlocks(0, 1);
        PublishStreamResponse response = mock(PublishStreamResponse.class);
        PublishStreamResponse.EndOfStream endOfStream = mock(PublishStreamResponse.EndOfStream.class);

        when(response.getEndStream()).thenReturn(endOfStream);
        when(endOfStream.getBlockNumber()).thenReturn(5L);
        when(endOfStream.getStatus()).thenReturn(Code.ERROR);
        when(response.hasEndStream()).thenReturn(true);

        publishClientManager.handleResponse(nextBlock, response);

        assertTrue(
                blockStreamManager.getNextBlock().getItems(0).getBlockHeader().getNumber() > 5L);
    }

    @Test
    void handleEndOfStreamOnPersistenceFailed()
            throws BlockSimulatorParsingException, IOException, InterruptedException, ParseException {
        Block nextBlock = createBlocks(0, 1);
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

    @Test
    void handleResendBlock() throws BlockSimulatorParsingException, IOException, InterruptedException, ParseException {
        Block nextBlock = createBlocks(0, 1);
        PublishStreamResponse response = mock(PublishStreamResponse.class);
        PublishStreamResponse.ResendBlock resendBlock = mock(PublishStreamResponse.ResendBlock.class);

        when(response.getResendBlock()).thenReturn(resendBlock);
        when(response.hasResendBlock()).thenReturn(true);
        when(resendBlock.getBlockNumber()).thenReturn(1L);

        publishClientManager.handleResponse(nextBlock, response);

        assertTrue(
                blockStreamManager.getNextBlock().getItems(0).getBlockHeader().getNumber() > 1L);
    }

    @Test
    void handleSkipBlock() throws BlockSimulatorParsingException, IOException, InterruptedException, ParseException {
        Block nextBlock = createBlocks(0, 1);
        PublishStreamResponse response = mock(PublishStreamResponse.class);
        PublishStreamResponse.SkipBlock skipBlock = mock(PublishStreamResponse.SkipBlock.class);

        when(response.getSkipBlock()).thenReturn(skipBlock);
        when(response.hasSkipBlock()).thenReturn(true);
        when(skipBlock.getBlockNumber()).thenReturn(1L);

        publishClientManager.handleResponse(nextBlock, response);

        assertTrue(
                blockStreamManager.getNextBlock().getItems(0).getBlockHeader().getNumber() > 1L);
    }

    @ParameterizedTest
    @MethodSource("provideEndStreamMode")
    void sendEndStream(EndStreamMode endStreamMode, EndStream.Code endStreamCode)
            throws BlockSimulatorParsingException, IOException, InterruptedException {
        publishClientManager.sendEndStream(endStreamMode);
        verify(publishStreamGrpcClient).handleEndStreamModeIfSet(endStreamCode);
    }

    private static Stream<Arguments> provideEndStreamMode() {
        return Stream.of(
                Arguments.of(RESET, EndStream.Code.RESET),
                Arguments.of(TIMEOUT, EndStream.Code.TIMEOUT),
                Arguments.of(ERROR, EndStream.Code.ERROR),
                Arguments.of(TOO_FAR_BEHIND, EndStream.Code.TOO_FAR_BEHIND));
    }
}
