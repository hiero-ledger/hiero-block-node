// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.mode;

import static org.hiero.block.simulator.fixtures.blocks.BlockBuilder.createBlocks;
import static org.hiero.block.simulator.fixtures.generator.TestBlockStreamManager.getTestBlockStreamManager;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hedera.hapi.block.stream.protoc.Block;
import com.swirlds.config.api.Configuration;
import java.io.IOException;
import java.util.Map;
import org.hiero.block.simulator.config.data.BlockStreamConfig;
import org.hiero.block.simulator.fixtures.TestUtils;
import org.hiero.block.simulator.generator.BlockStreamManager;
import org.hiero.block.simulator.grpc.PublishStreamGrpcClient;
import org.hiero.block.simulator.metrics.MetricsService;
import org.hiero.block.simulator.metrics.MetricsServiceImpl;
import org.hiero.block.simulator.mode.impl.PublisherClientModeHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class PublisherClientModeHandlerTest {

    private BlockStreamConfig blockStreamConfig;

    @Mock
    private PublishStreamGrpcClient publishStreamGrpcClient;

    private BlockStreamManager blockStreamManager;

    private MetricsService metricsService;

    private PublisherClientModeHandler publisherClientModeHandler;

    @BeforeEach
    void setUp() throws IOException {
        MockitoAnnotations.openMocks(this);

        Configuration configuration = TestUtils.getTestConfiguration(Map.of(
                "blockstream.streamingMode", "MILLIS_PER_BLOCK",
                "blockstream.millisecondsPerBlock", "0"));
        blockStreamConfig = configuration.getConfigData(BlockStreamConfig.class);

        metricsService = new MetricsServiceImpl(TestUtils.getTestMetrics(configuration));
    }

    @Test
    void testStartWithMillisPerBlockStreaming_WithBlocks() throws Exception {
        blockStreamManager = getTestBlockStreamManager(2);
        publisherClientModeHandler = new PublisherClientModeHandler(
                blockStreamConfig, publishStreamGrpcClient, blockStreamManager, metricsService);

        Block block1 = createBlocks(0, 1);
        Block block2 = createBlocks(1, 2);

        when(publishStreamGrpcClient.streamBlock(any(Block.class), any())).thenReturn(true);

        publisherClientModeHandler.start();

        verify(publishStreamGrpcClient).streamBlock(eq(block1), any());
        verify(publishStreamGrpcClient).streamBlock(eq(block2), any());
        verify(publishStreamGrpcClient).shutdown();
    }

    @Test
    void testStartWithMillisPerBlockStreaming_NoBlocks() throws Exception {
        blockStreamManager = getTestBlockStreamManager(0);
        publisherClientModeHandler = new PublisherClientModeHandler(
                blockStreamConfig, publishStreamGrpcClient, blockStreamManager, metricsService);

        publisherClientModeHandler.start();

        verify(publishStreamGrpcClient, never()).streamBlock(any(Block.class), any());
    }

    @Test
    void testStartWithMillisPerBlockStreaming_ShouldPublishFalse() throws Exception {
        blockStreamManager = getTestBlockStreamManager(2);
        publisherClientModeHandler = new PublisherClientModeHandler(
                blockStreamConfig, publishStreamGrpcClient, blockStreamManager, metricsService);

        when(publishStreamGrpcClient.streamBlock(any(Block.class), any())).thenReturn(true);

        publisherClientModeHandler.stop();
        publisherClientModeHandler.start();

        verify(publishStreamGrpcClient, never()).streamBlock(any(Block.class), any());
    }

    @Test
    void testStartWithMillisPerBlockStreaming_NoBlocksAndShouldPublishFalse() throws Exception {
        blockStreamManager = getTestBlockStreamManager(0);
        publisherClientModeHandler = new PublisherClientModeHandler(
                blockStreamConfig, publishStreamGrpcClient, blockStreamManager, metricsService);

        publisherClientModeHandler.stop();
        publisherClientModeHandler.start();

        verify(publishStreamGrpcClient, never()).streamBlock(any(Block.class), any());
    }

    @Test
    void testStartWithConstantRateStreaming_WithinMaxItems() throws Exception {
        blockStreamManager = getTestBlockStreamManager(2);
        Configuration configuration = TestUtils.getTestConfiguration(Map.of(
                "blockStream.streamingMode",
                "CONSTANT_RATE",
                "blockstream.delayBetweenBlockItems",
                "0",
                "blockStream.maxBlockItemsToStream",
                "13"));
        blockStreamConfig = configuration.getConfigData(BlockStreamConfig.class);

        publisherClientModeHandler = new PublisherClientModeHandler(
                blockStreamConfig, publishStreamGrpcClient, blockStreamManager, metricsService);
        when(publishStreamGrpcClient.streamBlock(any(Block.class), any())).thenReturn(true);

        Block block1 = createBlocks(0, 1);
        Block block2 = createBlocks(1, 2);

        publisherClientModeHandler.start();

        verify(publishStreamGrpcClient).streamBlock(eq(block1), any());
        verify(publishStreamGrpcClient).streamBlock(eq(block2), any());
        verify(publishStreamGrpcClient).shutdown();
    }

    @Test
    void testStartWithConstantRateStreaming_ExceedingMaxItems() throws Exception {
        blockStreamManager = getTestBlockStreamManager(4);
        Configuration configuration = TestUtils.getTestConfiguration(Map.of(
                "blockStream.streamingMode",
                "CONSTANT_RATE",
                "blockstream.delayBetweenBlockItems",
                "0",
                "blockStream.maxBlockItemsToStream",
                "18"));
        blockStreamConfig = configuration.getConfigData(BlockStreamConfig.class);

        publisherClientModeHandler = new PublisherClientModeHandler(
                blockStreamConfig, publishStreamGrpcClient, blockStreamManager, metricsService);
        when(publishStreamGrpcClient.streamBlock(any(Block.class), any())).thenReturn(true);

        Block block1 = createBlocks(0, 1);
        Block block2 = createBlocks(1, 2);
        Block block3 = createBlocks(2, 3);
        Block block4 = createBlocks(3, 4);

        publisherClientModeHandler.start();

        verify(publishStreamGrpcClient).streamBlock(eq(block1), any());
        verify(publishStreamGrpcClient).streamBlock(eq(block2), any());
        verify(publishStreamGrpcClient).streamBlock(eq(block3), any());
        verify(publishStreamGrpcClient).shutdown();
    }

    @Test
    void testStartWithConstantRateStreaming_NoBlocks() throws Exception {
        blockStreamManager = getTestBlockStreamManager(0);
        Configuration configuration =
                TestUtils.getTestConfiguration(Map.of("blockStream.streamingMode", "CONSTANT_RATE"));
        blockStreamConfig = configuration.getConfigData(BlockStreamConfig.class);
        publisherClientModeHandler = new PublisherClientModeHandler(
                blockStreamConfig, publishStreamGrpcClient, blockStreamManager, metricsService);

        publisherClientModeHandler.start();

        verify(publishStreamGrpcClient, never()).streamBlock(any(Block.class), any());
    }

    @Test
    void testStartWithExceptionDuringStreaming() throws Exception {
        blockStreamManager = mock(BlockStreamManager.class);
        publisherClientModeHandler = new PublisherClientModeHandler(
                blockStreamConfig, publishStreamGrpcClient, blockStreamManager, metricsService);

        when(blockStreamManager.getNextBlock()).thenThrow(new IOException("Test exception"));

        assertThrows(IOException.class, () -> publisherClientModeHandler.start());

        verify(publishStreamGrpcClient, never()).streamBlock(any(Block.class), any());
        verify(blockStreamManager).getNextBlock();
        verify(publishStreamGrpcClient).shutdown();
    }

    @Test
    void testMillisPerBlockStreaming_streamSuccessBecomesFalse() throws Exception {
        blockStreamManager = getTestBlockStreamManager(2);
        publisherClientModeHandler = new PublisherClientModeHandler(
                blockStreamConfig, publishStreamGrpcClient, blockStreamManager, metricsService);

        Block block1 = createBlocks(0, 1);
        Block block2 = createBlocks(1, 2);

        when(publishStreamGrpcClient.streamBlock(any(Block.class), any())).thenReturn(true);

        publisherClientModeHandler.start();

        verify(publishStreamGrpcClient).streamBlock(eq(block1), any());
        verify(publishStreamGrpcClient).streamBlock(eq(block2), any());
        verify(publishStreamGrpcClient).shutdown();
    }

    @Test
    void testConstantRateStreaming_streamSuccessBecomesFalse() throws Exception {
        blockStreamManager = getTestBlockStreamManager(2);
        Configuration configuration = TestUtils.getTestConfiguration(Map.of(
                "blockStream.streamingMode",
                "CONSTANT_RATE",
                "blockstream.delayBetweenBlockItems",
                "0",
                "blockStream.maxBlockItemsToStream",
                "7"));
        blockStreamConfig = configuration.getConfigData(BlockStreamConfig.class);

        publisherClientModeHandler = new PublisherClientModeHandler(
                blockStreamConfig, publishStreamGrpcClient, blockStreamManager, metricsService);

        Block block1 = createBlocks(0, 1);
        Block block2 = createBlocks(1, 2);
        when(publishStreamGrpcClient.streamBlock(any(Block.class), any())).thenReturn(true);

        publisherClientModeHandler.start();

        verify(publishStreamGrpcClient).streamBlock(eq(block1), any());
        verify(publishStreamGrpcClient).streamBlock(eq(block2), any());
        verify(publishStreamGrpcClient).shutdown();
    }
}
