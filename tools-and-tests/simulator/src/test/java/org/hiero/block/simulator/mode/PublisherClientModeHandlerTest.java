// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.mode;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.hedera.hapi.block.stream.protoc.Block;
import com.hedera.hapi.block.stream.protoc.BlockItem;
import com.swirlds.config.api.Configuration;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;
import org.hiero.block.api.protoc.PublishStreamResponse;
import org.hiero.block.simulator.TestUtils;
import org.hiero.block.simulator.config.data.BlockStreamConfig;
import org.hiero.block.simulator.config.types.StreamingMode;
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

    @Mock
    private BlockStreamConfig blockStreamConfig;

    @Mock
    private PublishStreamGrpcClient publishStreamGrpcClient;

    @Mock
    private BlockStreamManager blockStreamManager;

    @Mock
    private MetricsService metricsService;

    private PublisherClientModeHandler publisherClientModeHandler;

    @BeforeEach
    void setUp() throws IOException {
        MockitoAnnotations.openMocks(this);

        Configuration configuration = TestUtils.getTestConfiguration(
                Map.of("blockStream.maxBlockItemsToStream", "100", "blockStream.streamingMode", "CONSTANT_RATE"));

        metricsService = new MetricsServiceImpl(TestUtils.getTestMetrics(configuration));
    }

    @Test
    void testStartWithMillisPerBlockStreaming_WithBlocks() throws Exception {
        // Configure blockStreamConfig
        when(blockStreamConfig.streamingMode()).thenReturn(StreamingMode.MILLIS_PER_BLOCK);
        when(blockStreamConfig.millisecondsPerBlock()).thenReturn(0); // No delay for testing

        publisherClientModeHandler = new PublisherClientModeHandler(
                blockStreamConfig, publishStreamGrpcClient, blockStreamManager, metricsService);

        Block block1 = mock(Block.class);
        Block block2 = mock(Block.class);

        when(blockStreamManager.getNextBlock())
                .thenReturn(block1)
                .thenReturn(block2)
                .thenReturn(null);
        when(publishStreamGrpcClient.streamBlock(any(Block.class), any())).thenReturn(true);

        when(publishStreamGrpcClient.streamBlock(block1, any())).thenReturn(true);
        when(publishStreamGrpcClient.streamBlock(block2, any())).thenReturn(true);

        publisherClientModeHandler.start();

        verify(publishStreamGrpcClient).streamBlock(block1, any());
        verify(publishStreamGrpcClient).streamBlock(block2, any());
        verify(publishStreamGrpcClient).shutdown();
        verify(blockStreamManager, times(3)).getNextBlock();
    }

    @Test
    void testStartWithMillisPerBlockStreaming_NoBlocks() throws Exception {
        when(blockStreamConfig.streamingMode()).thenReturn(StreamingMode.MILLIS_PER_BLOCK);

        publisherClientModeHandler = new PublisherClientModeHandler(
                blockStreamConfig, publishStreamGrpcClient, blockStreamManager, metricsService);

        when(blockStreamManager.getNextBlock()).thenReturn(null);

        publisherClientModeHandler.start();

        verify(publishStreamGrpcClient, never()).streamBlock(any(Block.class), any());
        verify(blockStreamManager).getNextBlock();
    }

    @Test
    void testStartWithMillisPerBlockStreaming_ShouldPublishFalse() throws Exception {
        when(blockStreamConfig.streamingMode()).thenReturn(StreamingMode.MILLIS_PER_BLOCK);

        publisherClientModeHandler = new PublisherClientModeHandler(
                blockStreamConfig, publishStreamGrpcClient, blockStreamManager, metricsService);

        Block block1 = mock(Block.class);
        Block block2 = mock(Block.class);

        when(blockStreamManager.getNextBlock())
                .thenReturn(block1)
                .thenReturn(block2)
                .thenReturn(null);
        when(publishStreamGrpcClient.streamBlock(any(Block.class), any())).thenReturn(true);

        when(publishStreamGrpcClient.streamBlock(block1, any())).thenReturn(true);
        when(publishStreamGrpcClient.streamBlock(block2, any())).thenReturn(true);

        publisherClientModeHandler.stop();
        publisherClientModeHandler.start();

        verify(publishStreamGrpcClient, never()).streamBlock(any(Block.class), any());
        verify(blockStreamManager).getNextBlock();
    }

    @Test
    void testStartWithMillisPerBlockStreaming_NoBlocksAndShouldPublishFalse() throws Exception {
        when(blockStreamConfig.streamingMode()).thenReturn(StreamingMode.MILLIS_PER_BLOCK);

        publisherClientModeHandler = new PublisherClientModeHandler(
                blockStreamConfig, publishStreamGrpcClient, blockStreamManager, metricsService);

        when(blockStreamManager.getNextBlock()).thenReturn(null);

        publisherClientModeHandler.stop();
        publisherClientModeHandler.start();

        verify(publishStreamGrpcClient, never()).streamBlock(any(Block.class), any());
        verify(blockStreamManager).getNextBlock();
    }

    @Test
    void testStartWithConstantRateStreaming_WithinMaxItems() throws Exception {
        when(blockStreamConfig.streamingMode()).thenReturn(StreamingMode.CONSTANT_RATE);
        when(blockStreamConfig.delayBetweenBlockItems()).thenReturn(0);
        when(blockStreamConfig.maxBlockItemsToStream()).thenReturn(5);

        publisherClientModeHandler = new PublisherClientModeHandler(
                blockStreamConfig, publishStreamGrpcClient, blockStreamManager, metricsService);
        when(publishStreamGrpcClient.streamBlock(any(Block.class), any())).thenReturn(true);

        Block block1 = mock(Block.class);
        Block block2 = mock(Block.class);

        BlockItem blockItem1 = mock(BlockItem.class);
        BlockItem blockItem2 = mock(BlockItem.class);
        BlockItem blockItem3 = mock(BlockItem.class);
        BlockItem blockItem4 = mock(BlockItem.class);

        when(block1.getItemsList()).thenReturn(Arrays.asList(blockItem1, blockItem2));
        when(block2.getItemsList()).thenReturn(Arrays.asList(blockItem3, blockItem4));

        when(blockStreamManager.getNextBlock())
                .thenReturn(block1)
                .thenReturn(block2)
                .thenReturn(null);

        when(publishStreamGrpcClient.streamBlock(block1, any(Consumer.class))).thenReturn(true);
        when(publishStreamGrpcClient.streamBlock(block2, any())).thenReturn(true);

        publisherClientModeHandler.start();

        verify(publishStreamGrpcClient).streamBlock(block1, any());
        verify(publishStreamGrpcClient).streamBlock(block2, any());
        verify(publishStreamGrpcClient).shutdown();
        verify(blockStreamManager, times(3)).getNextBlock();
    }

    @Test
    void testStartWithConstantRateStreaming_ExceedingMaxItems() throws Exception {
        when(blockStreamConfig.streamingMode()).thenReturn(StreamingMode.CONSTANT_RATE);
        when(blockStreamConfig.delayBetweenBlockItems()).thenReturn(0);
        when(blockStreamConfig.maxBlockItemsToStream()).thenReturn(5);

        publisherClientModeHandler = new PublisherClientModeHandler(
                blockStreamConfig, publishStreamGrpcClient, blockStreamManager, metricsService);
        when(publishStreamGrpcClient.streamBlock(any(Block.class), any())).thenReturn(true);

        Block block1 = mock(Block.class);
        Block block2 = mock(Block.class);
        Block block3 = mock(Block.class);
        Block block4 = mock(Block.class);

        BlockItem blockItem1 = mock(BlockItem.class);
        BlockItem blockItem2 = mock(BlockItem.class);
        BlockItem blockItem3 = mock(BlockItem.class);
        BlockItem blockItem4 = mock(BlockItem.class);

        when(block1.getItemsList()).thenReturn(Arrays.asList(blockItem1, blockItem2));
        when(block2.getItemsList()).thenReturn(Arrays.asList(blockItem3, blockItem4));
        when(block3.getItemsList()).thenReturn(Arrays.asList(blockItem1, blockItem2));
        when(block4.getItemsList()).thenReturn(Arrays.asList(blockItem3, blockItem4));

        when(blockStreamManager.getNextBlock())
                .thenReturn(block1)
                .thenReturn(block2)
                .thenReturn(block3)
                .thenReturn(block4);

        when(publishStreamGrpcClient.streamBlock(block1, any())).thenReturn(true);
        when(publishStreamGrpcClient.streamBlock(block2, any())).thenReturn(true);
        when(publishStreamGrpcClient.streamBlock(block3, any())).thenReturn(true);
        when(publishStreamGrpcClient.streamBlock(block4, any())).thenReturn(true);

        publisherClientModeHandler.start();

        verify(publishStreamGrpcClient).streamBlock(block1, any());
        verify(publishStreamGrpcClient).streamBlock(block2, any());
        verify(publishStreamGrpcClient).streamBlock(block3, any());
        verify(publishStreamGrpcClient).shutdown();
        verify(blockStreamManager, times(3)).getNextBlock();
    }

    @Test
    void testStartWithConstantRateStreaming_NoBlocks() throws Exception {
        when(blockStreamConfig.streamingMode()).thenReturn(StreamingMode.CONSTANT_RATE);
        publisherClientModeHandler = new PublisherClientModeHandler(
                blockStreamConfig, publishStreamGrpcClient, blockStreamManager, metricsService);

        when(blockStreamManager.getNextBlock()).thenReturn(null);

        publisherClientModeHandler.start();

        verify(publishStreamGrpcClient, never()).streamBlock(any(Block.class), any());
        verify(blockStreamManager).getNextBlock();
    }

    @Test
    void testStartWithExceptionDuringStreaming() throws Exception {
        when(blockStreamConfig.streamingMode()).thenReturn(StreamingMode.MILLIS_PER_BLOCK);

        publisherClientModeHandler = new PublisherClientModeHandler(
                blockStreamConfig, publishStreamGrpcClient, blockStreamManager, metricsService);

        when(blockStreamManager.getNextBlock()).thenThrow(new IOException("Test exception"));

        assertThrows(IOException.class, () -> publisherClientModeHandler.start());

        verify(publishStreamGrpcClient, never()).streamBlock(any(Block.class), any());
        verify(blockStreamManager).getNextBlock();
        verify(publishStreamGrpcClient).shutdown();
        verifyNoMoreInteractions(blockStreamManager);
    }

    @Test
    void testMillisPerBlockStreaming_streamSuccessBecomesFalse() throws Exception {
        when(blockStreamConfig.streamingMode()).thenReturn(StreamingMode.MILLIS_PER_BLOCK);
        when(blockStreamConfig.millisecondsPerBlock()).thenReturn(1000);

        publisherClientModeHandler = new PublisherClientModeHandler(
                blockStreamConfig, publishStreamGrpcClient, blockStreamManager, metricsService);

        Block block1 = mock(Block.class);
        Block block2 = mock(Block.class);

        when(blockStreamManager.getNextBlock())
                .thenReturn(block1)
                .thenReturn(block2)
                .thenReturn(null);

        when(publishStreamGrpcClient.streamBlock(block1, any())).thenReturn(true);
        when(publishStreamGrpcClient.streamBlock(block2, any())).thenReturn(false);

        publisherClientModeHandler.start();

        verify(publishStreamGrpcClient).streamBlock(block1, any());
        verify(publishStreamGrpcClient).streamBlock(block2, any());
        verify(publishStreamGrpcClient).shutdown();
        verify(blockStreamManager, times(2)).getNextBlock();
    }

    @Test
    void testConstantRateStreaming_streamSuccessBecomesFalse() throws Exception {
        when(blockStreamConfig.streamingMode()).thenReturn(StreamingMode.CONSTANT_RATE);
        when(blockStreamConfig.delayBetweenBlockItems()).thenReturn(0);
        when(blockStreamConfig.maxBlockItemsToStream()).thenReturn(100);

        publisherClientModeHandler = new PublisherClientModeHandler(
                blockStreamConfig, publishStreamGrpcClient, blockStreamManager, metricsService);

        Block block1 = mock(Block.class);
        Block block2 = mock(Block.class);

        BlockItem blockItem1 = mock(BlockItem.class);
        BlockItem blockItem2 = mock(BlockItem.class);

        when(block1.getItemsList()).thenReturn(Collections.singletonList(blockItem1));
        when(block2.getItemsList()).thenReturn(Collections.singletonList(blockItem2));

        when(blockStreamManager.getNextBlock())
                .thenReturn(block1)
                .thenReturn(block2)
                .thenReturn(null);

        when(publishStreamGrpcClient.streamBlock(block1, any())).thenReturn(true);
        when(publishStreamGrpcClient.streamBlock(block2, any())).thenReturn(false);

        publisherClientModeHandler.start();

        verify(publishStreamGrpcClient).streamBlock(block1, any());
        verify(publishStreamGrpcClient).streamBlock(block2, any());
        verify(publishStreamGrpcClient).shutdown();
        verify(blockStreamManager, times(2)).getNextBlock();
    }
}
