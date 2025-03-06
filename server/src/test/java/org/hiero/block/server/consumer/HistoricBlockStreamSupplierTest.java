// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.consumer;

import static org.hiero.block.server.util.PersistTestUtils.generateBlockItemsUnparsedForWithBlockNumber;
import static org.hiero.block.server.util.TestConfigUtil.getTestBlockNodeContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.hapi.block.BlockUnparsed;
import com.hedera.hapi.block.SubscribeStreamResponseUnparsed;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.hiero.block.common.utils.ChunkUtils;
import org.hiero.block.server.config.BlockNodeContext;
import org.hiero.block.server.persistence.storage.read.BlockReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class HistoricBlockStreamSupplierTest {

    @Mock
    private BlockReader<BlockUnparsed> blockReader;

    @Mock
    private Pipeline<SubscribeStreamResponseUnparsed> helidonConsumerObserver;

    @Mock
    private Pipeline<? super SubscribeStreamResponseUnparsed> closedRangeHistoricStreamObserver;

    @Mock
    private BlockNodeContext blockNodeContext;

    private HistoricBlockStreamSupplier historicBlockStreamSupplier;

    private int maxBlockItemBatchSize;

    private final int testTimeout = 1000;

    @BeforeEach
    public void setUp() throws IOException {
        this.blockNodeContext = getTestBlockNodeContext(Map.of("consumer.maxBlockItemBatchSize", "10"));
        this.maxBlockItemBatchSize = blockNodeContext
                .configuration()
                .getConfigData(ConsumerConfig.class)
                .maxBlockItemBatchSize();

        // The startBlockNumber and endBlockNumber don't matter for these tests
        this.historicBlockStreamSupplier = new HistoricBlockStreamSupplier(
                0L,
                10L,
                blockReader,
                helidonConsumerObserver,
                blockNodeContext.metricsService(),
                blockNodeContext.configuration());
    }

    @ParameterizedTest
    @MethodSource("blockItemBatchSizes")
    public void testSendInBatches(int maxBatchSize, int expectedNumberOfInvocations) throws Exception {

        // Just picking an arbitrary, odd number of blocks to send that's not easily divisible by the batch size
        final List<BlockItemUnparsed> blockItems = generateBlockItemsUnparsedForWithBlockNumber(1, 3111);
        historicBlockStreamSupplier.sendInBatches(ChunkUtils.chunkify(blockItems, maxBatchSize));

        // Verify the helidon observer was invoked an expected number of times
        verify(helidonConsumerObserver, times(expectedNumberOfInvocations)).onNext(any());
    }

    @Test
    public void testClosedRangeHistoricStreamingHappyPath() throws IOException, ParseException {

        final int numberOfBlocks = 5;
        final int itemsPerBlock = 3000;
        final List<BlockUnparsed> blocks = generateBlocks(numberOfBlocks, itemsPerBlock);
        for (int i = 1; i <= numberOfBlocks; i++) {
            when(blockReader.read(i)).thenReturn(Optional.of(blocks.get(i - 1)));
        }

        final Runnable closedRangeHistoricStreamingRunnable = ClosedRangeHistoricStreamEventHandlerBuilder.build(
                1L,
                numberOfBlocks,
                blockReader,
                closedRangeHistoricStreamObserver,
                blockNodeContext.metricsService(),
                blockNodeContext.configuration());

        closedRangeHistoricStreamingRunnable.run();

        // Calculate the number of times the observer should be invoked
        // based on the number of blocks requested, the configuration
        // and the number of items per block
        int timesInvoked = (((itemsPerBlock / maxBlockItemBatchSize) * numberOfBlocks)) + 1;
        verify(closedRangeHistoricStreamObserver, timeout(testTimeout).times(timesInvoked))
                .onNext(any());
    }

    @Test
    public void testClosedRangeHistoricStreamingBlockNotFound() throws Exception {
        when(blockReader.read(1)).thenReturn(Optional.empty());

        final HistoricBlockStreamSupplier historicBlockStreamSupplier = new HistoricBlockStreamSupplier(
                1L,
                1L,
                blockReader,
                helidonConsumerObserver,
                blockNodeContext.metricsService(),
                blockNodeContext.configuration());

        historicBlockStreamSupplier.run();

        // Confirm the observer was invoked with the read stream not available message
        verify(helidonConsumerObserver, timeout(testTimeout).times(1)).onNext(any());
    }

    private List<BlockUnparsed> generateBlocks(int numberOfBlocks, int itemsPerBlock) {
        final List<BlockUnparsed> blocks = new LinkedList<>();
        for (int i = 1; i <= numberOfBlocks; i++) {
            final List<BlockItemUnparsed> blockItems = generateBlockItemsUnparsedForWithBlockNumber(i, itemsPerBlock);
            blocks.add(BlockUnparsed.newBuilder().blockItems(blockItems).build());
        }

        return blocks;
    }

    // Test various batch sizes to make sure the splitting is correct
    private static Stream<Arguments> blockItemBatchSizes() {
        return Stream.of(
                Arguments.of(1, 3111),
                Arguments.of(2, 1556),
                Arguments.of(251, 13),
                Arguments.of(1000, 4),
                Arguments.of(2000, 2),
                Arguments.of(3110, 2));
    }
}
