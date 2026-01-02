// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.backfill.client.BackfillSourceConfig;
import org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.historicalblocks.LongRange;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Unit tests for {@link BackfillRunner}.
 */
@Timeout(value = 5, unit = TimeUnit.SECONDS)
class BackfillRunnerTest {

    private BackfillFetcher mockFetcher;
    private BackfillConfiguration mockConfig;
    private BlockMessagingFacility mockMessaging;
    private BackfillMetricsCallback mockMetricsCallback;
    private BackfillPersistenceAwaiter mockPersistenceAwaiter;
    private System.Logger logger;
    private BackfillRunner subject;

    @BeforeEach
    void setUp() {
        mockFetcher = mock(BackfillFetcher.class);
        mockConfig = mock(BackfillConfiguration.class);
        mockMessaging = mock(BlockMessagingFacility.class);
        mockMetricsCallback = mock(BackfillMetricsCallback.class);
        mockPersistenceAwaiter = mock(BackfillPersistenceAwaiter.class);
        logger = System.getLogger(BackfillRunnerTest.class.getName());
        subject = new BackfillRunner(
                mockFetcher, mockConfig, mockMessaging, logger, mockMetricsCallback, mockPersistenceAwaiter);
    }

    @Nested
    @DisplayName("computeChunk Tests")
    class ComputeChunkTests {

        @Test
        @DisplayName("should return null when node not in availability map")
        void shouldReturnNullWhenNodeNotInAvailability() {
            // given
            BackfillSourceConfig nodeConfig = mock(BackfillSourceConfig.class);
            NodeSelectionStrategy.NodeSelection selection = new NodeSelectionStrategy.NodeSelection(nodeConfig, 100L);
            Map<BackfillSourceConfig, List<LongRange>> availability = new HashMap<>();
            // nodeConfig not in map

            // when
            LongRange result = BackfillRunner.computeChunk(selection, availability, 200L, 10);

            // then
            assertNull(result);
        }

        @Test
        @DisplayName("should return null when no range covers start block")
        void shouldReturnNullWhenNoRangeCoverStart() {
            // given
            BackfillSourceConfig nodeConfig = mock(BackfillSourceConfig.class);
            NodeSelectionStrategy.NodeSelection selection = new NodeSelectionStrategy.NodeSelection(nodeConfig, 100L);
            Map<BackfillSourceConfig, List<LongRange>> availability = new HashMap<>();
            // Range 50-80 does not cover start block 100
            availability.put(nodeConfig, List.of(new LongRange(50, 80)));

            // when
            LongRange result = BackfillRunner.computeChunk(selection, availability, 200L, 10);

            // then
            assertNull(result);
        }

        @Test
        @DisplayName("should limit chunk to batch size")
        void shouldLimitChunkToBatchSize() {
            // given
            BackfillSourceConfig nodeConfig = mock(BackfillSourceConfig.class);
            NodeSelectionStrategy.NodeSelection selection = new NodeSelectionStrategy.NodeSelection(nodeConfig, 100L);
            Map<BackfillSourceConfig, List<LongRange>> availability = new HashMap<>();
            availability.put(nodeConfig, List.of(new LongRange(0, 500)));
            long batchSize = 10;

            // when
            LongRange result = BackfillRunner.computeChunk(selection, availability, 500L, batchSize);

            // then
            assertNotNull(result);
            assertEquals(100L, result.start());
            assertEquals(109L, result.end()); // 100 + 10 - 1
        }

        @Test
        @DisplayName("should limit chunk to gap end")
        void shouldLimitChunkToGapEnd() {
            // given
            BackfillSourceConfig nodeConfig = mock(BackfillSourceConfig.class);
            NodeSelectionStrategy.NodeSelection selection = new NodeSelectionStrategy.NodeSelection(nodeConfig, 100L);
            Map<BackfillSourceConfig, List<LongRange>> availability = new HashMap<>();
            availability.put(nodeConfig, List.of(new LongRange(0, 500)));
            long gapEnd = 105L;
            long batchSize = 20;

            // when
            LongRange result = BackfillRunner.computeChunk(selection, availability, gapEnd, batchSize);

            // then
            assertNotNull(result);
            assertEquals(100L, result.start());
            assertEquals(105L, result.end()); // Limited by gapEnd
        }

        @Test
        @DisplayName("should limit chunk to range end")
        void shouldLimitChunkToRangeEnd() {
            // given
            BackfillSourceConfig nodeConfig = mock(BackfillSourceConfig.class);
            NodeSelectionStrategy.NodeSelection selection = new NodeSelectionStrategy.NodeSelection(nodeConfig, 100L);
            Map<BackfillSourceConfig, List<LongRange>> availability = new HashMap<>();
            availability.put(nodeConfig, List.of(new LongRange(0, 103))); // Range ends at 103
            long gapEnd = 500L;
            long batchSize = 20;

            // when
            LongRange result = BackfillRunner.computeChunk(selection, availability, gapEnd, batchSize);

            // then
            assertNotNull(result);
            assertEquals(100L, result.start());
            assertEquals(103L, result.end()); // Limited by range end
        }

        @Test
        @DisplayName("should select correct range when multiple ranges available")
        void shouldSelectCorrectRangeFromMultiple() {
            // given
            BackfillSourceConfig nodeConfig = mock(BackfillSourceConfig.class);
            NodeSelectionStrategy.NodeSelection selection = new NodeSelectionStrategy.NodeSelection(nodeConfig, 150L);
            Map<BackfillSourceConfig, List<LongRange>> availability = new HashMap<>();
            availability.put(
                    nodeConfig,
                    List.of(
                            new LongRange(0, 50),
                            new LongRange(100, 200), // This covers 150
                            new LongRange(300, 400)));
            long batchSize = 10;

            // when
            LongRange result = BackfillRunner.computeChunk(selection, availability, 500L, batchSize);

            // then
            assertNotNull(result);
            assertEquals(150L, result.start());
            assertEquals(159L, result.end());
        }
    }

    @Nested
    @DisplayName("run Tests")
    class RunTests {

        @Test
        @DisplayName("should handle empty availability gracefully")
        void shouldHandleEmptyAvailability() throws Exception {
            // given
            TypedGap gap = new TypedGap(new LongRange(0, 10), GapType.HISTORICAL);
            when(mockFetcher.getAvailabilityForRange(any())).thenReturn(Collections.emptyMap());

            // when
            subject.run(gap);

            // then - should complete without error
            verify(mockFetcher, atLeastOnce()).resetStatus();
            verify(mockFetcher, atLeastOnce()).getAvailabilityForRange(any());
            verify(mockMessaging, never()).sendBackfilledBlockNotification(any());
        }

        @Test
        @DisplayName("should report fetch error when no nodes available")
        void shouldReportFetchErrorWhenNoNodesAvailable() throws Exception {
            // given
            TypedGap gap = new TypedGap(new LongRange(0, 10), GapType.HISTORICAL);
            BackfillSourceConfig nodeConfig = mock(BackfillSourceConfig.class);
            Map<BackfillSourceConfig, List<LongRange>> availability = new HashMap<>();
            availability.put(nodeConfig, List.of(new LongRange(0, 10)));

            when(mockFetcher.getAvailabilityForRange(any())).thenReturn(availability);
            when(mockFetcher.selectNextChunk(anyLong(), anyLong(), any())).thenReturn(Optional.empty());

            // Replan returns empty
            when(mockFetcher.getAvailabilityForRange(any()))
                    .thenReturn(availability)
                    .thenReturn(Collections.emptyMap());

            // when
            subject.run(gap);

            // then
            verify(mockMetricsCallback).onFetchError(any(RuntimeException.class));
        }

        @Test
        @DisplayName("should remove node from availability when fetch returns empty")
        void shouldRemoveNodeOnEmptyFetch() throws Exception {
            // given
            TypedGap gap = new TypedGap(new LongRange(0, 10), GapType.HISTORICAL);
            BackfillSourceConfig nodeConfig = mock(BackfillSourceConfig.class);
            Map<BackfillSourceConfig, List<LongRange>> availability = new HashMap<>();
            availability.put(nodeConfig, List.of(new LongRange(0, 10)));

            when(mockConfig.fetchBatchSize()).thenReturn(10);
            when(mockFetcher.getAvailabilityForRange(any())).thenReturn(availability);
            when(mockFetcher.selectNextChunk(anyLong(), anyLong(), any()))
                    .thenReturn(Optional.of(new NodeSelectionStrategy.NodeSelection(nodeConfig, 0L)));
            when(mockFetcher.fetchBlocksFromNode(eq(nodeConfig), any())).thenReturn(Collections.emptyList());

            // Replan returns empty after failure
            when(mockFetcher.getAvailabilityForRange(any()))
                    .thenReturn(availability)
                    .thenReturn(Collections.emptyMap());

            // when
            subject.run(gap);

            // then - should have tried to fetch
            verify(mockFetcher).fetchBlocksFromNode(eq(nodeConfig), any());
        }
    }

    @Nested
    @DisplayName("Backpressure Tests")
    class BackpressureTests {

        @Test
        @DisplayName("should track blocks before sending")
        void shouldTrackBlocksBeforeSending() throws Exception {
            // given
            TypedGap gap = new TypedGap(new LongRange(0, 0), GapType.HISTORICAL);
            BackfillSourceConfig nodeConfig = mock(BackfillSourceConfig.class);
            Map<BackfillSourceConfig, List<LongRange>> availability = new HashMap<>();
            availability.put(nodeConfig, List.of(new LongRange(0, 0)));

            BlockUnparsed mockBlock = createMockBlockUnparsed(0L);

            when(mockConfig.fetchBatchSize()).thenReturn(10);
            when(mockConfig.delayBetweenBatches()).thenReturn(0);
            when(mockConfig.perBlockProcessingTimeout()).thenReturn(1000);
            when(mockFetcher.getAvailabilityForRange(any())).thenReturn(availability);
            when(mockFetcher.selectNextChunk(anyLong(), anyLong(), any()))
                    .thenReturn(Optional.of(new NodeSelectionStrategy.NodeSelection(nodeConfig, 0L)));
            when(mockFetcher.fetchBlocksFromNode(eq(nodeConfig), any())).thenReturn(List.of(mockBlock));
            when(mockPersistenceAwaiter.awaitPersistence(anyLong(), anyLong())).thenReturn(true);

            // when
            subject.run(gap);

            // then - should track block before sending
            verify(mockPersistenceAwaiter).trackBlock(0L);
            verify(mockMessaging).sendBackfilledBlockNotification(any(BackfilledBlockNotification.class));
        }

        @Test
        @DisplayName("should await persistence for each block")
        void shouldAwaitPersistenceForEachBlock() throws Exception {
            // given
            TypedGap gap = new TypedGap(new LongRange(0, 0), GapType.HISTORICAL);
            BackfillSourceConfig nodeConfig = mock(BackfillSourceConfig.class);
            Map<BackfillSourceConfig, List<LongRange>> availability = new HashMap<>();
            availability.put(nodeConfig, List.of(new LongRange(0, 0)));

            BlockUnparsed mockBlock = createMockBlockUnparsed(0L);

            when(mockConfig.fetchBatchSize()).thenReturn(10);
            when(mockConfig.delayBetweenBatches()).thenReturn(0);
            when(mockConfig.perBlockProcessingTimeout()).thenReturn(1000);
            when(mockFetcher.getAvailabilityForRange(any())).thenReturn(availability);
            when(mockFetcher.selectNextChunk(anyLong(), anyLong(), any()))
                    .thenReturn(Optional.of(new NodeSelectionStrategy.NodeSelection(nodeConfig, 0L)));
            when(mockFetcher.fetchBlocksFromNode(eq(nodeConfig), any())).thenReturn(List.of(mockBlock));
            when(mockPersistenceAwaiter.awaitPersistence(anyLong(), anyLong())).thenReturn(true);

            // when
            subject.run(gap);

            // then
            verify(mockPersistenceAwaiter).awaitPersistence(eq(0L), eq(1000L));
        }

        @Test
        @DisplayName("should continue on persistence timeout")
        void shouldContinueOnPersistenceTimeout() throws Exception {
            // given
            TypedGap gap = new TypedGap(new LongRange(0, 0), GapType.HISTORICAL);
            BackfillSourceConfig nodeConfig = mock(BackfillSourceConfig.class);
            Map<BackfillSourceConfig, List<LongRange>> availability = new HashMap<>();
            availability.put(nodeConfig, List.of(new LongRange(0, 0)));

            BlockUnparsed mockBlock = createMockBlockUnparsed(0L);

            when(mockConfig.fetchBatchSize()).thenReturn(10);
            when(mockConfig.delayBetweenBatches()).thenReturn(0);
            when(mockConfig.perBlockProcessingTimeout()).thenReturn(100);
            when(mockFetcher.getAvailabilityForRange(any())).thenReturn(availability);
            when(mockFetcher.selectNextChunk(anyLong(), anyLong(), any()))
                    .thenReturn(Optional.of(new NodeSelectionStrategy.NodeSelection(nodeConfig, 0L)));
            when(mockFetcher.fetchBlocksFromNode(eq(nodeConfig), any())).thenReturn(List.of(mockBlock));
            when(mockPersistenceAwaiter.awaitPersistence(anyLong(), anyLong())).thenReturn(false); // Timeout!

            // when - should not throw
            subject.run(gap);

            // then - completed despite timeout
            verify(mockPersistenceAwaiter).awaitPersistence(eq(0L), eq(100L));
        }
    }

    @Nested
    @DisplayName("Metrics Tests")
    class MetricsTests {

        @Test
        @DisplayName("should report block fetched metric")
        void shouldReportBlockFetched() throws Exception {
            // given
            TypedGap gap = new TypedGap(new LongRange(0, 0), GapType.HISTORICAL);
            BackfillSourceConfig nodeConfig = mock(BackfillSourceConfig.class);
            Map<BackfillSourceConfig, List<LongRange>> availability = new HashMap<>();
            availability.put(nodeConfig, List.of(new LongRange(0, 0)));

            BlockUnparsed mockBlock = createMockBlockUnparsed(0L);

            when(mockConfig.fetchBatchSize()).thenReturn(10);
            when(mockConfig.delayBetweenBatches()).thenReturn(0);
            when(mockConfig.perBlockProcessingTimeout()).thenReturn(1000);
            when(mockFetcher.getAvailabilityForRange(any())).thenReturn(availability);
            when(mockFetcher.selectNextChunk(anyLong(), anyLong(), any()))
                    .thenReturn(Optional.of(new NodeSelectionStrategy.NodeSelection(nodeConfig, 0L)));
            when(mockFetcher.fetchBlocksFromNode(eq(nodeConfig), any())).thenReturn(List.of(mockBlock));
            when(mockPersistenceAwaiter.awaitPersistence(anyLong(), anyLong())).thenReturn(true);

            // when
            subject.run(gap);

            // then
            verify(mockMetricsCallback).onBlockFetched(0L);
        }

        @Test
        @DisplayName("should report block dispatched metric")
        void shouldReportBlockDispatched() throws Exception {
            // given
            TypedGap gap = new TypedGap(new LongRange(0, 0), GapType.HISTORICAL);
            BackfillSourceConfig nodeConfig = mock(BackfillSourceConfig.class);
            Map<BackfillSourceConfig, List<LongRange>> availability = new HashMap<>();
            availability.put(nodeConfig, List.of(new LongRange(0, 0)));

            BlockUnparsed mockBlock = createMockBlockUnparsed(0L);

            when(mockConfig.fetchBatchSize()).thenReturn(10);
            when(mockConfig.delayBetweenBatches()).thenReturn(0);
            when(mockConfig.perBlockProcessingTimeout()).thenReturn(1000);
            when(mockFetcher.getAvailabilityForRange(any())).thenReturn(availability);
            when(mockFetcher.selectNextChunk(anyLong(), anyLong(), any()))
                    .thenReturn(Optional.of(new NodeSelectionStrategy.NodeSelection(nodeConfig, 0L)));
            when(mockFetcher.fetchBlocksFromNode(eq(nodeConfig), any())).thenReturn(List.of(mockBlock));
            when(mockPersistenceAwaiter.awaitPersistence(anyLong(), anyLong())).thenReturn(true);

            // when
            subject.run(gap);

            // then
            verify(mockMetricsCallback).onBlockDispatched(0L);
        }

        @Test
        @DisplayName("should report multiple blocks fetched and dispatched")
        void shouldReportMultipleBlocks() throws Exception {
            // given
            TypedGap gap = new TypedGap(new LongRange(0, 2), GapType.HISTORICAL);
            BackfillSourceConfig nodeConfig = mock(BackfillSourceConfig.class);
            Map<BackfillSourceConfig, List<LongRange>> availability = new HashMap<>();
            availability.put(nodeConfig, List.of(new LongRange(0, 2)));

            BlockUnparsed mockBlock0 = createMockBlockUnparsed(0L);
            BlockUnparsed mockBlock1 = createMockBlockUnparsed(1L);
            BlockUnparsed mockBlock2 = createMockBlockUnparsed(2L);

            when(mockConfig.fetchBatchSize()).thenReturn(10);
            when(mockConfig.delayBetweenBatches()).thenReturn(0);
            when(mockConfig.perBlockProcessingTimeout()).thenReturn(1000);
            when(mockFetcher.getAvailabilityForRange(any())).thenReturn(availability);
            when(mockFetcher.selectNextChunk(anyLong(), anyLong(), any()))
                    .thenReturn(Optional.of(new NodeSelectionStrategy.NodeSelection(nodeConfig, 0L)));
            when(mockFetcher.fetchBlocksFromNode(eq(nodeConfig), any()))
                    .thenReturn(List.of(mockBlock0, mockBlock1, mockBlock2));
            when(mockPersistenceAwaiter.awaitPersistence(anyLong(), anyLong())).thenReturn(true);

            // when
            subject.run(gap);

            // then
            verify(mockMetricsCallback, times(3)).onBlockFetched(anyLong());
            verify(mockMetricsCallback, times(3)).onBlockDispatched(anyLong());
        }
    }

    /**
     * Creates a mock BlockUnparsed with a block header containing the specified block number.
     */
    private BlockUnparsed createMockBlockUnparsed(long blockNumber) {
        // Create a real BlockHeader and serialize it
        com.hedera.hapi.block.stream.output.BlockHeader blockHeader =
                com.hedera.hapi.block.stream.output.BlockHeader.newBuilder()
                        .number(blockNumber)
                        .build();

        com.hedera.pbj.runtime.io.buffer.Bytes headerBytes =
                com.hedera.hapi.block.stream.output.BlockHeader.PROTOBUF.toBytes(blockHeader);

        org.hiero.block.internal.BlockItemUnparsed blockItem = org.hiero.block.internal.BlockItemUnparsed.newBuilder()
                .blockHeader(headerBytes)
                .build();

        return BlockUnparsed.newBuilder().blockItems(List.of(blockItem)).build();
    }
}
