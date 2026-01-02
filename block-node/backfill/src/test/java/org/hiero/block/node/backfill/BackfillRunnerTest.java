// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hedera.hapi.block.stream.Block;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.blocks.BlockUtils;
import org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.backfill.client.BackfillSourceConfig;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
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

    /** Default test configuration with sensible defaults for unit tests. */
    private static final BackfillConfiguration TEST_CONFIG = BackfillPluginTest.BackfillConfigBuilder.NewBuilder()
            .delayBetweenBatches(0) // 0 for fast tests
            .buildRecord();

    private BackfillFetcher mockFetcher;
    private BackfillConfiguration config;
    private TestBlockMessagingFacility messaging;
    private BackfillMetricsCallback mockMetricsCallback;
    private BackfillPersistenceAwaiter persistenceAwaiter;
    private System.Logger logger;
    private BackfillRunner subject;

    @BeforeEach
    void setUp() {
        mockFetcher = mock(BackfillFetcher.class);
        config = TEST_CONFIG;
        messaging = new TestBlockMessagingFacility();
        mockMetricsCallback = mock(BackfillMetricsCallback.class);
        persistenceAwaiter = new BackfillPersistenceAwaiter();
        logger = System.getLogger(BackfillRunnerTest.class.getName());
        subject = new BackfillRunner(mockFetcher, config, messaging, logger, mockMetricsCallback, persistenceAwaiter);
    }

    /**
     * Creates a BlockUnparsed with a block header containing the specified block number.
     * Uses testFixtures utilities instead of manual construction.
     */
    private static BlockUnparsed createTestBlock(long blockNumber) {
        Block block = new Block(Arrays.asList(SimpleTestBlockItemBuilder.createSimpleBlockWithNumber(blockNumber)));
        return BlockUtils.toBlockUnparsed(block);
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
            GapDetector.Gap gap = new GapDetector.Gap(new LongRange(0, 10), GapDetector.Type.HISTORICAL);
            when(mockFetcher.getAvailabilityForRange(any())).thenReturn(Collections.emptyMap());

            // when
            subject.run(gap);

            // then - should complete without error
            verify(mockFetcher, atLeastOnce()).resetStatus();
            verify(mockFetcher, atLeastOnce()).getAvailabilityForRange(any());
            // Using real TestBlockMessagingFacility - verify no notifications were sent
            assertTrue(
                    messaging.getSentBlockItems().isEmpty(),
                    "No block items should be sent when availability is empty");
        }

        @Test
        @DisplayName("should report fetch error when no nodes available")
        void shouldReportFetchErrorWhenNoNodesAvailable() throws Exception {
            // given
            GapDetector.Gap gap = new GapDetector.Gap(new LongRange(0, 10), GapDetector.Type.HISTORICAL);
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
            GapDetector.Gap gap = new GapDetector.Gap(new LongRange(0, 10), GapDetector.Type.HISTORICAL);
            BackfillSourceConfig nodeConfig = mock(BackfillSourceConfig.class);
            Map<BackfillSourceConfig, List<LongRange>> availability = new HashMap<>();
            availability.put(nodeConfig, List.of(new LongRange(0, 10)));

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
        @DisplayName("should track blocks before sending and clear after persistence")
        void shouldTrackBlocksBeforeSending() throws Exception {
            // given
            GapDetector.Gap gap = new GapDetector.Gap(new LongRange(0, 0), GapDetector.Type.HISTORICAL);
            BackfillSourceConfig nodeConfig = mock(BackfillSourceConfig.class);
            Map<BackfillSourceConfig, List<LongRange>> availability = new HashMap<>();
            availability.put(nodeConfig, List.of(new LongRange(0, 0)));

            BlockUnparsed testBlock = createTestBlock(0L);

            when(mockFetcher.getAvailabilityForRange(any())).thenReturn(availability);
            when(mockFetcher.selectNextChunk(anyLong(), anyLong(), any()))
                    .thenReturn(Optional.of(new NodeSelectionStrategy.NodeSelection(nodeConfig, 0L)));
            when(mockFetcher.fetchBlocksFromNode(eq(nodeConfig), any())).thenReturn(List.of(testBlock));

            // Register the persistence awaiter to receive notifications
            messaging.registerBlockNotificationHandler(persistenceAwaiter, false, "persistence-awaiter");

            // Register a handler that simulates immediate persistence (verification + persist flow)
            messaging.registerBlockNotificationHandler(
                    new org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler() {
                        @Override
                        public void handleBackfilled(
                                org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification notification) {
                            // Simulate immediate persistence
                            messaging.sendBlockPersisted(new PersistedNotification(
                                    notification.blockNumber(), true, 1, BlockSource.BACKFILL));
                        }
                    },
                    false,
                    "test-persistence-handler");

            // when
            subject.run(gap);

            // then - block was tracked and then cleared after persistence
            assertEquals(0, persistenceAwaiter.getPendingCount(), "All blocks should be persisted and cleared");
            verify(mockMetricsCallback).onBlockFetched(0L);
            verify(mockMetricsCallback).onBlockDispatched(0L);
        }

        @Test
        @DisplayName("should await persistence for each block")
        void shouldAwaitPersistenceForEachBlock() throws Exception {
            // given
            GapDetector.Gap gap = new GapDetector.Gap(new LongRange(0, 0), GapDetector.Type.HISTORICAL);
            BackfillSourceConfig nodeConfig = mock(BackfillSourceConfig.class);
            Map<BackfillSourceConfig, List<LongRange>> availability = new HashMap<>();
            availability.put(nodeConfig, List.of(new LongRange(0, 0)));

            BlockUnparsed testBlock = createTestBlock(0L);

            when(mockFetcher.getAvailabilityForRange(any())).thenReturn(availability);
            when(mockFetcher.selectNextChunk(anyLong(), anyLong(), any()))
                    .thenReturn(Optional.of(new NodeSelectionStrategy.NodeSelection(nodeConfig, 0L)));
            when(mockFetcher.fetchBlocksFromNode(eq(nodeConfig), any())).thenReturn(List.of(testBlock));

            // Register persistence awaiter and simulate persistence
            messaging.registerBlockNotificationHandler(persistenceAwaiter, false, "persistence-awaiter");
            messaging.registerBlockNotificationHandler(
                    new org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler() {
                        @Override
                        public void handleBackfilled(
                                org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification notification) {
                            messaging.sendBlockPersisted(new PersistedNotification(
                                    notification.blockNumber(), true, 1, BlockSource.BACKFILL));
                        }
                    },
                    false,
                    "test-persistence-handler");

            // when
            subject.run(gap);

            // then - persistence notification was received
            assertEquals(
                    1, messaging.getSentPersistedNotifications().size(), "One persistence notification should be sent");
            assertEquals(
                    0L,
                    messaging.getSentPersistedNotifications().getFirst().blockNumber(),
                    "Persisted block should be block 0");
        }

        @Test
        @DisplayName("should continue on persistence timeout")
        void shouldContinueOnPersistenceTimeout() throws Exception {
            // given - use a config with very short timeout
            BackfillConfiguration shortTimeoutConfig = BackfillPluginTest.BackfillConfigBuilder.NewBuilder()
                    .delayBetweenBatches(0)
                    .perBlockProcessingTimeout(50) // very short for timeout test
                    .buildRecord();

            // Create runner with short timeout config
            BackfillRunner timeoutSubject = new BackfillRunner(
                    mockFetcher, shortTimeoutConfig, messaging, logger, mockMetricsCallback, persistenceAwaiter);

            GapDetector.Gap gap = new GapDetector.Gap(new LongRange(0, 0), GapDetector.Type.HISTORICAL);
            BackfillSourceConfig nodeConfig = mock(BackfillSourceConfig.class);
            Map<BackfillSourceConfig, List<LongRange>> availability = new HashMap<>();
            availability.put(nodeConfig, List.of(new LongRange(0, 0)));

            BlockUnparsed testBlock = createTestBlock(0L);

            when(mockFetcher.getAvailabilityForRange(any())).thenReturn(availability);
            when(mockFetcher.selectNextChunk(anyLong(), anyLong(), any()))
                    .thenReturn(Optional.of(new NodeSelectionStrategy.NodeSelection(nodeConfig, 0L)));
            when(mockFetcher.fetchBlocksFromNode(eq(nodeConfig), any())).thenReturn(List.of(testBlock));

            // Register awaiter but do NOT register a handler that sends persistence notification
            // This will cause the await to timeout
            messaging.registerBlockNotificationHandler(persistenceAwaiter, false, "persistence-awaiter");

            // when - should not throw even though persistence times out
            timeoutSubject.run(gap);

            // then - completed despite timeout, metrics still reported
            verify(mockMetricsCallback).onBlockFetched(0L);
            verify(mockMetricsCallback).onBlockDispatched(0L);
            // Pending count is 0 because awaitPersistence removes the block after await (timeout or success)
            assertEquals(0, persistenceAwaiter.getPendingCount(), "Block should be removed after await");
        }
    }

    @Nested
    @DisplayName("Metrics Tests")
    class MetricsTests {

        @Test
        @DisplayName("should report block fetched metric")
        void shouldReportBlockFetched() throws Exception {
            // given
            GapDetector.Gap gap = new GapDetector.Gap(new LongRange(0, 0), GapDetector.Type.HISTORICAL);
            BackfillSourceConfig nodeConfig = mock(BackfillSourceConfig.class);
            Map<BackfillSourceConfig, List<LongRange>> availability = new HashMap<>();
            availability.put(nodeConfig, List.of(new LongRange(0, 0)));

            BlockUnparsed testBlock = createTestBlock(0L);

            when(mockFetcher.getAvailabilityForRange(any())).thenReturn(availability);
            when(mockFetcher.selectNextChunk(anyLong(), anyLong(), any()))
                    .thenReturn(Optional.of(new NodeSelectionStrategy.NodeSelection(nodeConfig, 0L)));
            when(mockFetcher.fetchBlocksFromNode(eq(nodeConfig), any())).thenReturn(List.of(testBlock));

            // Register handlers for persistence flow
            messaging.registerBlockNotificationHandler(persistenceAwaiter, false, "persistence-awaiter");
            messaging.registerBlockNotificationHandler(
                    new org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler() {
                        @Override
                        public void handleBackfilled(
                                org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification notification) {
                            messaging.sendBlockPersisted(new PersistedNotification(
                                    notification.blockNumber(), true, 1, BlockSource.BACKFILL));
                        }
                    },
                    false,
                    "test-persistence-handler");

            // when
            subject.run(gap);

            // then
            verify(mockMetricsCallback).onBlockFetched(0L);
        }

        @Test
        @DisplayName("should report block dispatched metric")
        void shouldReportBlockDispatched() throws Exception {
            // given
            GapDetector.Gap gap = new GapDetector.Gap(new LongRange(0, 0), GapDetector.Type.HISTORICAL);
            BackfillSourceConfig nodeConfig = mock(BackfillSourceConfig.class);
            Map<BackfillSourceConfig, List<LongRange>> availability = new HashMap<>();
            availability.put(nodeConfig, List.of(new LongRange(0, 0)));

            BlockUnparsed testBlock = createTestBlock(0L);

            when(mockFetcher.getAvailabilityForRange(any())).thenReturn(availability);
            when(mockFetcher.selectNextChunk(anyLong(), anyLong(), any()))
                    .thenReturn(Optional.of(new NodeSelectionStrategy.NodeSelection(nodeConfig, 0L)));
            when(mockFetcher.fetchBlocksFromNode(eq(nodeConfig), any())).thenReturn(List.of(testBlock));

            // Register handlers for persistence flow
            messaging.registerBlockNotificationHandler(persistenceAwaiter, false, "persistence-awaiter");
            messaging.registerBlockNotificationHandler(
                    new org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler() {
                        @Override
                        public void handleBackfilled(
                                org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification notification) {
                            messaging.sendBlockPersisted(new PersistedNotification(
                                    notification.blockNumber(), true, 1, BlockSource.BACKFILL));
                        }
                    },
                    false,
                    "test-persistence-handler");

            // when
            subject.run(gap);

            // then
            verify(mockMetricsCallback).onBlockDispatched(0L);
        }

        @Test
        @DisplayName("should report multiple blocks fetched and dispatched")
        void shouldReportMultipleBlocks() throws Exception {
            // given
            GapDetector.Gap gap = new GapDetector.Gap(new LongRange(0, 2), GapDetector.Type.HISTORICAL);
            BackfillSourceConfig nodeConfig = mock(BackfillSourceConfig.class);
            Map<BackfillSourceConfig, List<LongRange>> availability = new HashMap<>();
            availability.put(nodeConfig, List.of(new LongRange(0, 2)));

            BlockUnparsed testBlock0 = createTestBlock(0L);
            BlockUnparsed testBlock1 = createTestBlock(1L);
            BlockUnparsed testBlock2 = createTestBlock(2L);

            when(mockFetcher.getAvailabilityForRange(any())).thenReturn(availability);
            when(mockFetcher.selectNextChunk(anyLong(), anyLong(), any()))
                    .thenReturn(Optional.of(new NodeSelectionStrategy.NodeSelection(nodeConfig, 0L)));
            when(mockFetcher.fetchBlocksFromNode(eq(nodeConfig), any()))
                    .thenReturn(List.of(testBlock0, testBlock1, testBlock2));

            // Register handlers for persistence flow
            messaging.registerBlockNotificationHandler(persistenceAwaiter, false, "persistence-awaiter");
            messaging.registerBlockNotificationHandler(
                    new org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler() {
                        @Override
                        public void handleBackfilled(
                                org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification notification) {
                            messaging.sendBlockPersisted(new PersistedNotification(
                                    notification.blockNumber(), true, 1, BlockSource.BACKFILL));
                        }
                    },
                    false,
                    "test-persistence-handler");

            // when
            subject.run(gap);

            // then
            verify(mockMetricsCallback, times(3)).onBlockFetched(anyLong());
            verify(mockMetricsCallback, times(3)).onBlockDispatched(anyLong());
        }
    }
}
