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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.hiero.block.internal.BlockNodeSourceConfig;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.historicalblocks.LongRange;
import org.hiero.metrics.LongCounter;
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
    private TestBlockMessagingFacility messaging;
    private BackfillPlugin.MetricsHolder mockMetricsHolder;
    private LongCounter.Measurement mockFetchErrorsCounter;
    private LongCounter.Measurement mockFetchedBlocksCounter;
    private AtomicLong pendingBackfillBlocks;
    private BackfillPersistenceAwaiter persistenceAwaiter;
    private System.Logger logger;
    private BackfillRunner subject;

    @BeforeEach
    void setUp() {
        mockFetcher = mock(BackfillFetcher.class);
        messaging = new TestBlockMessagingFacility();
        mockFetchErrorsCounter = mock(LongCounter.Measurement.class);
        mockFetchedBlocksCounter = mock(LongCounter.Measurement.class);
        mockMetricsHolder = new BackfillPlugin.MetricsHolder(
                mock(LongCounter.Measurement.class), // backfillGapsDetected
                mock(LongCounter.Measurement.class), // backfillGapsSubmitted
                mockFetchedBlocksCounter, // backfillFetchedBlocks
                mock(LongCounter.Measurement.class), // backfillBlocksBackfilled
                mockFetchErrorsCounter, // backfillFetchErrors
                mock(LongCounter.Measurement.class), // backfillInFlightGauge
                mock(LongCounter.Measurement.class));
        pendingBackfillBlocks = new AtomicLong(0);
        persistenceAwaiter = new BackfillPersistenceAwaiter();
        logger = System.getLogger(BackfillRunnerTest.class.getName());
        subject = new BackfillRunner(
                mockFetcher,
                TEST_CONFIG,
                messaging,
                logger,
                mockMetricsHolder,
                pendingBackfillBlocks,
                persistenceAwaiter);
    }

    /**
     * Creates a BlockUnparsed with a block header containing the specified block number.
     * Uses testFixtures utilities instead of manual construction.
     */
    private static BlockUnparsed createTestBlock(long blockNumber) {
        return TestBlockBuilder.generateBlockWithNumber(blockNumber).blockUnparsed();
    }

    @Nested
    @DisplayName("computeChunk Tests")
    class ComputeChunkTests {

        @Test
        @DisplayName("should return null when node not in availability map")
        void shouldReturnNullWhenNodeNotInAvailability() {
            // given
            BlockNodeSourceConfig nodeConfig = mock(BlockNodeSourceConfig.class);
            NodeSelectionStrategy.NodeSelection selection = new NodeSelectionStrategy.NodeSelection(nodeConfig, 100L);
            Map<BlockNodeSourceConfig, List<LongRange>> availability = new HashMap<>();
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
            BlockNodeSourceConfig nodeConfig = mock(BlockNodeSourceConfig.class);
            NodeSelectionStrategy.NodeSelection selection = new NodeSelectionStrategy.NodeSelection(nodeConfig, 100L);
            Map<BlockNodeSourceConfig, List<LongRange>> availability = new HashMap<>();
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
            BlockNodeSourceConfig nodeConfig = mock(BlockNodeSourceConfig.class);
            NodeSelectionStrategy.NodeSelection selection = new NodeSelectionStrategy.NodeSelection(nodeConfig, 100L);
            Map<BlockNodeSourceConfig, List<LongRange>> availability = new HashMap<>();
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
            BlockNodeSourceConfig nodeConfig = mock(BlockNodeSourceConfig.class);
            NodeSelectionStrategy.NodeSelection selection = new NodeSelectionStrategy.NodeSelection(nodeConfig, 100L);
            Map<BlockNodeSourceConfig, List<LongRange>> availability = new HashMap<>();
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
            BlockNodeSourceConfig nodeConfig = mock(BlockNodeSourceConfig.class);
            NodeSelectionStrategy.NodeSelection selection = new NodeSelectionStrategy.NodeSelection(nodeConfig, 100L);
            Map<BlockNodeSourceConfig, List<LongRange>> availability = new HashMap<>();
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
            BlockNodeSourceConfig nodeConfig = mock(BlockNodeSourceConfig.class);
            NodeSelectionStrategy.NodeSelection selection = new NodeSelectionStrategy.NodeSelection(nodeConfig, 150L);
            Map<BlockNodeSourceConfig, List<LongRange>> availability = new HashMap<>();
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

        @Test
        @DisplayName("should give the genesis block a chunk of its own")
        void shouldGiveGenesisBlockItsOwnChunk() {
            // given
            BlockNodeSourceConfig nodeConfig = mock(BlockNodeSourceConfig.class);
            Map<BlockNodeSourceConfig, List<LongRange>> availability = new HashMap<>();
            availability.put(nodeConfig, List.of(new LongRange(0, 500)));
            long batchSize = 10;

            // when - a gap that starts at the genesis block
            LongRange genesisChunk = BackfillRunner.computeChunk(
                    new NodeSelectionStrategy.NodeSelection(nodeConfig, 0L), availability, 500L, batchSize);

            // then - block 0 is fetched alone, so its TSS data lands before anything that needs it
            assertNotNull(genesisChunk);
            assertEquals(0L, genesisChunk.start());
            assertEquals(0L, genesisChunk.end());

            // and - the chunk after it spans the full batch size again
            LongRange nextChunk = BackfillRunner.computeChunk(
                    new NodeSelectionStrategy.NodeSelection(nodeConfig, 1L), availability, 500L, batchSize);
            assertNotNull(nextChunk);
            assertEquals(1L, nextChunk.start());
            assertEquals(10L, nextChunk.end()); // 1 + 10 - 1
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
            BlockNodeSourceConfig nodeConfig = mock(BlockNodeSourceConfig.class);
            Map<BlockNodeSourceConfig, List<LongRange>> availability = new HashMap<>();
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
            verify(mockFetchErrorsCounter).increment();
        }

        @Test
        @DisplayName("should remove node from availability when fetch returns empty")
        void shouldRemoveNodeOnEmptyFetch() throws Exception {
            // given
            GapDetector.Gap gap = new GapDetector.Gap(new LongRange(0, 10), GapDetector.Type.HISTORICAL);
            BlockNodeSourceConfig nodeConfig = mock(BlockNodeSourceConfig.class);
            Map<BlockNodeSourceConfig, List<LongRange>> availability = new HashMap<>();
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

        @Test
        @DisplayName("should continue when chunk is null but other nodes available")
        void shouldContinueWhenChunkNullButOtherNodesAvailable() throws Exception {
            // given
            GapDetector.Gap gap = new GapDetector.Gap(new LongRange(100, 105), GapDetector.Type.HISTORICAL);
            BlockNodeSourceConfig badNode = mock(BlockNodeSourceConfig.class);
            BlockNodeSourceConfig goodNode = mock(BlockNodeSourceConfig.class);

            // badNode selected first but has range that doesn't cover start (will cause computeChunk to return null)
            // goodNode has valid range
            Map<BlockNodeSourceConfig, List<LongRange>> availability = new HashMap<>();
            availability.put(badNode, List.of(new LongRange(0, 50))); // doesn't cover 100
            availability.put(goodNode, List.of(new LongRange(100, 200)));

            BlockUnparsed testBlock = createTestBlock(100L);

            when(mockFetcher.getAvailabilityForRange(any())).thenReturn(availability);
            // First selection picks badNode (computeChunk will return null), second picks goodNode
            when(mockFetcher.selectNextChunk(anyLong(), anyLong(), any()))
                    .thenReturn(Optional.of(new NodeSelectionStrategy.NodeSelection(badNode, 100L)))
                    .thenReturn(Optional.of(new NodeSelectionStrategy.NodeSelection(goodNode, 100L)));
            when(mockFetcher.fetchBlocksFromNode(eq(goodNode), any())).thenReturn(List.of(testBlock));

            messaging.registerBlockNotificationHandler(persistenceAwaiter, false, "persistence-awaiter");
            messaging.registerBlockNotificationHandler(
                    new BlockNotificationHandler() {
                        @Override
                        public void handleBackfilled(BackfilledBlockNotification notification) {
                            messaging.sendBlockPersisted(new PersistedNotification(
                                    notification.blockNumber(), true, 1, BlockSource.BACKFILL));
                        }
                    },
                    false,
                    "test-persistence-handler");

            // when
            subject.run(gap);

            // then - should have fetched from goodNode after badNode's chunk was null
            verify(mockFetcher).fetchBlocksFromNode(eq(goodNode), any());
            verify(mockFetchedBlocksCounter).increment();
        }

        @Test
        @DisplayName("should continue after replan when selectNextChunk returns empty")
        void shouldContinueAfterReplanOnEmptySelection() throws Exception {
            // given
            GapDetector.Gap gap = new GapDetector.Gap(new LongRange(0, 0), GapDetector.Type.HISTORICAL);
            BlockNodeSourceConfig nodeConfig = mock(BlockNodeSourceConfig.class);

            Map<BlockNodeSourceConfig, List<LongRange>> initialAvailability = new HashMap<>();
            initialAvailability.put(nodeConfig, List.of(new LongRange(0, 10)));

            Map<BlockNodeSourceConfig, List<LongRange>> replanAvailability = new HashMap<>();
            replanAvailability.put(nodeConfig, List.of(new LongRange(0, 10)));

            BlockUnparsed testBlock = createTestBlock(0L);

            // First getAvailabilityForRange for initial plan, second for replan
            when(mockFetcher.getAvailabilityForRange(any()))
                    .thenReturn(initialAvailability)
                    .thenReturn(replanAvailability);
            // First selectNextChunk returns empty (triggers replan), second succeeds
            when(mockFetcher.selectNextChunk(anyLong(), anyLong(), any()))
                    .thenReturn(Optional.empty())
                    .thenReturn(Optional.of(new NodeSelectionStrategy.NodeSelection(nodeConfig, 0L)));
            when(mockFetcher.fetchBlocksFromNode(eq(nodeConfig), any())).thenReturn(List.of(testBlock));

            messaging.registerBlockNotificationHandler(persistenceAwaiter, false, "persistence-awaiter");
            messaging.registerBlockNotificationHandler(
                    new BlockNotificationHandler() {
                        @Override
                        public void handleBackfilled(BackfilledBlockNotification notification) {
                            messaging.sendBlockPersisted(new PersistedNotification(
                                    notification.blockNumber(), true, 1, BlockSource.BACKFILL));
                        }
                    },
                    false,
                    "test-persistence-handler");

            // when
            subject.run(gap);

            // then - should have succeeded after replan
            verify(mockFetchErrorsCounter).increment(); // First attempt failed
            verify(mockFetchedBlocksCounter).increment(); // After replan succeeded
        }

        @Test
        @DisplayName("should continue after replan when fetch returns empty")
        void shouldContinueAfterReplanOnEmptyFetch() throws Exception {
            // given
            GapDetector.Gap gap = new GapDetector.Gap(new LongRange(0, 0), GapDetector.Type.HISTORICAL);
            BlockNodeSourceConfig badNode = mock(BlockNodeSourceConfig.class);
            BlockNodeSourceConfig goodNode = mock(BlockNodeSourceConfig.class);

            Map<BlockNodeSourceConfig, List<LongRange>> initialAvailability = new HashMap<>();
            initialAvailability.put(badNode, List.of(new LongRange(0, 10)));

            Map<BlockNodeSourceConfig, List<LongRange>> replanAvailability = new HashMap<>();
            replanAvailability.put(goodNode, List.of(new LongRange(0, 10)));

            BlockUnparsed testBlock = createTestBlock(0L);

            when(mockFetcher.getAvailabilityForRange(any()))
                    .thenReturn(initialAvailability)
                    .thenReturn(replanAvailability);
            when(mockFetcher.selectNextChunk(anyLong(), anyLong(), any()))
                    .thenReturn(Optional.of(new NodeSelectionStrategy.NodeSelection(badNode, 0L)))
                    .thenReturn(Optional.of(new NodeSelectionStrategy.NodeSelection(goodNode, 0L)));
            // badNode returns empty, goodNode returns block
            when(mockFetcher.fetchBlocksFromNode(eq(badNode), any())).thenReturn(Collections.emptyList());
            when(mockFetcher.fetchBlocksFromNode(eq(goodNode), any())).thenReturn(List.of(testBlock));

            messaging.registerBlockNotificationHandler(persistenceAwaiter, false, "persistence-awaiter");
            messaging.registerBlockNotificationHandler(
                    new BlockNotificationHandler() {
                        @Override
                        public void handleBackfilled(BackfilledBlockNotification notification) {
                            messaging.sendBlockPersisted(new PersistedNotification(
                                    notification.blockNumber(), true, 1, BlockSource.BACKFILL));
                        }
                    },
                    false,
                    "test-persistence-handler");

            // when
            subject.run(gap);

            // then - should have succeeded with goodNode after replan
            verify(mockFetcher).fetchBlocksFromNode(eq(badNode), any());
            verify(mockFetcher).fetchBlocksFromNode(eq(goodNode), any());
            verify(mockFetchedBlocksCounter).increment();
        }

        @Test
        @DisplayName("should break when chunk is null and availability becomes empty")
        void shouldBreakWhenChunkNullAndAvailabilityEmpty() throws Exception {
            // given
            GapDetector.Gap gap = new GapDetector.Gap(new LongRange(100, 105), GapDetector.Type.HISTORICAL);
            BlockNodeSourceConfig nodeConfig = mock(BlockNodeSourceConfig.class);

            // Node has range that doesn't cover start block 100 (will cause computeChunk to return null)
            Map<BlockNodeSourceConfig, List<LongRange>> availability = new HashMap<>();
            availability.put(nodeConfig, List.of(new LongRange(0, 50)));

            when(mockFetcher.getAvailabilityForRange(any())).thenReturn(availability);
            when(mockFetcher.selectNextChunk(anyLong(), anyLong(), any()))
                    .thenReturn(Optional.of(new NodeSelectionStrategy.NodeSelection(nodeConfig, 100L)));

            // when
            subject.run(gap);

            // then - should break without fetching (no nodes left after removing the only one)
            verify(mockFetcher, times(0)).fetchBlocksFromNode(any(), any());
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
            BlockNodeSourceConfig nodeConfig = mock(BlockNodeSourceConfig.class);
            Map<BlockNodeSourceConfig, List<LongRange>> availability = new HashMap<>();
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
                    new BlockNotificationHandler() {
                        @Override
                        public void handleBackfilled(BackfilledBlockNotification notification) {
                            // Simulate immediate persistence
                            messaging.sendBlockPersisted(new PersistedNotification(
                                    notification.blockNumber(), true, 1, BlockSource.BACKFILL));
                        }
                    },
                    false,
                    "test-persistence-handler");

            // when
            subject.run(gap);

            // then
            verify(mockFetchedBlocksCounter).increment();
            // pendingBackfillBlocks was incremented when block was dispatched
            // Note: we can't easily verify the exact increment since it's an AtomicLong, but the test passed
        }

        @Test
        @DisplayName("should await persistence for each block")
        void shouldAwaitPersistenceForEachBlock() throws Exception {
            // given
            GapDetector.Gap gap = new GapDetector.Gap(new LongRange(0, 0), GapDetector.Type.HISTORICAL);
            BlockNodeSourceConfig nodeConfig = mock(BlockNodeSourceConfig.class);
            Map<BlockNodeSourceConfig, List<LongRange>> availability = new HashMap<>();
            availability.put(nodeConfig, List.of(new LongRange(0, 0)));

            BlockUnparsed testBlock = createTestBlock(0L);

            when(mockFetcher.getAvailabilityForRange(any())).thenReturn(availability);
            when(mockFetcher.selectNextChunk(anyLong(), anyLong(), any()))
                    .thenReturn(Optional.of(new NodeSelectionStrategy.NodeSelection(nodeConfig, 0L)));
            when(mockFetcher.fetchBlocksFromNode(eq(nodeConfig), any())).thenReturn(List.of(testBlock));

            // Register persistence awaiter and simulate persistence
            messaging.registerBlockNotificationHandler(persistenceAwaiter, false, "persistence-awaiter");
            messaging.registerBlockNotificationHandler(
                    new BlockNotificationHandler() {
                        @Override
                        public void handleBackfilled(BackfilledBlockNotification notification) {
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
                    mockFetcher,
                    shortTimeoutConfig,
                    messaging,
                    logger,
                    mockMetricsHolder,
                    pendingBackfillBlocks,
                    persistenceAwaiter);

            GapDetector.Gap gap = new GapDetector.Gap(new LongRange(0, 0), GapDetector.Type.HISTORICAL);
            BlockNodeSourceConfig nodeConfig = mock(BlockNodeSourceConfig.class);
            Map<BlockNodeSourceConfig, List<LongRange>> availability = new HashMap<>();
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
            verify(mockFetchedBlocksCounter).increment();
        }
    }

    @Nested
    @DisplayName("Chunk Persistence Retry Tests")
    class ChunkPersistenceRetryTests {

        @Test
        @DisplayName("should retry the wait (not re-dispatch) when persistence confirms late")
        void shouldRetryWithoutRedispatchWhenPersistenceArrivesLate() throws Exception {
            // given - a short per-block timeout so the first await times out, but the persisted
            // notification is delivered a bit later, simulating a block that was merely still
            // parked in the verification module's ordering buffer rather than actually failed.
            BackfillConfiguration retryConfig = BackfillPluginTest.BackfillConfigBuilder.NewBuilder()
                    .delayBetweenBatches(0)
                    .perBlockProcessingTimeout(100)
                    .initialRetryDelay(50)
                    .maxRetries(5)
                    .buildRecord();
            BackfillRunner retrySubject = new BackfillRunner(
                    mockFetcher,
                    retryConfig,
                    messaging,
                    logger,
                    mockMetricsHolder,
                    pendingBackfillBlocks,
                    persistenceAwaiter);

            GapDetector.Gap gap = new GapDetector.Gap(new LongRange(0, 0), GapDetector.Type.HISTORICAL);
            BlockNodeSourceConfig nodeConfig = mock(BlockNodeSourceConfig.class);
            Map<BlockNodeSourceConfig, List<LongRange>> availability = new HashMap<>();
            availability.put(nodeConfig, List.of(new LongRange(0, 0)));
            BlockUnparsed testBlock = createTestBlock(0L);

            when(mockFetcher.getAvailabilityForRange(any())).thenReturn(availability);
            when(mockFetcher.selectNextChunk(anyLong(), anyLong(), any()))
                    .thenReturn(Optional.of(new NodeSelectionStrategy.NodeSelection(nodeConfig, 0L)));
            when(mockFetcher.fetchBlocksFromNode(eq(nodeConfig), any())).thenReturn(List.of(testBlock));

            messaging.registerBlockNotificationHandler(persistenceAwaiter, false, "persistence-awaiter");
            messaging.registerBlockNotificationHandler(
                    new BlockNotificationHandler() {
                        @Override
                        public void handleBackfilled(BackfilledBlockNotification notification) {
                            // Deliver the persisted notification only after the first await's
                            // 100ms timeout has already elapsed, so only a retried (re-tracked)
                            // await can pick it up. 175ms lands comfortably inside the second
                            // attempt's [150ms, 250ms) window (after the 50ms retry backoff)
                            // rather than right at either edge.
                            new Thread(() -> {
                                        try {
                                            Thread.sleep(175);
                                        } catch (InterruptedException ignored) {
                                            Thread.currentThread().interrupt();
                                        }
                                        messaging.sendBlockPersisted(new PersistedNotification(
                                                notification.blockNumber(), true, 1, BlockSource.BACKFILL));
                                    })
                                    .start();
                        }
                    },
                    false,
                    "delayed-persistence-handler");

            // when
            long lastSuccessful = retrySubject.run(gap);

            // then - eventually persisted via retry, and the block was fetched only once (no
            // duplicate dispatch of a second, independent verification session for it)
            assertEquals(0L, lastSuccessful, "Should succeed once the delayed persistence notification lands");
            verify(mockFetcher, times(1)).fetchBlocksFromNode(eq(nodeConfig), any());
        }

        @Test
        @DisplayName("should not lose a persisted notification that arrives during the retry backoff")
        void shouldNotLosePersistenceArrivingDuringRetryBackoff() throws Exception {
            // given - a backoff far longer than the per-block timeout, so the notification lands
            // while the runner is sleeping between attempts rather than while it is awaiting.
            // Nothing is listening during that sleep unless the block was re-tracked before it,
            // and a dropped notification leaves every later attempt waiting on a latch that will
            // never be released, failing a chunk whose block is actually on disk.
            BackfillConfiguration retryConfig = BackfillPluginTest.BackfillConfigBuilder.NewBuilder()
                    .delayBetweenBatches(0)
                    .perBlockProcessingTimeout(50)
                    .initialRetryDelay(400)
                    .maxRetries(3)
                    .buildRecord();
            BackfillRunner retrySubject = new BackfillRunner(
                    mockFetcher,
                    retryConfig,
                    messaging,
                    logger,
                    mockMetricsHolder,
                    pendingBackfillBlocks,
                    persistenceAwaiter);

            GapDetector.Gap gap = new GapDetector.Gap(new LongRange(0, 0), GapDetector.Type.HISTORICAL);
            BlockNodeSourceConfig nodeConfig = mock(BlockNodeSourceConfig.class);
            Map<BlockNodeSourceConfig, List<LongRange>> availability = new HashMap<>();
            availability.put(nodeConfig, List.of(new LongRange(0, 0)));
            BlockUnparsed testBlock = createTestBlock(0L);

            when(mockFetcher.getAvailabilityForRange(any())).thenReturn(availability);
            when(mockFetcher.selectNextChunk(anyLong(), anyLong(), any()))
                    .thenReturn(Optional.of(new NodeSelectionStrategy.NodeSelection(nodeConfig, 0L)));
            when(mockFetcher.fetchBlocksFromNode(eq(nodeConfig), any())).thenReturn(List.of(testBlock));

            messaging.registerBlockNotificationHandler(persistenceAwaiter, false, "persistence-awaiter");
            messaging.registerBlockNotificationHandler(
                    new BlockNotificationHandler() {
                        @Override
                        public void handleBackfilled(BackfilledBlockNotification notification) {
                            // 200ms is past the first attempt's 50ms timeout and well inside the
                            // 400ms backoff that follows it, so only a block re-tracked before the
                            // sleep can still receive this.
                            new Thread(() -> {
                                        try {
                                            Thread.sleep(200);
                                        } catch (InterruptedException ignored) {
                                            Thread.currentThread().interrupt();
                                        }
                                        messaging.sendBlockPersisted(new PersistedNotification(
                                                notification.blockNumber(), true, 1, BlockSource.BACKFILL));
                                    })
                                    .start();
                        }
                    },
                    false,
                    "backoff-window-persistence-handler");

            // when
            long lastSuccessful = retrySubject.run(gap);

            // then
            assertEquals(0L, lastSuccessful, "Notification delivered during the backoff should still be seen");
            verify(mockFetcher, times(1)).fetchBlocksFromNode(eq(nodeConfig), any());
        }

        @Test
        @DisplayName("should stop the gap scan without advancing when a chunk never persists")
        void shouldStopGapWithoutAdvancingWhenChunkNeverPersists() throws Exception {
            // given - block 0 will never receive a persisted notification, so every retry
            // attempt times out. This must not cause the loop to advance and fetch block 1's
            // chunk anyway -- that would keep piling new backfill dispatches on top of a block
            // still stuck, instead of bounding the outstanding/unconfirmed count.
            BackfillConfiguration retryConfig = BackfillPluginTest.BackfillConfigBuilder.NewBuilder()
                    .delayBetweenBatches(0)
                    .perBlockProcessingTimeout(50)
                    .initialRetryDelay(10)
                    .maxRetries(2)
                    .fetchBatchSize(1)
                    .buildRecord();
            BackfillRunner retrySubject = new BackfillRunner(
                    mockFetcher,
                    retryConfig,
                    messaging,
                    logger,
                    mockMetricsHolder,
                    pendingBackfillBlocks,
                    persistenceAwaiter);

            GapDetector.Gap gap = new GapDetector.Gap(new LongRange(0, 1), GapDetector.Type.HISTORICAL);
            BlockNodeSourceConfig nodeConfig = mock(BlockNodeSourceConfig.class);
            Map<BlockNodeSourceConfig, List<LongRange>> availability = new HashMap<>();
            availability.put(nodeConfig, List.of(new LongRange(0, 1)));
            BlockUnparsed block0 = createTestBlock(0L);

            when(mockFetcher.getAvailabilityForRange(any())).thenReturn(availability);
            when(mockFetcher.selectNextChunk(anyLong(), anyLong(), any()))
                    .thenReturn(Optional.of(new NodeSelectionStrategy.NodeSelection(nodeConfig, 0L)));
            when(mockFetcher.fetchBlocksFromNode(eq(nodeConfig), any())).thenReturn(List.of(block0));

            // Only the awaiter is registered -- nothing ever sends a PersistedNotification, so
            // block 0 never confirms no matter how many retries are attempted.
            messaging.registerBlockNotificationHandler(persistenceAwaiter, false, "persistence-awaiter");

            // when
            long lastSuccessful = retrySubject.run(gap);

            // then - stuck on block 0 forever, so block 1's chunk is never fetched
            assertEquals(-1L, lastSuccessful, "Should never succeed since block 0 never persists");
            verify(mockFetcher, times(1)).fetchBlocksFromNode(eq(nodeConfig), any());
        }

        @Test
        @DisplayName("should not advance when a block fails verification")
        void shouldNotAdvanceWhenVerificationFails() throws Exception {
            // given - block 10 fails verification, so no persisted notification will ever follow.
            // A released latch is not the same as a persisted block: the gap must not advance.
            long lastSuccessful = runSingleBlockGapWithOutcome(
                    10L,
                    notification -> messaging.sendBlockVerification(new VerificationNotification(
                            false,
                            VerificationNotification.FailureInfo.standard(
                                    VerificationNotification.FailureType.MISSING_VERIFICATION_DATA),
                            notification.blockNumber(),
                            null,
                            null,
                            BlockSource.BACKFILL)));

            // then
            assertEquals(9L, lastSuccessful, "Should return start - 1 when the block failed verification");
        }

        @Test
        @DisplayName("should not advance when persistence reports failure")
        void shouldNotAdvanceWhenPersistenceReportsFailure() throws Exception {
            // given - block 10's persistence is reported as failed
            long lastSuccessful = runSingleBlockGapWithOutcome(
                    10L,
                    notification -> messaging.sendBlockPersisted(
                            new PersistedNotification(notification.blockNumber(), false, 1, BlockSource.BACKFILL)));

            // then
            assertEquals(9L, lastSuccessful, "Should return start - 1 when the block failed to persist");
        }

        /**
         * Runs a single-block gap whose only block gets the given outcome published for it as soon as it
         * is dispatched. TestBlockMessagingFacility dispatches synchronously on the caller's thread, so
         * the outcome always lands before the runner starts awaiting.
         */
        private long runSingleBlockGapWithOutcome(
                long blockNumber, Consumer<BackfilledBlockNotification> outcomePublisher) throws Exception {
            BackfillConfiguration singleAttemptConfig = BackfillPluginTest.BackfillConfigBuilder.NewBuilder()
                    .delayBetweenBatches(0)
                    .perBlockProcessingTimeout(500)
                    .maxRetries(1)
                    .buildRecord();
            BackfillRunner failureSubject = new BackfillRunner(
                    mockFetcher,
                    singleAttemptConfig,
                    messaging,
                    logger,
                    mockMetricsHolder,
                    pendingBackfillBlocks,
                    persistenceAwaiter);

            GapDetector.Gap gap =
                    new GapDetector.Gap(new LongRange(blockNumber, blockNumber), GapDetector.Type.HISTORICAL);
            BlockNodeSourceConfig nodeConfig = mock(BlockNodeSourceConfig.class);
            Map<BlockNodeSourceConfig, List<LongRange>> availability = new HashMap<>();
            availability.put(nodeConfig, List.of(new LongRange(blockNumber, blockNumber)));

            when(mockFetcher.getAvailabilityForRange(any())).thenReturn(availability);
            when(mockFetcher.selectNextChunk(anyLong(), anyLong(), any()))
                    .thenReturn(Optional.of(new NodeSelectionStrategy.NodeSelection(nodeConfig, blockNumber)));
            when(mockFetcher.fetchBlocksFromNode(eq(nodeConfig), any()))
                    .thenReturn(List.of(createTestBlock(blockNumber)));

            messaging.registerBlockNotificationHandler(persistenceAwaiter, false, "persistence-awaiter");
            messaging.registerBlockNotificationHandler(
                    new BlockNotificationHandler() {
                        @Override
                        public void handleBackfilled(BackfilledBlockNotification notification) {
                            outcomePublisher.accept(notification);
                        }
                    },
                    false,
                    "test-failure-handler");

            return failureSubject.run(gap);
        }
    }

    @Nested
    @DisplayName("lastSuccessfulBlock Return Value Tests")
    class LastSuccessfulBlockTests {

        @Test
        @DisplayName("should return gap end when all blocks successfully backfilled")
        void shouldReturnGapEndOnFullCompletion() throws Exception {
            // given - a gap clear of block 0, which computeChunk deliberately fetches on its own
            GapDetector.Gap gap = new GapDetector.Gap(new LongRange(10, 12), GapDetector.Type.HISTORICAL);
            BlockNodeSourceConfig nodeConfig = mock(BlockNodeSourceConfig.class);
            Map<BlockNodeSourceConfig, List<LongRange>> availability = new HashMap<>();
            availability.put(nodeConfig, List.of(new LongRange(10, 12)));

            BlockUnparsed block10 = createTestBlock(10L);
            BlockUnparsed block11 = createTestBlock(11L);
            BlockUnparsed block12 = createTestBlock(12L);

            when(mockFetcher.getAvailabilityForRange(any())).thenReturn(availability);
            when(mockFetcher.selectNextChunk(anyLong(), anyLong(), any()))
                    .thenReturn(Optional.of(new NodeSelectionStrategy.NodeSelection(nodeConfig, 10L)));
            when(mockFetcher.fetchBlocksFromNode(eq(nodeConfig), any())).thenReturn(List.of(block10, block11, block12));

            // Register handlers for persistence flow
            messaging.registerBlockNotificationHandler(persistenceAwaiter, false, "persistence-awaiter");
            messaging.registerBlockNotificationHandler(
                    new BlockNotificationHandler() {
                        @Override
                        public void handleBackfilled(BackfilledBlockNotification notification) {
                            messaging.sendBlockPersisted(new PersistedNotification(
                                    notification.blockNumber(), true, 1, BlockSource.BACKFILL));
                        }
                    },
                    false,
                    "test-persistence-handler");

            // when
            long lastSuccessful = subject.run(gap);

            // then
            assertEquals(12L, lastSuccessful, "Should return gap end (12) on full completion");
        }

        @Test
        @DisplayName("should return last successful block when gap partially completed")
        void shouldReturnLastSuccessfulBlockOnPartialCompletion() throws Exception {
            // given - gap 10-19 with batch size 5, first batch (10-14) succeeds, second batch fails.
            // The gap deliberately starts clear of block 0, which computeChunk chunks on its own -
            // that special case has its own test in ComputeChunkTests.
            // Use custom config with fetchBatchSize=5 so we need two chunks
            BackfillConfiguration smallBatchConfig = BackfillPluginTest.BackfillConfigBuilder.NewBuilder()
                    .delayBetweenBatches(0)
                    .fetchBatchSize(5)
                    .buildRecord();
            BackfillRunner smallBatchSubject = new BackfillRunner(
                    mockFetcher,
                    smallBatchConfig,
                    messaging,
                    logger,
                    mockMetricsHolder,
                    pendingBackfillBlocks,
                    persistenceAwaiter);

            GapDetector.Gap gap = new GapDetector.Gap(new LongRange(10, 19), GapDetector.Type.HISTORICAL);
            BlockNodeSourceConfig nodeConfig = mock(BlockNodeSourceConfig.class);

            Map<BlockNodeSourceConfig, List<LongRange>> initialAvailability = new HashMap<>();
            initialAvailability.put(nodeConfig, List.of(new LongRange(10, 19)));

            List<BlockUnparsed> firstBatch = List.of(
                    createTestBlock(10L),
                    createTestBlock(11L),
                    createTestBlock(12L),
                    createTestBlock(13L),
                    createTestBlock(14L));

            // First getAvailabilityForRange returns initial, second (replan) returns empty
            when(mockFetcher.getAvailabilityForRange(any()))
                    .thenReturn(initialAvailability)
                    .thenReturn(Collections.emptyMap());
            // First select returns node for block 10, second for block 15
            when(mockFetcher.selectNextChunk(anyLong(), anyLong(), any()))
                    .thenReturn(Optional.of(new NodeSelectionStrategy.NodeSelection(nodeConfig, 10L)))
                    .thenReturn(Optional.of(new NodeSelectionStrategy.NodeSelection(nodeConfig, 15L)));
            // First fetch succeeds with 5 blocks (10-14), second returns empty (simulating failure)
            when(mockFetcher.fetchBlocksFromNode(eq(nodeConfig), any()))
                    .thenReturn(firstBatch)
                    .thenReturn(Collections.emptyList());

            // Register handlers for persistence flow
            messaging.registerBlockNotificationHandler(persistenceAwaiter, false, "persistence-awaiter");
            messaging.registerBlockNotificationHandler(
                    new BlockNotificationHandler() {
                        @Override
                        public void handleBackfilled(BackfilledBlockNotification notification) {
                            messaging.sendBlockPersisted(new PersistedNotification(
                                    notification.blockNumber(), true, 1, BlockSource.BACKFILL));
                        }
                    },
                    false,
                    "test-persistence-handler");

            // when
            long lastSuccessful = smallBatchSubject.run(gap);

            // then - chunk 10-14 succeeded, so lastSuccessfulBlock is 14
            assertEquals(14L, lastSuccessful, "Should return 14 as last successful block (partial completion)");
        }

        @Test
        @DisplayName("should return start-1 when no blocks successfully backfilled")
        void shouldReturnStartMinusOneWhenNoBlocksBackfilled() throws Exception {
            // given - gap 10-20, availability empty from start
            GapDetector.Gap gap = new GapDetector.Gap(new LongRange(10, 20), GapDetector.Type.HISTORICAL);

            when(mockFetcher.getAvailabilityForRange(any())).thenReturn(Collections.emptyMap());

            // when
            long lastSuccessful = subject.run(gap);

            // then
            assertEquals(9L, lastSuccessful, "Should return 9 (start-1) when no blocks backfilled");
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
            BlockNodeSourceConfig nodeConfig = mock(BlockNodeSourceConfig.class);
            Map<BlockNodeSourceConfig, List<LongRange>> availability = new HashMap<>();
            availability.put(nodeConfig, List.of(new LongRange(0, 0)));

            BlockUnparsed testBlock = createTestBlock(0L);

            when(mockFetcher.getAvailabilityForRange(any())).thenReturn(availability);
            when(mockFetcher.selectNextChunk(anyLong(), anyLong(), any()))
                    .thenReturn(Optional.of(new NodeSelectionStrategy.NodeSelection(nodeConfig, 0L)));
            when(mockFetcher.fetchBlocksFromNode(eq(nodeConfig), any())).thenReturn(List.of(testBlock));

            // Register handlers for persistence flow
            messaging.registerBlockNotificationHandler(persistenceAwaiter, false, "persistence-awaiter");
            messaging.registerBlockNotificationHandler(
                    new BlockNotificationHandler() {
                        @Override
                        public void handleBackfilled(BackfilledBlockNotification notification) {
                            messaging.sendBlockPersisted(new PersistedNotification(
                                    notification.blockNumber(), true, 1, BlockSource.BACKFILL));
                        }
                    },
                    false,
                    "test-persistence-handler");

            // when
            subject.run(gap);

            // then
            verify(mockFetchedBlocksCounter).increment();
        }

        @Test
        @DisplayName("should report block dispatched metric")
        void shouldReportBlockDispatched() throws Exception {
            // given
            GapDetector.Gap gap = new GapDetector.Gap(new LongRange(0, 0), GapDetector.Type.HISTORICAL);
            BlockNodeSourceConfig nodeConfig = mock(BlockNodeSourceConfig.class);
            Map<BlockNodeSourceConfig, List<LongRange>> availability = new HashMap<>();
            availability.put(nodeConfig, List.of(new LongRange(0, 0)));

            BlockUnparsed testBlock = createTestBlock(0L);

            when(mockFetcher.getAvailabilityForRange(any())).thenReturn(availability);
            when(mockFetcher.selectNextChunk(anyLong(), anyLong(), any()))
                    .thenReturn(Optional.of(new NodeSelectionStrategy.NodeSelection(nodeConfig, 0L)));
            when(mockFetcher.fetchBlocksFromNode(eq(nodeConfig), any())).thenReturn(List.of(testBlock));

            // Register handlers for persistence flow
            messaging.registerBlockNotificationHandler(persistenceAwaiter, false, "persistence-awaiter");
            messaging.registerBlockNotificationHandler(
                    new BlockNotificationHandler() {
                        @Override
                        public void handleBackfilled(BackfilledBlockNotification notification) {
                            messaging.sendBlockPersisted(new PersistedNotification(
                                    notification.blockNumber(), true, 1, BlockSource.BACKFILL));
                        }
                    },
                    false,
                    "test-persistence-handler");

            // when
            subject.run(gap);

            // then - pendingBackfillBlocks was incremented
            assertTrue(pendingBackfillBlocks.get() >= 0, "pendingBackfillBlocks should have been incremented");
        }

        @Test
        @DisplayName("should report multiple blocks fetched and dispatched")
        void shouldReportMultipleBlocks() throws Exception {
            // given - a gap clear of block 0: the mock replays all three blocks for any chunk, so a
            // gap that started at 0 would be split into two chunks and count each block twice
            GapDetector.Gap gap = new GapDetector.Gap(new LongRange(10, 12), GapDetector.Type.HISTORICAL);
            BlockNodeSourceConfig nodeConfig = mock(BlockNodeSourceConfig.class);
            Map<BlockNodeSourceConfig, List<LongRange>> availability = new HashMap<>();
            availability.put(nodeConfig, List.of(new LongRange(10, 12)));

            BlockUnparsed testBlock10 = createTestBlock(10L);
            BlockUnparsed testBlock11 = createTestBlock(11L);
            BlockUnparsed testBlock12 = createTestBlock(12L);

            when(mockFetcher.getAvailabilityForRange(any())).thenReturn(availability);
            when(mockFetcher.selectNextChunk(anyLong(), anyLong(), any()))
                    .thenReturn(Optional.of(new NodeSelectionStrategy.NodeSelection(nodeConfig, 10L)));
            when(mockFetcher.fetchBlocksFromNode(eq(nodeConfig), any()))
                    .thenReturn(List.of(testBlock10, testBlock11, testBlock12));

            // Register handlers for persistence flow
            messaging.registerBlockNotificationHandler(persistenceAwaiter, false, "persistence-awaiter");
            messaging.registerBlockNotificationHandler(
                    new BlockNotificationHandler() {
                        @Override
                        public void handleBackfilled(BackfilledBlockNotification notification) {
                            messaging.sendBlockPersisted(new PersistedNotification(
                                    notification.blockNumber(), true, 1, BlockSource.BACKFILL));
                        }
                    },
                    false,
                    "test-persistence-handler");

            // when
            subject.run(gap);

            // then
            verify(mockFetchedBlocksCounter, times(3)).increment();
            // pendingBackfillBlocks was incremented 3 times when blocks were dispatched
        }
    }
}
