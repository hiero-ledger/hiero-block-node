// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import org.hiero.block.node.spi.historicalblocks.LongRange;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/// Unit tests for how [BackfillPlugin] hands gaps to its schedulers and keeps the live-tail high-water mark.
class BackfillPluginGapSchedulingTest {

    private BackfillPlugin plugin;

    @BeforeEach
    void setUp() {
        plugin = new BackfillPlugin();
    }

    /// A scheduler whose worker never runs, so submitted gaps stay queued and a small capacity fills up.
    private static BackfillTaskScheduler idleScheduler(final int queueCapacity) {
        return new BackfillTaskScheduler(
                mock(ExecutorService.class),
                gap -> {},
                queueCapacity,
                mock(BackfillFetcher.class),
                mock(BackfillPersistenceAwaiter.class));
    }

    /// A scheduler whose executor rejects the worker, making `submit` throw after the gap is queued.
    private static BackfillTaskScheduler throwingScheduler() {
        final ExecutorService executor = mock(ExecutorService.class);
        when(executor.submit(any(Runnable.class))).thenThrow(new RejectedExecutionException("executor is down"));
        return new BackfillTaskScheduler(
                executor, gap -> {}, 10, mock(BackfillFetcher.class), mock(BackfillPersistenceAwaiter.class));
    }

    private static GapDetector.Gap liveTailGap(final long start, final long end) {
        return new GapDetector.Gap(new LongRange(start, end), GapDetector.Type.LIVE_TAIL);
    }

    private static GapDetector.Gap historicalGap(final long start, final long end) {
        return new GapDetector.Gap(new LongRange(start, end), GapDetector.Type.HISTORICAL);
    }

    @Nested
    @DisplayName("submitGap")
    class SubmitGapTests {

        @Test
        @DisplayName("Returns false instead of throwing when the target scheduler does not exist yet")
        void nullSchedulerReturnsFalse() {
            assertDoesNotThrow(() -> {
                assertFalse(plugin.submitGap(liveTailGap(0, 9)));
                assertFalse(plugin.submitGap(historicalGap(0, 9)));
            });
        }

        @Test
        @DisplayName("Returns false instead of propagating a RuntimeException from the scheduler")
        void schedulerFailureReturnsFalse() {
            plugin.liveTailScheduler = throwingScheduler();
            plugin.historicalScheduler = throwingScheduler();

            assertDoesNotThrow(() -> {
                assertFalse(plugin.submitGap(liveTailGap(0, 9)));
                assertFalse(plugin.submitGap(historicalGap(0, 9)));
            });
        }

        @Test
        @DisplayName("Returns false for a historical gap when its queue is full")
        void historicalQueueFullReturnsFalse() {
            plugin.historicalScheduler = idleScheduler(1);

            assertTrue(plugin.submitGap(historicalGap(0, 9)));
            assertFalse(plugin.submitGap(historicalGap(10, 19)));
        }
    }

    @Nested
    @DisplayName("scheduleGap for live-tail gaps")
    class LiveTailScheduleGapTests {

        @Test
        @DisplayName("A submitted gap advances the high-water mark and a repeat is deduplicated")
        void successfulSubmitAdvancesMarkAndDeduplicates() {
            final BackfillTaskScheduler scheduler = idleScheduler(10);
            plugin.liveTailScheduler = scheduler;

            assertTrue(plugin.scheduleGap(liveTailGap(0, 9)));
            assertEquals(9, plugin.liveTailHighWaterMark.get());

            assertFalse(plugin.scheduleGap(liveTailGap(0, 9)), "The same range must not be queued twice");
            assertEquals(1, scheduler.queueSize());
        }

        @Test
        @DisplayName("A partially overlapping gap is trimmed to the part above the high-water mark")
        void overlappingGapIsTrimmed() {
            final BackfillTaskScheduler scheduler = idleScheduler(10);
            plugin.liveTailScheduler = scheduler;
            assertTrue(plugin.scheduleGap(liveTailGap(0, 9)));

            assertTrue(plugin.scheduleGap(liveTailGap(5, 14)));

            assertEquals(14, plugin.liveTailHighWaterMark.get());
            assertEquals(2, scheduler.queueSize());
        }

        @Test
        @DisplayName("A rejected gap leaves the high-water mark unchanged so the range can be queued again")
        void queueFullDoesNotAdvanceMark() {
            plugin.liveTailScheduler = idleScheduler(1);
            assertTrue(plugin.scheduleGap(liveTailGap(0, 9)));

            assertFalse(plugin.scheduleGap(liveTailGap(10, 19)), "The queue is full");
            assertEquals(9, plugin.liveTailHighWaterMark.get());

            plugin.liveTailScheduler = idleScheduler(2);
            assertTrue(plugin.scheduleGap(liveTailGap(10, 19)), "The same range is accepted once there is room");
            assertEquals(19, plugin.liveTailHighWaterMark.get());
        }

        @Test
        @DisplayName("A scheduler failure is not propagated and leaves the high-water mark unchanged")
        void schedulerFailureDoesNotAdvanceMark() {
            plugin.liveTailScheduler = throwingScheduler();

            assertDoesNotThrow(() -> assertFalse(plugin.scheduleGap(liveTailGap(0, 9))));
            assertEquals(-1, plugin.liveTailHighWaterMark.get());

            plugin.liveTailScheduler = idleScheduler(10);
            assertTrue(plugin.scheduleGap(liveTailGap(0, 9)), "The range is accepted once the scheduler works");
            assertEquals(9, plugin.liveTailHighWaterMark.get());
        }

        @Test
        @DisplayName("A missing scheduler leaves the high-water mark unchanged")
        void missingSchedulerDoesNotAdvanceMark() {
            assertFalse(plugin.scheduleGap(liveTailGap(0, 9)));
            assertEquals(-1, plugin.liveTailHighWaterMark.get());
        }

        @Test
        @DisplayName("A failed submission restores the mark to its value before the reservation after a worker reset")
        void rollbackRestoresMarkAfterWorkerReset() {
            plugin.liveTailScheduler = idleScheduler(10);
            assertTrue(plugin.scheduleGap(liveTailGap(0, 19)));

            // A worker ends the gap early and pulls the mark back down, as GapProcessor does.
            plugin.liveTailHighWaterMark.set(4);
            plugin.liveTailScheduler = idleScheduler(1);
            plugin.liveTailScheduler.submit(liveTailGap(100, 100));

            assertFalse(plugin.scheduleGap(liveTailGap(5, 19)), "The queue is full");
            assertEquals(4, plugin.liveTailHighWaterMark.get());
        }

        @Test
        @DisplayName("A worker reset made while a submission is failing is not overwritten by the rollback")
        void rollbackKeepsWorkerResetMadeDuringSubmit() {
            plugin.liveTailScheduler = idleScheduler(10);
            assertTrue(plugin.scheduleGap(liveTailGap(0, 19)));

            final ExecutorService executor = mock(ExecutorService.class);
            when(executor.submit(any(Runnable.class))).thenAnswer(invocation -> {
                // A worker ends a gap early and pulls the mark down while this submission is in flight.
                plugin.liveTailHighWaterMark.set(4);
                throw new RejectedExecutionException("executor is down");
            });
            plugin.liveTailScheduler = new BackfillTaskScheduler(
                    executor, gap -> {}, 10, mock(BackfillFetcher.class), mock(BackfillPersistenceAwaiter.class));

            assertFalse(plugin.scheduleGap(liveTailGap(10, 25)));
            assertEquals(4, plugin.liveTailHighWaterMark.get(), "The worker's reset must survive the rollback");
        }
    }

    @Nested
    @DisplayName("scheduleGap for historical gaps")
    class HistoricalScheduleGapTests {

        @Test
        @DisplayName("Returns true when the gap is accepted")
        void acceptedGapReturnsTrue() {
            plugin.historicalScheduler = idleScheduler(10);

            assertTrue(plugin.scheduleGap(historicalGap(0, 9)));
        }

        @Test
        @DisplayName("Returns false when the scheduler fails or is missing")
        void failedSubmissionReturnsFalse() {
            assertFalse(plugin.scheduleGap(historicalGap(0, 9)), "No scheduler yet");

            plugin.historicalScheduler = throwingScheduler();
            assertDoesNotThrow(() -> assertFalse(plugin.scheduleGap(historicalGap(0, 9))));
        }

        @Test
        @DisplayName("Does not touch the live-tail high-water mark")
        void doesNotChangeLiveTailMark() {
            plugin.historicalScheduler = idleScheduler(10);

            plugin.scheduleGap(historicalGap(0, 9));

            assertEquals(-1, plugin.liveTailHighWaterMark.get());
        }
    }
}
