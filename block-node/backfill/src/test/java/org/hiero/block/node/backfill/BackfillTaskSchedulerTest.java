// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.hiero.block.node.spi.historicalblocks.LongRange;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Unit tests for {@link BackfillTaskScheduler}.
 */
@Timeout(value = 5, unit = TimeUnit.SECONDS)
class BackfillTaskSchedulerTest {

    private ExecutorService executor;
    private BackfillFetcher mockFetcher;
    private BackfillPersistenceAwaiter mockAwaiter;
    private List<GapDetector.Gap> processedGaps;
    private CountDownLatch processingLatch;

    @BeforeEach
    void setUp() {
        executor = Executors.newSingleThreadExecutor();
        mockFetcher = mock(BackfillFetcher.class);
        mockAwaiter = mock(BackfillPersistenceAwaiter.class);
        processedGaps = Collections.synchronizedList(new ArrayList<>());
        processingLatch = new CountDownLatch(1);
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    private BackfillTaskScheduler createScheduler(int queueCapacity) {
        return createScheduler(queueCapacity, gap -> {
            processedGaps.add(gap);
            processingLatch.countDown();
        });
    }

    private BackfillTaskScheduler createScheduler(int queueCapacity, Consumer<GapDetector.Gap> processor) {
        return new BackfillTaskScheduler(executor, processor, queueCapacity, mockFetcher, mockAwaiter);
    }

    private GapDetector.Gap createGap(long start, long end, GapDetector.Type type) {
        return new GapDetector.Gap(new LongRange(start, end), type);
    }

    @Nested
    @DisplayName("submit Tests")
    class SubmitTests {

        @Test
        @DisplayName("should accept gap when queue has capacity")
        void shouldAcceptGapWhenQueueHasCapacity() {
            // given
            BackfillTaskScheduler scheduler = createScheduler(10);
            GapDetector.Gap gap = createGap(0, 10, GapDetector.Type.HISTORICAL);

            // when
            boolean accepted = scheduler.submit(gap);

            // then
            assertTrue(accepted);

            scheduler.close();
        }

        @Test
        @DisplayName("should reject gap when queue is full")
        void shouldRejectGapWhenQueueIsFull() throws InterruptedException {
            // given - scheduler with capacity 2 and slow processor
            CountDownLatch blockProcessing = new CountDownLatch(1);
            BackfillTaskScheduler scheduler = createScheduler(2, gap -> {
                try {
                    blockProcessing.await(); // Block processing
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            // Submit first gap (will start processing and block)
            scheduler.submit(createGap(0, 10, GapDetector.Type.HISTORICAL));
            Thread.sleep(50); // Give time for worker to start

            // Fill the queue
            scheduler.submit(createGap(11, 20, GapDetector.Type.HISTORICAL));
            scheduler.submit(createGap(21, 30, GapDetector.Type.HISTORICAL));

            // when - queue is now full
            boolean accepted = scheduler.submit(createGap(31, 40, GapDetector.Type.HISTORICAL));

            // then
            assertFalse(accepted);

            blockProcessing.countDown();
            scheduler.close();
        }

        @Test
        @DisplayName("should reject gap after shutdown")
        void shouldRejectGapAfterShutdown() {
            // given
            BackfillTaskScheduler scheduler = createScheduler(10);
            scheduler.close();

            // when
            boolean accepted = scheduler.submit(createGap(0, 10, GapDetector.Type.HISTORICAL));

            // then
            assertFalse(accepted);
        }

        @Test
        @DisplayName("should start worker on first submit")
        void shouldStartWorkerOnFirstSubmit() throws InterruptedException {
            // given
            CountDownLatch workerStarted = new CountDownLatch(1);
            BackfillTaskScheduler scheduler = createScheduler(10, gap -> {
                workerStarted.countDown();
            });

            // when
            scheduler.submit(createGap(0, 10, GapDetector.Type.HISTORICAL));

            // then
            boolean started = workerStarted.await(1, TimeUnit.SECONDS);
            assertTrue(started);

            scheduler.close();
        }
    }

    @Nested
    @DisplayName("queueSize Tests")
    class QueueSizeTests {

        @Test
        @DisplayName("should return zero for empty queue")
        void shouldReturnZeroForEmptyQueue() {
            // given
            BackfillTaskScheduler scheduler = createScheduler(10);

            // when
            int size = scheduler.queueSize();

            // then
            assertEquals(0, size);

            scheduler.close();
        }

        @Test
        @DisplayName("should return correct size after submissions")
        void shouldReturnCorrectSizeAfterSubmissions() throws InterruptedException {
            // given - block the processor
            CountDownLatch blockProcessing = new CountDownLatch(1);
            BackfillTaskScheduler scheduler = createScheduler(10, gap -> {
                try {
                    blockProcessing.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            // Submit first (starts processing)
            scheduler.submit(createGap(0, 10, GapDetector.Type.HISTORICAL));
            Thread.sleep(50); // Give time for worker to start

            // Submit more to queue
            scheduler.submit(createGap(11, 20, GapDetector.Type.HISTORICAL));
            scheduler.submit(createGap(21, 30, GapDetector.Type.HISTORICAL));

            // when
            int size = scheduler.queueSize();

            // then - first gap is being processed, 2 in queue
            assertEquals(2, size);

            blockProcessing.countDown();
            scheduler.close();
        }
    }

    @Nested
    @DisplayName("isRunning Tests")
    class IsRunningTests {

        @Test
        @DisplayName("should return false initially")
        void shouldReturnFalseInitially() {
            // given
            BackfillTaskScheduler scheduler = createScheduler(10);

            // when
            boolean running = scheduler.isRunning();

            // then
            assertFalse(running);

            scheduler.close();
        }

        @Test
        @DisplayName("should return true while processing")
        void shouldReturnTrueWhileProcessing() throws InterruptedException {
            // given
            CountDownLatch processingStarted = new CountDownLatch(1);
            CountDownLatch blockProcessing = new CountDownLatch(1);
            BackfillTaskScheduler scheduler = createScheduler(10, gap -> {
                processingStarted.countDown();
                try {
                    blockProcessing.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            scheduler.submit(createGap(0, 10, GapDetector.Type.HISTORICAL));
            processingStarted.await(1, TimeUnit.SECONDS);

            // when
            boolean running = scheduler.isRunning();

            // then
            assertTrue(running);

            blockProcessing.countDown();
            scheduler.close();
        }

        @Test
        @DisplayName("should return false after processing completes")
        void shouldReturnFalseAfterProcessingCompletes() throws InterruptedException {
            // given
            CountDownLatch processingDone = new CountDownLatch(1);
            BackfillTaskScheduler scheduler = createScheduler(10, gap -> {
                processingDone.countDown();
            });

            scheduler.submit(createGap(0, 10, GapDetector.Type.HISTORICAL));
            processingDone.await(1, TimeUnit.SECONDS);
            Thread.sleep(50); // Give time for worker to finish

            // when
            boolean running = scheduler.isRunning();

            // then
            assertFalse(running);

            scheduler.close();
        }
    }

    @Nested
    @DisplayName("close Tests")
    class CloseTests {

        @Test
        @DisplayName("should stop accepting new gaps after close")
        void shouldStopAcceptingNewGaps() {
            // given
            BackfillTaskScheduler scheduler = createScheduler(10);

            // when
            scheduler.close();
            boolean accepted = scheduler.submit(createGap(0, 10, GapDetector.Type.HISTORICAL));

            // then
            assertFalse(accepted);
        }

        @Test
        @DisplayName("should clear queue on close")
        void shouldClearQueueOnClose() throws InterruptedException {
            // given - block processing and fill queue
            CountDownLatch blockProcessing = new CountDownLatch(1);
            BackfillTaskScheduler scheduler = createScheduler(10, gap -> {
                try {
                    blockProcessing.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            scheduler.submit(createGap(0, 10, GapDetector.Type.HISTORICAL));
            Thread.sleep(50);
            scheduler.submit(createGap(11, 20, GapDetector.Type.HISTORICAL));
            scheduler.submit(createGap(21, 30, GapDetector.Type.HISTORICAL));

            assertEquals(2, scheduler.queueSize());

            // when
            scheduler.close();

            // then
            assertEquals(0, scheduler.queueSize());

            blockProcessing.countDown();
        }
    }

    @Nested
    @DisplayName("drain Tests")
    class DrainTests {

        @Test
        @DisplayName("should process all queued gaps in order")
        void shouldProcessAllQueuedGapsInOrder() throws InterruptedException {
            // given
            List<GapDetector.Gap> processed = Collections.synchronizedList(new ArrayList<>());
            CountDownLatch allProcessed = new CountDownLatch(3);
            BackfillTaskScheduler scheduler = createScheduler(10, gap -> {
                processed.add(gap);
                allProcessed.countDown();
            });

            GapDetector.Gap gap1 = createGap(0, 10, GapDetector.Type.HISTORICAL);
            GapDetector.Gap gap2 = createGap(11, 20, GapDetector.Type.HISTORICAL);
            GapDetector.Gap gap3 = createGap(21, 30, GapDetector.Type.LIVE_TAIL);

            // when
            scheduler.submit(gap1);
            scheduler.submit(gap2);
            scheduler.submit(gap3);

            // then
            boolean completed = allProcessed.await(2, TimeUnit.SECONDS);
            assertTrue(completed);
            assertEquals(List.of(gap1, gap2, gap3), processed);

            scheduler.close();
        }

        @Test
        @DisplayName("should stop processing on shutdown")
        void shouldStopOnShutdown() throws InterruptedException {
            // given
            AtomicInteger processedCount = new AtomicInteger(0);
            CountDownLatch firstProcessed = new CountDownLatch(1);
            BackfillTaskScheduler scheduler = createScheduler(10, gap -> {
                processedCount.incrementAndGet();
                firstProcessed.countDown();
                try {
                    Thread.sleep(100); // Slow processing
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            scheduler.submit(createGap(0, 10, GapDetector.Type.HISTORICAL));
            scheduler.submit(createGap(11, 20, GapDetector.Type.HISTORICAL));
            scheduler.submit(createGap(21, 30, GapDetector.Type.HISTORICAL));

            // Wait for first to start processing
            firstProcessed.await(1, TimeUnit.SECONDS);

            // when - close while processing
            scheduler.close();
            Thread.sleep(200);

            // then - not all gaps processed due to shutdown
            assertTrue(processedCount.get() < 3);
        }

        @Test
        @DisplayName("should restart worker if queue not empty after drain")
        void shouldRestartWorkerIfQueueNotEmpty() throws InterruptedException {
            // given
            List<GapDetector.Gap> processed = Collections.synchronizedList(new ArrayList<>());
            CountDownLatch firstBatchDone = new CountDownLatch(1);
            CountDownLatch allDone = new CountDownLatch(4);
            AtomicInteger callCount = new AtomicInteger(0);

            BackfillTaskScheduler scheduler = createScheduler(10, gap -> {
                processed.add(gap);
                allDone.countDown();
                if (callCount.incrementAndGet() == 2) {
                    firstBatchDone.countDown();
                }
            });

            // Submit 2 gaps
            scheduler.submit(createGap(0, 10, GapDetector.Type.HISTORICAL));
            scheduler.submit(createGap(11, 20, GapDetector.Type.HISTORICAL));

            // Wait for first batch to complete
            firstBatchDone.await(1, TimeUnit.SECONDS);
            Thread.sleep(50); // Let worker finish

            // Submit 2 more gaps - should restart worker
            scheduler.submit(createGap(21, 30, GapDetector.Type.HISTORICAL));
            scheduler.submit(createGap(31, 40, GapDetector.Type.HISTORICAL));

            // when/then
            boolean completed = allDone.await(2, TimeUnit.SECONDS);
            assertTrue(completed);
            assertEquals(4, processed.size());

            scheduler.close();
        }
    }

    @Nested
    @DisplayName("getFetcher Tests")
    class GetFetcherTests {

        @Test
        @DisplayName("should return the fetcher passed to constructor")
        void shouldReturnFetcher() {
            // given
            BackfillTaskScheduler scheduler = createScheduler(10);

            // when
            BackfillFetcher fetcher = scheduler.getFetcher();

            // then
            assertSame(mockFetcher, fetcher);

            scheduler.close();
        }
    }
}
