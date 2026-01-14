// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Unit tests for {@link BackfillPersistenceAwaiter}.
 */
@Timeout(value = 5, unit = TimeUnit.SECONDS)
class BackfillPersistenceAwaiterTest {

    private BackfillPersistenceAwaiter subject;

    @BeforeEach
    void setUp() {
        subject = new BackfillPersistenceAwaiter();
    }

    @Nested
    @DisplayName("trackBlock Tests")
    class TrackBlockTests {

        @Test
        @DisplayName("should track a new block - await times out without notification")
        void shouldTrackNewBlock() {
            // given
            long blockNumber = 100L;

            // when
            subject.trackBlock(blockNumber);

            // then - block is tracked, so await times out without notification
            boolean result = subject.awaitPersistence(blockNumber, 50);
            assertFalse(result);
        }

        @Test
        @DisplayName("should not duplicate tracking when same block tracked twice")
        void shouldNotDuplicateTracking() {
            // given
            long blockNumber = 100L;

            // when
            subject.trackBlock(blockNumber);
            subject.trackBlock(blockNumber);

            // then - still only one latch, notification releases it
            subject.handlePersisted(new PersistedNotification(blockNumber, true, 1, BlockSource.BACKFILL));
            boolean result = subject.awaitPersistence(blockNumber, 100);
            assertTrue(result);
        }

        @Test
        @DisplayName("should track multiple different blocks independently")
        void shouldTrackMultipleBlocks() {
            // given
            long block1 = 100L;
            long block2 = 101L;
            long block3 = 102L;

            // when
            subject.trackBlock(block1);
            subject.trackBlock(block2);
            subject.trackBlock(block3);

            // then - each block can be persisted independently
            subject.handlePersisted(new PersistedNotification(block2, true, 1, BlockSource.BACKFILL));
            assertTrue(subject.awaitPersistence(block2, 100));
            assertFalse(subject.awaitPersistence(block1, 50)); // still waiting
            assertFalse(subject.awaitPersistence(block3, 50)); // still waiting
        }
    }

    @Nested
    @DisplayName("awaitPersistence Tests")
    class AwaitPersistenceTests {

        @Test
        @DisplayName("should return true when block is persisted before timeout")
        void shouldReturnTrueWhenPersistedBeforeTimeout() throws InterruptedException {
            // given
            long blockNumber = 100L;
            subject.trackBlock(blockNumber);

            CountDownLatch awaitStarted = new CountDownLatch(1);
            AtomicBoolean result = new AtomicBoolean(false);

            // Start awaiting in another thread
            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.submit(() -> {
                awaitStarted.countDown();
                result.set(subject.awaitPersistence(blockNumber, 1000));
            });

            // Wait for await to start, then send persistence notification
            assertTrue(awaitStarted.await(1, TimeUnit.SECONDS), "Await thread should start");
            Thread.sleep(10); // Brief pause to ensure await is blocking
            subject.handlePersisted(new PersistedNotification(blockNumber, true, 1, BlockSource.BACKFILL));

            // then
            executor.shutdown();
            assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
            assertTrue(result.get());
        }

        @Test
        @DisplayName("should return false when timeout occurs")
        void shouldReturnFalseOnTimeout() {
            // given
            long blockNumber = 100L;
            subject.trackBlock(blockNumber);

            // when - short timeout, no persistence notification
            boolean result = subject.awaitPersistence(blockNumber, 50);

            // then
            assertFalse(result);
        }

        @Test
        @DisplayName("should return true immediately if block not tracked")
        void shouldReturnTrueIfBlockNotTracked() {
            // given
            long blockNumber = 100L;
            // not tracked

            // when
            boolean result = subject.awaitPersistence(blockNumber, 1000);

            // then
            assertTrue(result);
        }

        @Test
        @DisplayName("should remove block from pending after await completes")
        void shouldRemoveBlockAfterAwait() {
            // given
            long blockNumber = 100L;
            subject.trackBlock(blockNumber);

            // Persist the block
            subject.handlePersisted(new PersistedNotification(blockNumber, true, 1, BlockSource.BACKFILL));

            // when
            boolean firstAwait = subject.awaitPersistence(blockNumber, 100);

            // then - first await succeeds, second await returns true immediately (not tracked)
            assertTrue(firstAwait);
            boolean secondAwait = subject.awaitPersistence(blockNumber, 50);
            assertTrue(secondAwait); // returns true because block is no longer tracked
        }
    }

    @Nested
    @DisplayName("handlePersisted Tests")
    class HandlePersistedTests {

        @Test
        @DisplayName("should release latch on successful persistence notification")
        void shouldReleaseLatchOnSuccessfulPersistence() {
            // given
            long blockNumber = 100L;
            subject.trackBlock(blockNumber);

            // when
            subject.handlePersisted(new PersistedNotification(blockNumber, true, 1, BlockSource.BACKFILL));

            // then - await should return immediately
            boolean result = subject.awaitPersistence(blockNumber, 100);
            assertTrue(result);
        }

        @Test
        @DisplayName("should release latch on failed persistence notification")
        void shouldReleaseLatchOnFailedPersistence() {
            // given
            long blockNumber = 100L;
            subject.trackBlock(blockNumber);

            // when - persistence failed
            subject.handlePersisted(new PersistedNotification(blockNumber, false, 1, BlockSource.BACKFILL));

            // then - await should still return (latch released)
            boolean result = subject.awaitPersistence(blockNumber, 100);
            assertTrue(result);
        }

        @Test
        @DisplayName("should ignore non-BACKFILL source notifications")
        void shouldIgnoreNonBackfillNotifications() {
            // given
            long blockNumber = 100L;
            subject.trackBlock(blockNumber);

            // when - notification from PUBLISHER source
            subject.handlePersisted(new PersistedNotification(blockNumber, true, 1, BlockSource.PUBLISHER));

            // then - block should still be pending (latch not released), await times out
            boolean result = subject.awaitPersistence(blockNumber, 50);
            assertFalse(result);
        }

        @Test
        @DisplayName("should handle notification for untracked block gracefully")
        void shouldHandleUntrackedBlockNotification() {
            // given
            long blockNumber = 100L;
            // not tracked

            // when - should not throw
            subject.handlePersisted(new PersistedNotification(blockNumber, true, 1, BlockSource.BACKFILL));

            // then - await returns true immediately (block was never tracked)
            boolean result = subject.awaitPersistence(blockNumber, 50);
            assertTrue(result);
        }
    }

    @Nested
    @DisplayName("handleVerification Tests")
    class HandleVerificationTests {

        @Test
        @DisplayName("should release latch on verification failure for fail-fast behavior")
        void shouldReleaseLatchOnVerificationFailure() {
            // given
            long blockNumber = 100L;
            subject.trackBlock(blockNumber);

            // when - verification failed
            subject.handleVerification(
                    new VerificationNotification(false, blockNumber, null, null, BlockSource.BACKFILL));

            // then - await should return immediately
            boolean result = subject.awaitPersistence(blockNumber, 100);
            assertTrue(result);
        }

        @Test
        @DisplayName("should not release latch on verification success")
        void shouldNotReleaseLatchOnVerificationSuccess() {
            // given
            long blockNumber = 100L;
            subject.trackBlock(blockNumber);

            // when - verification succeeded
            subject.handleVerification(
                    new VerificationNotification(true, blockNumber, null, null, BlockSource.BACKFILL));

            // then - block should still be pending (waiting for persistence), await times out
            boolean result = subject.awaitPersistence(blockNumber, 50);
            assertFalse(result);
        }

        @Test
        @DisplayName("should ignore non-BACKFILL source verification notifications")
        void shouldIgnoreNonBackfillVerifications() {
            // given
            long blockNumber = 100L;
            subject.trackBlock(blockNumber);

            // when - verification failure from PUBLISHER source
            subject.handleVerification(
                    new VerificationNotification(false, blockNumber, null, null, BlockSource.PUBLISHER));

            // then - block should still be pending, await times out
            boolean result = subject.awaitPersistence(blockNumber, 50);
            assertFalse(result);
        }
    }

    @Nested
    @DisplayName("clear Tests")
    class ClearTests {

        @Test
        @DisplayName("should release all waiting threads when cleared")
        void shouldReleaseAllLatches() throws InterruptedException {
            // given
            long block1 = 100L;
            long block2 = 101L;
            subject.trackBlock(block1);
            subject.trackBlock(block2);

            AtomicBoolean thread1Released = new AtomicBoolean(false);
            AtomicBoolean thread2Released = new AtomicBoolean(false);
            CountDownLatch threadsStarted = new CountDownLatch(2);

            ExecutorService executor = Executors.newFixedThreadPool(2);
            executor.submit(() -> {
                threadsStarted.countDown();
                subject.awaitPersistence(block1, 5000);
                thread1Released.set(true);
            });
            executor.submit(() -> {
                threadsStarted.countDown();
                subject.awaitPersistence(block2, 5000);
                thread2Released.set(true);
            });

            // Wait for threads to start waiting
            assertTrue(threadsStarted.await(1, TimeUnit.SECONDS), "Threads should start");
            Thread.sleep(50); // Give threads time to enter await

            // when
            subject.clear();

            // then
            executor.shutdown();
            assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS), "Threads should be released");
            assertTrue(thread1Released.get());
            assertTrue(thread2Released.get());
        }

        @Test
        @DisplayName("should empty pending map after clear")
        void shouldEmptyPendingMap() {
            // given
            long block1 = 100L;
            long block2 = 101L;
            long block3 = 102L;
            subject.trackBlock(block1);
            subject.trackBlock(block2);
            subject.trackBlock(block3);

            // when
            subject.clear();

            // then - all awaits return true immediately (blocks no longer tracked)
            assertTrue(subject.awaitPersistence(block1, 50));
            assertTrue(subject.awaitPersistence(block2, 50));
            assertTrue(subject.awaitPersistence(block3, 50));
        }
    }

    @Nested
    @DisplayName("Concurrency Tests")
    class ConcurrencyTests {

        @Test
        @DisplayName("should handle concurrent track and await operations safely")
        void shouldHandleConcurrentTrackAndAwait() throws InterruptedException {
            // given
            int numBlocks = 100;
            CountDownLatch allDone = new CountDownLatch(numBlocks);
            ExecutorService executor = Executors.newFixedThreadPool(10);

            // when - concurrent track, persist, and await
            for (int i = 0; i < numBlocks; i++) {
                final long blockNumber = i;
                executor.submit(() -> {
                    subject.trackBlock(blockNumber);
                    subject.handlePersisted(new PersistedNotification(blockNumber, true, 1, BlockSource.BACKFILL));
                    subject.awaitPersistence(blockNumber, 1000);
                    allDone.countDown();
                });
            }

            // then
            boolean completed = allDone.await(4, TimeUnit.SECONDS);
            assertTrue(completed);

            executor.shutdown();
        }
    }
}
