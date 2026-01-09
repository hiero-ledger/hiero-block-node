// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
        @DisplayName("should track a new block and increment pending count")
        void shouldTrackNewBlock() {
            // given
            long blockNumber = 100L;

            // when
            subject.trackBlock(blockNumber);

            // then
            assertEquals(1, subject.getPendingCount());
        }

        @Test
        @DisplayName("should not duplicate tracking when same block tracked twice")
        void shouldNotDuplicateTracking() {
            // given
            long blockNumber = 100L;

            // when
            subject.trackBlock(blockNumber);
            subject.trackBlock(blockNumber);

            // then
            assertEquals(1, subject.getPendingCount());
        }

        @Test
        @DisplayName("should track multiple different blocks")
        void shouldTrackMultipleBlocks() {
            // given
            long block1 = 100L;
            long block2 = 101L;
            long block3 = 102L;

            // when
            subject.trackBlock(block1);
            subject.trackBlock(block2);
            subject.trackBlock(block3);

            // then
            assertEquals(3, subject.getPendingCount());
        }
    }

    @Nested
    @DisplayName("awaitPersistence Tests")
    class AwaitPersistenceTests {

        @Test
        @DisplayName("should return true when block is persisted before timeout")
        void shouldReturnTrueWhenPersistedBeforeTimeout() {
            // given
            long blockNumber = 100L;
            subject.trackBlock(blockNumber);

            // Simulate persistence notification arriving in another thread
            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.submit(() -> {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                subject.handlePersisted(new PersistedNotification(blockNumber, true, 1, BlockSource.BACKFILL));
            });

            // when
            boolean result = subject.awaitPersistence(blockNumber, 1000);

            // then
            assertTrue(result);
            assertEquals(0, subject.getPendingCount());

            executor.shutdown();
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
            assertEquals(0, subject.getPendingCount()); // removed after await
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
            assertEquals(1, subject.getPendingCount());

            // Persist the block
            subject.handlePersisted(new PersistedNotification(blockNumber, true, 1, BlockSource.BACKFILL));

            // when
            subject.awaitPersistence(blockNumber, 100);

            // then
            assertEquals(0, subject.getPendingCount());
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

            // then - block should still be pending (latch not released)
            assertEquals(1, subject.getPendingCount());

            // await should timeout
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

            // then
            assertEquals(0, subject.getPendingCount());
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

            // then - block should still be pending, waiting for persistence
            assertEquals(1, subject.getPendingCount());

            // await should timeout
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

            // then - block should still be pending
            assertEquals(1, subject.getPendingCount());

            // await should timeout
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
            threadsStarted.await(1, TimeUnit.SECONDS);
            Thread.sleep(50); // Give threads time to enter await

            // when
            subject.clear();

            // then - give threads time to be released
            Thread.sleep(100);
            assertTrue(thread1Released.get());
            assertTrue(thread2Released.get());

            executor.shutdown();
        }

        @Test
        @DisplayName("should empty pending map after clear")
        void shouldEmptyPendingMap() {
            // given
            subject.trackBlock(100L);
            subject.trackBlock(101L);
            subject.trackBlock(102L);
            assertEquals(3, subject.getPendingCount());

            // when
            subject.clear();

            // then
            assertEquals(0, subject.getPendingCount());
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
            assertEquals(0, subject.getPendingCount());

            executor.shutdown();
        }
    }

    @Nested
    @DisplayName("getPendingCount Tests")
    class GetPendingCountTests {

        @Test
        @DisplayName("should return zero initially")
        void shouldReturnZeroInitially() {
            // when
            int count = subject.getPendingCount();

            // then
            assertEquals(0, count);
        }

        @Test
        @DisplayName("should return correct count after tracking blocks")
        void shouldReturnCorrectCountAfterTracking() {
            // given
            subject.trackBlock(1L);
            subject.trackBlock(2L);
            subject.trackBlock(3L);

            // when
            int count = subject.getPendingCount();

            // then
            assertEquals(3, count);
        }
    }
}
